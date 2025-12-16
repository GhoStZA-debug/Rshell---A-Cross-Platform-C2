package logger

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
)

// LogLevel 日志级别
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

var (
	levelNames = map[LogLevel]string{
		DEBUG: "DEBUG",
		INFO:  "INFO",
		WARN:  "WARN",
		ERROR: "ERROR",
		FATAL: "FATAL",
	}

	levelColors = map[LogLevel]*color.Color{
		DEBUG: color.New(color.FgHiCyan),
		INFO:  color.New(color.FgGreen),
		WARN:  color.New(color.FgYellow),
		ERROR: color.New(color.FgRed),
		FATAL: color.New(color.FgHiRed, color.Bold),
	}

	timeColor = color.New(color.FgHiBlack).SprintFunc()

	// 全局配置
	globalLevel = INFO
	showCaller  = false
	loggerMu    sync.Mutex
)

// Config 日志配置
type Config struct {
	Level      LogLevel
	ShowCaller bool
	Colorful   bool
}

// SetConfig 设置日志配置
func SetConfig(config Config) {
	loggerMu.Lock()
	defer loggerMu.Unlock()

	globalLevel = config.Level
	showCaller = config.ShowCaller

	if !config.Colorful {
		color.NoColor = true
	}
}

// SetLevel 设置日志级别
func SetLevel(level LogLevel) {
	loggerMu.Lock()
	globalLevel = level
	loggerMu.Unlock()
}

// GetLevel 获取当前日志级别
func GetLevel() LogLevel {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	return globalLevel
}

// EnableColor 启用/禁用彩色输出
func EnableColor(enabled bool) {
	color.NoColor = !enabled
}

// timestamp 获取时间戳字符串
func timestamp() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

// getCallerInfo 获取调用者信息
func getCallerInfo() string {
	if !showCaller {
		return ""
	}

	pc, file, line, ok := runtime.Caller(3)
	if !ok {
		return ""
	}

	funcName := runtime.FuncForPC(pc).Name()
	shortFile := file
	if idx := strings.LastIndex(file, "/"); idx != -1 {
		shortFile = file[idx+1:]
	}

	shortFunc := funcName
	if idx := strings.LastIndex(funcName, "."); idx != -1 {
		shortFunc = funcName[idx+1:]
	}

	return fmt.Sprintf("%s:%d %s", shortFile, line, shortFunc)
}

// ========== 核心修改：支持两种调用方式 ==========

// logf 格式化日志（支持 fmt.Sprintf 风格）
func logf(level LogLevel, format string, args ...interface{}) {
	if level < globalLevel {
		return
	}

	loggerMu.Lock()
	defer loggerMu.Unlock()

	msg := fmt.Sprintf(format, args...)
	printLog(level, msg)
}

// log 简单日志（支持多个参数拼接）
func log(level LogLevel, args ...interface{}) {
	if level < globalLevel {
		return
	}

	loggerMu.Lock()
	defer loggerMu.Unlock()

	// 将多个参数拼接成字符串
	var msg string
	if len(args) == 1 {
		msg = fmt.Sprintf("%v", args[0])
	} else {
		parts := make([]string, len(args))
		for i, arg := range args {
			parts[i] = fmt.Sprintf("%v", arg)
		}
		msg = strings.Join(parts, " ")
	}

	printLog(level, msg)
}

// printLog 实际打印日志
func printLog(level LogLevel, msg string) {
	callerInfo := getCallerInfo()
	timeStr := timeColor(timestamp())
	levelStr := levelColors[level].Sprintf("[%s]", levelNames[level])

	if callerInfo != "" {
		callerStr := color.New(color.FgHiBlack).Sprintf("(%s)", callerInfo)
		fmt.Fprintf(os.Stderr, "%s %s %s %s\n", timeStr, levelStr, callerStr, msg)
	} else {
		fmt.Fprintf(os.Stderr, "%s %s %s\n", timeStr, levelStr, msg)
	}
}

// ========== 公共接口 ==========

// Debug 调试日志
func Debug(args ...interface{}) {
	log(DEBUG, args...)
}

// Debugf 格式化调试日志
func Debugf(format string, args ...interface{}) {
	logf(DEBUG, format, args...)
}

// Info 信息日志
func Info(args ...interface{}) {
	log(INFO, args...)
}

// Infof 格式化信息日志
func Infof(format string, args ...interface{}) {
	logf(INFO, format, args...)
}

// Warn 警告日志
func Warn(args ...interface{}) {
	log(WARN, args...)
}

// Warnf 格式化警告日志
func Warnf(format string, args ...interface{}) {
	logf(WARN, format, args...)
}

// Error 错误日志
func Error(args ...interface{}) {
	log(ERROR, args...)
}

// Errorf 格式化错误日志
func Errorf(format string, args ...interface{}) {
	logf(ERROR, format, args...)
}

// Fatal 致命错误日志
func Fatal(args ...interface{}) {
	log(FATAL, args...)
	os.Exit(1)
}

// Fatalf 格式化致命错误日志
func Fatalf(format string, args ...interface{}) {
	logf(FATAL, format, args...)
	os.Exit(1)
}

// ErrorWithStack 带堆栈信息的错误日志
func ErrorWithStack(err error, format string, args ...interface{}) {
	if err == nil {
		return
	}

	var msg string
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	} else {
		msg = format
	}

	if msg != "" {
		msg = fmt.Sprintf("%s: %v", msg, err)
	} else {
		msg = err.Error()
	}

	log(ERROR, msg)

	// 在DEBUG级别打印堆栈
	if globalLevel == DEBUG {
		printStack()
	}
}

// printStack 打印堆栈信息
func printStack() {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	fmt.Fprintf(os.Stderr, "\n%s\n", buf)
}

// StructuredLog 结构化日志
func StructuredLog(level LogLevel, fields map[string]interface{}, message string) {
	if level < globalLevel {
		return
	}

	loggerMu.Lock()
	defer loggerMu.Unlock()

	timeStr := timeColor(timestamp())
	levelStr := levelColors[level].Sprintf("[%s]", levelNames[level])

	var fieldsStr string
	if len(fields) > 0 {
		var parts []string
		for k, v := range fields {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		fieldsStr = " " + strings.Join(parts, " ")
	}

	callerInfo := getCallerInfo()

	if callerInfo != "" {
		callerStr := color.New(color.FgHiBlack).Sprintf("(%s)", callerInfo)
		fmt.Fprintf(os.Stderr, "%s %s %s %s%s\n", timeStr, levelStr, callerStr, message, fieldsStr)
	} else {
		fmt.Fprintf(os.Stderr, "%s %s %s%s\n", timeStr, levelStr, message, fieldsStr)
	}
}
