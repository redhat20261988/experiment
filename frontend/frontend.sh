#!/bin/bash

# 前端服务管理脚本
# 支持启动、停止、重启、查看状态等操作

# 脚本所在目录（frontend目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 项目根目录
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# 前端目录
FRONTEND_DIR="$SCRIPT_DIR"
# PID文件路径
PID_FILE="$FRONTEND_DIR/.frontend.pid"
# 日志文件路径
LOG_FILE="$FRONTEND_DIR/.frontend.log"
# 端口号（Vite 默认端口 5173）
PORT=5173

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查进程是否运行
is_running() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            return 0
        else
            # PID文件存在但进程不存在，清理PID文件
            rm -f "$PID_FILE"
            return 1
        fi
    fi
    return 1
}

# 检查端口是否被占用
is_port_in_use() {
    if command -v lsof > /dev/null 2>&1; then
        lsof -i :$PORT > /dev/null 2>&1
    elif command -v netstat > /dev/null 2>&1; then
        netstat -an | grep ":$PORT " | grep LISTEN > /dev/null 2>&1
    else
        # 如果两个命令都不存在，尝试使用ss命令
        ss -an | grep ":$PORT " | grep LISTEN > /dev/null 2>&1
    fi
}

# 启动服务
start() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_warn "前端服务已经在运行中 (PID: $PID)"
        return 1
    fi

    # 检查端口是否被占用
    if is_port_in_use; then
        print_error "端口 $PORT 已被占用，请先停止占用该端口的进程"
        return 1
    fi

    # 检查node和npm是否安装
    if ! command -v node > /dev/null 2>&1; then
        print_error "Node.js 未安装，请先安装 Node.js"
        return 1
    fi

    if ! command -v npm > /dev/null 2>&1; then
        print_error "npm 未安装，请先安装 npm"
        return 1
    fi

    # 检查node_modules是否存在
    if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
        print_warn "node_modules 不存在，正在安装依赖..."
        cd "$FRONTEND_DIR" || exit 1
        npm install
        if [ $? -ne 0 ]; then
            print_error "依赖安装失败"
            return 1
        fi
    fi

    print_info "正在启动前端服务..."
    cd "$FRONTEND_DIR" || exit 1

    # 后台启动服务，将输出重定向到日志文件
    nohup npm run dev > "$LOG_FILE" 2>&1 &
    PID=$!

    # 等待一下，检查进程是否成功启动
    sleep 2
    if ps -p "$PID" > /dev/null 2>&1; then
        echo $PID > "$PID_FILE"
        print_info "前端服务启动成功 (PID: $PID)"
        print_info "服务地址: http://localhost:$PORT"
        print_info "日志文件: $LOG_FILE"
        return 0
    else
        print_error "前端服务启动失败，请查看日志: $LOG_FILE"
        return 1
    fi
}

# 停止服务
stop() {
    if ! is_running; then
        print_warn "前端服务未运行"
        return 1
    fi

    PID=$(cat "$PID_FILE")
    print_info "正在停止前端服务 (PID: $PID)..."

    # 尝试优雅停止
    kill "$PID" 2>/dev/null

    # 等待进程结束
    for i in {1..10}; do
        if ! ps -p "$PID" > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done

    # 如果进程仍在运行，强制杀死
    if ps -p "$PID" > /dev/null 2>&1; then
        print_warn "进程未响应，强制停止..."
        kill -9 "$PID" 2>/dev/null
        sleep 1
    fi

    # 清理PID文件
    rm -f "$PID_FILE"

    if ! ps -p "$PID" > /dev/null 2>&1; then
        print_info "前端服务已停止"
        return 0
    else
        print_error "停止前端服务失败"
        return 1
    fi
}

# 强制释放端口（当无 PID 文件但端口被占用时）
force_release_port() {
    if ! is_running && is_port_in_use; then
        print_warn "检测到端口 $PORT 被占用但无 PID 文件，尝试释放端口..."
        if command -v lsof > /dev/null 2>&1; then
            PIDS=$(lsof -t -i :$PORT 2>/dev/null)
            if [ -n "$PIDS" ]; then
                echo "$PIDS" | xargs kill 2>/dev/null
                sleep 2
                # 若仍未释放则强制结束
                PIDS=$(lsof -t -i :$PORT 2>/dev/null)
                [ -n "$PIDS" ] && echo "$PIDS" | xargs kill -9 2>/dev/null
                sleep 1
                print_info "已释放端口 $PORT"
            fi
        fi
    fi
}

# 重启服务
restart() {
    print_info "正在重启前端服务..."
    stop
    sleep 1
    force_release_port
    start
}

# 查看状态
status() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_info "前端服务正在运行 (PID: $PID)"
        print_info "服务地址: http://localhost:$PORT"

        # 显示进程信息
        if command -v ps > /dev/null 2>&1; then
            echo ""
            ps -p "$PID" -o pid,ppid,cmd,etime,stat
        fi

        # 检查端口
        if is_port_in_use; then
            print_info "端口 $PORT 正在监听"
        else
            print_warn "端口 $PORT 未监听"
        fi

        return 0
    else
        print_warn "前端服务未运行"
        return 1
    fi
}

# 查看日志
logs() {
    if [ -f "$LOG_FILE" ]; then
        tail -f "$LOG_FILE"
    else
        print_warn "日志文件不存在: $LOG_FILE"
    fi
}

# 显示帮助信息
usage() {
    echo "用法: $0 {start|stop|restart|status|logs|help}"
    echo ""
    echo "命令说明:"
    echo "  start   - 启动前端服务"
    echo "  stop    - 停止前端服务"
    echo "  restart - 重启前端服务"
    echo "  status  - 查看服务状态"
    echo "  logs    - 查看服务日志（实时）"
    echo "  help    - 显示帮助信息"
    echo ""
    echo "配置文件:"
    echo "  PID文件: $PID_FILE"
    echo "  日志文件: $LOG_FILE"
    echo "  端口: $PORT"
}

# 主函数
main() {
    case "$1" in
        start)
            start
            ;;
        stop)
            stop
            ;;
        restart)
            restart
            ;;
        status)
            status
            ;;
        logs)
            logs
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            print_error "未知命令: $1"
            echo ""
            usage
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"
