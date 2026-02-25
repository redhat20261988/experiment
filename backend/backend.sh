#!/bin/bash

# 后端服务管理脚本
# 支持启动、停止、重启、查看状态等操作

# 脚本所在目录（backend目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 项目根目录
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# 后端目录
BACKEND_DIR="$SCRIPT_DIR"
# PID文件路径
PID_FILE="$BACKEND_DIR/.backend.pid"
# 日志文件路径（放在logs目录下）
LOG_FILE="$BACKEND_DIR/logs/backend.log"
# 端口号（从application.yml中获取，默认8080）
PORT=8080
# JAR文件路径（如果已打包）
JAR_FILE="$BACKEND_DIR/target/funding-rate-monitor-*.jar"
# 主类
MAIN_CLASS="com.experiment.FundingRateMonitorApplication"
# JVM参数
# 堆内存配置
JVM_OPTS="-Xms512m -Xmx512m"
# G1垃圾回收器配置
JVM_OPTS="$JVM_OPTS -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
# Metaspace配置
JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m"
# GC日志配置（用于监控和分析GC性能）
GC_LOG_DIR="$BACKEND_DIR/logs"
GC_LOG_FILE="$GC_LOG_DIR/gc.log"
JVM_OPTS="$JVM_OPTS -Xlog:gc*:file=$GC_LOG_FILE:time,tags:filecount=5,filesize=10M"

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

# 查找JAR文件
find_jar_file() {
    local jar_files=($BACKEND_DIR/target/funding-rate-monitor-*.jar)
    if [ -f "${jar_files[0]}" ]; then
        echo "${jar_files[0]}"
        return 0
    fi
    return 1
}

# 删除旧的JAR文件
clean_old_jars() {
    print_info "正在清理旧的JAR文件..."
    local jar_files=($BACKEND_DIR/target/funding-rate-monitor-*.jar)
    local jar_count=0
    for jar_file in "${jar_files[@]}"; do
        if [ -f "$jar_file" ]; then
            print_info "删除旧JAR文件: $jar_file"
            rm -f "$jar_file"
            jar_count=$((jar_count + 1))
        fi
    done
    if [ $jar_count -eq 0 ]; then
        print_info "未找到旧的JAR文件"
    else
        print_info "已删除 $jar_count 个旧JAR文件"
    fi
}

# 自动编译项目（独立 Spring Boot 项目，在 backend 目录下直接构建）
auto_build() {
    print_info "正在自动编译项目..."
    cd "$BACKEND_DIR" || exit 1

    if ! command -v mvn > /dev/null 2>&1; then
        print_error "Maven 未安装，无法编译项目"
        return 1
    fi

    # 删除旧的JAR文件
    clean_old_jars

    # 编译项目
    print_info "执行编译命令: mvn clean package -DskipTests"
    mvn clean package -DskipTests

    if [ $? -eq 0 ]; then
        print_info "项目编译成功"
        JAR_PATH=$(find_jar_file)
        if [ -n "$JAR_PATH" ]; then
            print_info "新JAR文件位置: $JAR_PATH"
            return 0
        else
            print_error "编译成功但未找到JAR文件"
            return 1
        fi
    else
        print_error "项目编译失败"
        return 1
    fi
}

# 检查服务健康状态（项目无 actuator 时使用根路径检查）
check_health() {
    if command -v curl > /dev/null 2>&1; then
        local response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$PORT/ 2>/dev/null)
        # 任何有效的 HTTP 响应（非 000 表示连接成功）表示服务已启动
        if [ -n "$response" ] && [ "$response" != "000" ]; then
            return 0
        fi
    fi
    return 1
}

# 等待服务启动完成
wait_for_service() {
    local max_wait=${1:-60}  # 默认等待60秒
    local check_interval=2   # 每2秒检查一次
    local elapsed=0

    print_info "等待服务启动完成（最多等待 ${max_wait} 秒）..."

    while [ $elapsed -lt $max_wait ]; do
        # 检查进程是否还在运行
        if [ -f "$PID_FILE" ]; then
            local pid=$(cat "$PID_FILE")
            if ! ps -p "$pid" > /dev/null 2>&1; then
                print_error "服务进程已退出，启动失败"
                return 1
            fi
        fi

        # 检查端口是否监听
        if is_port_in_use; then
            # 检查健康状态
            if check_health; then
                print_info "服务启动成功！"
                return 0
            fi
        fi

        sleep $check_interval
        elapsed=$((elapsed + check_interval))

        # 显示进度
        if [ $((elapsed % 10)) -eq 0 ]; then
            print_info "已等待 ${elapsed} 秒，继续等待..."
        fi
    done

    # 超时检查
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            if is_port_in_use; then
                print_warn "服务已启动但健康检查未通过，请查看日志: $LOG_FILE"
                return 0  # 进程在运行且端口在监听，认为启动成功
            else
                print_error "服务进程运行中但端口未监听，启动可能失败"
                return 1
            fi
        else
            print_error "服务进程已退出，启动失败"
            return 1
        fi
    else
        print_error "等待超时，服务可能启动失败"
        return 1
    fi
}

# 启动服务
start() {
    local force_build=${1:-false}  # 是否强制编译，默认为false

    if is_running; then
        PID=$(cat "$PID_FILE")
        print_warn "后端服务已经在运行中 (PID: $PID)"
        return 1
    fi

    # 检查端口是否被占用
    if is_port_in_use; then
        print_error "端口 $PORT 已被占用，请先停止占用该端口的进程"
        return 1
    fi

    # 检查Java是否安装
    if ! command -v java > /dev/null 2>&1; then
        print_error "Java 未安装，请先安装 Java"
        return 1
    fi

    # 检查Maven是否安装（用于编译，启动时需要重新打包）
    if ! command -v mvn > /dev/null 2>&1; then
        print_error "Maven 未安装，无法重新打包项目"
        print_error "启动服务需要先重新打包，请先安装 Maven"
        return 1
    fi

    print_info "正在启动后端服务..."
    cd "$BACKEND_DIR" || exit 1

    # 创建GC日志目录（如果不存在）
    mkdir -p "$GC_LOG_DIR"
    if [ ! -d "$GC_LOG_DIR" ]; then
        print_error "无法创建GC日志目录: $GC_LOG_DIR"
        return 1
    fi

    # 每次启动时都删除旧的JAR文件并重新打包
    print_info "启动前清理旧的JAR文件并重新打包..."
    if ! auto_build; then
        print_error "自动编译失败，无法启动服务"
        return 1
    fi
    # 重新查找JAR文件
    JAR_PATH=$(find_jar_file)

    # 查找JAR文件
    if [ -n "$JAR_PATH" ]; then
        print_info "找到JAR文件: $JAR_PATH"
        print_info "使用JAR文件启动服务..."

        # 使用JAR文件启动，输出到日志文件
        nohup java $JVM_OPTS -jar "$JAR_PATH" > "$LOG_FILE" 2>&1 &
        PID=$!
    else
        print_error "未找到JAR文件，无法启动服务"
        return 1
    fi

    # 保存PID
    echo $PID > "$PID_FILE"
    print_info "后端服务进程已启动 (PID: $PID)"
    print_info "正在显示启动日志输出..."
    print_info "=========================================="

    # 等待日志文件创建
    sleep 1

    # 实时显示启动日志，持续30秒或直到服务启动完成
    local log_display_time=30
    local log_display_elapsed=0
    local log_display_interval=1
    local last_line_count=0

    # 如果日志文件已存在，记录当前行数
    if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
        last_line_count=$(wc -l < "$LOG_FILE" 2>/dev/null || echo "0")
    fi

    while [ $log_display_elapsed -lt $log_display_time ]; do
        # 检查进程是否还在运行
        if ! ps -p "$PID" > /dev/null 2>&1; then
            print_error ""
            print_error "服务进程已退出，启动失败"
            break
        fi

        # 如果日志文件存在且有内容，显示新增的日志行
        if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
            local current_line_count=$(wc -l < "$LOG_FILE" 2>/dev/null || echo "0")
            if [ "$current_line_count" -gt "$last_line_count" ]; then
                # 显示新增的日志行
                tail -n +$((last_line_count + 1)) "$LOG_FILE" 2>/dev/null
                last_line_count=$current_line_count
            fi
        fi

        # 检查服务是否已启动完成（端口监听）
        if is_port_in_use; then
            print_info ""
            print_info "服务已启动完成（端口 $PORT 正在监听）"
            break
        fi

        sleep $log_display_interval
        log_display_elapsed=$((log_display_elapsed + log_display_interval))
    done

    print_info "=========================================="
    print_info "（完整日志请查看: $LOG_FILE）"
    print_info ""

    # 检查启动过程中是否有错误信息并输出到标准输出
    if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
        # 检查日志文件中是否有错误信息
        local error_lines=$(grep -iE "error|exception|failed|失败" "$LOG_FILE" 2>/dev/null)
        if [ -n "$error_lines" ]; then
            local error_count=$(echo "$error_lines" | wc -l)
            print_error "检测到启动过程中的错误信息（共 $error_count 条）："
            echo "$error_lines" | tail -20 | while IFS= read -r line; do
                echo "  $line"
            done
        fi
    fi

    if ps -p "$PID" > /dev/null 2>&1; then
        print_info "后端服务进程运行中 (PID: $PID)"
        print_info "服务地址: http://localhost:$PORT"
        print_info "应用日志文件: $LOG_FILE"
        print_info "GC日志文件: $GC_LOG_FILE"

        # 等待服务完全启动
        if wait_for_service 120; then
            print_info "后端服务启动成功！"
            return 0
        else
            print_error "后端服务启动失败，请查看日志: $LOG_FILE"
            # 显示最近的错误信息
            if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
                local recent_errors=$(tail -50 "$LOG_FILE" | grep -iE "error|exception|failed|失败" 2>/dev/null)
                if [ -n "$recent_errors" ]; then
                    print_error "最近的错误信息："
                    echo "$recent_errors" | tail -20 | while IFS= read -r line; do
                        echo "  $line"
                    done
                else
                    print_error "最后20行日志："
                    tail -20 "$LOG_FILE" | while IFS= read -r line; do
                        echo "  $line"
                    done
                fi
            fi
            # 清理PID文件
            rm -f "$PID_FILE"
            return 1
        fi
    else
        print_error "后端服务进程启动失败"
        # 显示启动失败的错误信息
        if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
            local startup_errors=$(tail -50 "$LOG_FILE" | grep -iE "error|exception|failed|失败" 2>/dev/null)
            if [ -n "$startup_errors" ]; then
                print_error "启动失败的错误信息："
                echo "$startup_errors" | tail -20 | while IFS= read -r line; do
                    echo "  $line"
                done
            else
                print_error "最后30行日志："
                tail -30 "$LOG_FILE" | while IFS= read -r line; do
                    echo "  $line"
                done
            fi
        fi
        return 1
    fi
}

# 停止服务
stop() {
    local stopped=false

    # 首先尝试通过PID文件停止
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_info "正在停止后端服务 (PID: $PID)..."

        # 尝试优雅停止（发送SIGTERM信号）
        kill "$PID" 2>/dev/null

        # 等待进程结束
        for i in {1..30}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                stopped=true
                break
            fi
            sleep 1
        done

        # 如果进程仍在运行，强制杀死
        if ps -p "$PID" > /dev/null 2>&1; then
            print_warn "进程未响应，强制停止..."
            kill -9 "$PID" 2>/dev/null
            sleep 1
            if ! ps -p "$PID" > /dev/null 2>&1; then
                stopped=true
            fi
        fi

        # 清理PID文件
        rm -f "$PID_FILE"
    fi

    # 检查端口是否仍被占用，如果是，尝试找到并停止占用端口的进程
    if is_port_in_use; then
        print_warn "端口 $PORT 仍被占用，尝试查找并停止占用端口的进程..."

        local port_pid=""
        if command -v lsof > /dev/null 2>&1; then
            port_pid=$(lsof -ti :$PORT 2>/dev/null | head -1)
        elif command -v netstat > /dev/null 2>&1; then
            port_pid=$(netstat -tlnp 2>/dev/null | grep ":$PORT " | grep LISTEN | awk '{print $7}' | cut -d'/' -f1 | head -1)
        elif command -v ss > /dev/null 2>&1; then
            port_pid=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep LISTEN | awk '{print $6}' | cut -d',' -f2 | cut -d'=' -f2 | head -1)
        fi

        if [ -n "$port_pid" ] && [ "$port_pid" != " " ]; then
            print_info "找到占用端口的进程 (PID: $port_pid)，正在停止..."
            kill "$port_pid" 2>/dev/null

            # 等待进程结束
            for i in {1..15}; do
                if ! ps -p "$port_pid" > /dev/null 2>&1; then
                    stopped=true
                    break
                fi
                sleep 1
            done

            # 如果进程仍在运行，强制杀死
            if ps -p "$port_pid" > /dev/null 2>&1; then
                print_warn "占用端口的进程未响应，强制停止..."
                kill -9 "$port_pid" 2>/dev/null
                sleep 1
                if ! ps -p "$port_pid" > /dev/null 2>&1; then
                    stopped=true
                fi
            fi
        fi

        # 额外等待，确保端口释放
        sleep 2

        # 再次检查端口
        if is_port_in_use; then
            print_error "端口 $PORT 仍被占用，停止失败"
            return 1
        fi
    fi

    if [ "$stopped" = true ] || ! is_port_in_use; then
        print_info "后端服务已停止"
        return 0
    else
        print_error "停止后端服务失败"
        return 1
    fi
}

# 重启服务
restart() {
    print_info "=========================================="
    print_info "正在重启后端服务..."
    print_info "=========================================="
    print_info ""
    print_info "步骤 1/2: 停止服务"
    print_info "----------------------------------------"
    stop
    sleep 2

    # 再次检查端口是否已释放
    if is_port_in_use; then
        print_warn "端口 $PORT 仍被占用，等待释放..."
        local wait_count=0
        while is_port_in_use && [ $wait_count -lt 10 ]; do
            sleep 1
            wait_count=$((wait_count + 1))
        done

        if is_port_in_use; then
            print_error "端口 $PORT 仍被占用，无法启动服务"
            print_error "请手动停止占用端口的进程，或使用 'stop' 命令强制停止"
            return 1
        fi
    fi

    print_info ""
    print_info "步骤 2/2: 启动服务（将自动编译）"
    print_info "----------------------------------------"

    # 调用start函数并等待结果（强制编译）
    if start true; then
        print_info ""
        print_info "=========================================="
        print_info "后端服务重启成功！"
        print_info "=========================================="
        return 0
    else
        print_error ""
        print_error "=========================================="
        print_error "后端服务重启失败"
        print_error "=========================================="
        return 1
    fi
}

# 查看状态
status() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_info "后端服务正在运行 (PID: $PID)"
        print_info "服务地址: http://localhost:$PORT"

        # 显示进程信息
        if command -v ps > /dev/null 2>&1; then
            echo ""
            ps -p "$PID" -o pid,ppid,cmd,etime,stat,%mem,%cpu
        fi

        # 检查端口
        if is_port_in_use; then
            print_info "端口 $PORT 正在监听"
        else
            print_warn "端口 $PORT 未监听"
        fi

        # 尝试检查健康状态
        if command -v curl > /dev/null 2>&1; then
            echo ""
            print_info "健康检查:"
            curl -s http://localhost:$PORT/ 2>/dev/null | head -5 || print_warn "无法连接到服务"
        fi

        return 0
    else
        print_warn "后端服务未运行"
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

# 打包项目
build() {
    print_info "正在打包项目..."

    if ! command -v mvn > /dev/null 2>&1; then
        print_error "Maven 未安装，无法打包项目"
        return 1
    fi

    # 删除旧的JAR文件
    clean_old_jars

    # 编译项目
    cd "$BACKEND_DIR" || exit 1
    print_info "执行编译命令: mvn clean package -DskipTests"
    mvn clean package -DskipTests

    if [ $? -eq 0 ]; then
        print_info "项目打包成功"
        JAR_PATH=$(find_jar_file)
        if [ -n "$JAR_PATH" ]; then
            print_info "JAR文件位置: $JAR_PATH"
        fi
        return 0
    else
        print_error "项目打包失败"
        return 1
    fi
}

# 显示帮助信息
usage() {
    echo "用法: $0 {start|stop|restart|status|logs|build|help}"
    echo ""
    echo "命令说明:"
    echo "  start   - 启动后端服务"
    echo "  stop    - 停止后端服务"
    echo "  restart - 重启后端服务"
    echo "  status  - 查看服务状态"
    echo "  logs    - 查看服务日志（实时）"
    echo "  build   - 打包项目（生成JAR文件）"
    echo "  help    - 显示帮助信息"
    echo ""
    echo "配置文件:"
    echo "  PID文件: $PID_FILE"
    echo "  应用日志文件: $LOG_FILE"
    echo "  GC日志文件: $GC_LOG_FILE"
    echo "  端口: $PORT"
    echo "  主类: $MAIN_CLASS"
    echo ""
    echo "JVM参数:"
    echo "  $JVM_OPTS"
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
        build)
            build
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
