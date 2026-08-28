#!/usr/bin/env bash
# TradingView 服务部署脚本（服务器上执行）
# 参考 stocks-API 的 deploy.sh 方式
# 用法: bash deploy_server_tv.sh
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== 1. 安装系统依赖 ==="
sudo apt-get update -qq || true
sudo apt-get install -y -qq python3 python3-pip python3-venv git curl || true

echo "=== 2. 创建虚拟环境 ==="
if [ ! -d "venv_tv" ]; then
    python3 -m venv venv_tv
fi
source venv_tv/bin/activate

echo "=== 3. 安装 Python 依赖 ==="
pip install -q -U pip wheel
pip install -q -r server_tv_requirements.txt

echo "=== 4. 配置 .env ==="
if [ ! -f ".env" ]; then
    cat > .env <<'EOF'
R2_ACCOUNT_ID=8e43ef2043266e0898cf9e02ca53df2f
R2_ACCESS_KEY_ID=你的_Access_Key_ID
R2_SECRET_ACCESS_KEY=你的_Secret_Access_Key
R2_BUCKET=stocks-tv
TV_SYNC_INTERVAL=1
TV_REGIONS=us
PORT=3216
EOF
    echo ">>> 编辑 .env 填入 R2 凭据后重新运行 <<<"
    exit 1
fi
set -a; source .env; set +a

echo "=== 5. 启动服务（后台）==="
# 用 nohup 后台运行（或改用 systemd / 1Panel 管理）
nohup python server_tv.py > server_tv.log 2>&1 &
echo ">>> 已启动，日志: server_tv.log（端口 $PORT）<<<"
echo ">>> 停止: pkill -f server_tv.py <<<"