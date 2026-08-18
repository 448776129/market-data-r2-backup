#!/usr/bin/env bash
# 服务器自部署初始化脚本（Ubuntu/Debian）
# 用法：bash 服务器自部署版/deploy.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_DIR/.env"

echo "==> [1/4] 安装系统依赖"
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv cron || true

echo "==> [2/4] 建立 Python 虚拟环境"
if [ ! -d "$REPO_DIR/.venv" ]; then
    python3 -m venv "$REPO_DIR/.venv"
fi
"$REPO_DIR/.venv/bin/pip" install --upgrade pip
"$REPO_DIR/.venv/bin/pip" install -r "$REPO_DIR/服务器自部署版/requirements-server.txt"

echo "==> [3/4] 配置环境变量"
if [ ! -f "$ENV_FILE" ]; then
    cp "$REPO_DIR/服务器自部署版/.env.example" "$ENV_FILE"
    echo "⚠️  请编辑 $ENV_FILE 填入 R2/KV 凭据后重跑本脚本"
    exit 0
fi

echo "==> [4/4] 安装 crontab"
(crontab -l 2>/dev/null | grep -v market-data-r2; echo "# === market-data-r2 采集任务 ==="; cat "$REPO_DIR/服务器自部署版/crontab.conf") | crontab -

echo "✅ 完成。查看：crontab -l"
echo "首次全量历史：source $ENV_FILE && python3 $REPO_DIR/scripts/fetch_history.py --region all"