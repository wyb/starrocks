#!/usr/bin/env python3
"""
StarRocks PR Analytics Daemon - 每隔 1 小时自动运行 pipeline 任务。

Usage:
    # 直接在前台运行
    python3 daemon.py

    # 在后台持久运行并将日志记录到文件
    nohup python3 daemon.py > daemon.log 2>&1 &

    # 查看运行状态
    tail -f daemon.log

    # 停止后台进程
    ps aux | grep daemon.py | grep -v grep | awk '{print $2}' | xargs kill
"""

import time
import subprocess
import sys
import os
from datetime import datetime

# 检查频率（秒）：1小时 = 3600秒
INTERVAL = 3600

def run_pipeline():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*60}", flush=True)
    print(f"[{now}] Starting scheduled pipeline execution...", flush=True)
    print(f"{'='*60}\n", flush=True)
    
    try:
        # 使用当前 Python 解释器运行同目录下的 pr.py
        # --days 1 足够覆盖每小时的增量数据，fetch 步骤会自动去重
        result = subprocess.run(
            [sys.executable, "pr.py", "pipeline", "--days", "1"],
            check=False,
            text=True
        )
        
        finish_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if result.returncode == 0:
            print(f"\n[{finish_now}] Pipeline finished successfully.", flush=True)
        else:
            print(f"\n[{finish_now}] Pipeline failed with return code {result.returncode}.", flush=True)
            
    except Exception as e:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Unexpected error: {e}", flush=True)

def main():
    # 切换到脚本所在目录，确保相对路径正确
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir:
        os.chdir(script_dir)

    print(f"StarRocks PR Analytics Daemon started.", flush=True)
    print(f"Interval: {INTERVAL} seconds (1 hour)", flush=True)
    print(f"PID: {os.getpid()}", flush=True)
    
    try:
        while True:
            run_pipeline()
            
            next_run = time.time() + INTERVAL
            next_run_str = datetime.fromtimestamp(next_run).strftime("%H:%M:%S")
            print(f"\nWaiting for 1 hour. Next run at approx: {next_run_str}", flush=True)
            
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("\nDaemon stopped by user.")
        sys.exit(0)

if __name__ == "__main__":
    main()
