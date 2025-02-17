import schedule
import time
from datetime import datetime
import subprocess
import sys
import os

def check_time_range():
    """檢查當前時間是否在指定範圍內（01:00-08:00）"""
    current_hour = datetime.now().hour
    return 1 <= current_hour < 8

def run_download():
    """執行下載腳本"""
    if check_time_range():
        print(f"\n開始執行下載任務 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            # 使用 subprocess 執行下載腳本
            result = subprocess.run([sys.executable, 'download_wrds_tables.py'], 
                                 capture_output=True, 
                                 text=True)
            
            # 記錄執行結果
            log_dir = 'logs'
            os.makedirs(log_dir, exist_ok=True)
            
            current_date = datetime.now().strftime('%Y%m%d')
            log_file = os.path.join(log_dir, f'download_log_{current_date}.txt')
            
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*50}\n")
                f.write(f"執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"執行結果:\n{result.stdout}\n")
                if result.stderr:
                    f.write(f"錯誤信息:\n{result.stderr}\n")
                f.write(f"{'='*50}\n")
            
            print(f"下載任務完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            print(f"執行過程中發生錯誤: {str(e)}")
    else:
        print(f"當前時間 {datetime.now().strftime('%H:%M:%S')} 不在執行時間範圍內")

def main():
    print("啟動 WRDS 數據下載排程...")
    print("下載任務將在每天 01:00-08:00 之間執行")
    
    # 設置每小時檢查一次
    schedule.every().hour.at(":00").do(run_download)
    
    # 立即執行一次檢查
    run_download()
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # 每分鐘檢查一次排程
        except KeyboardInterrupt:
            print("\n程序已停止")
            break
        except Exception as e:
            print(f"發生錯誤: {str(e)}")
            time.sleep(60)  # 發生錯誤時等待一分鐘後繼續

if __name__ == "__main__":
    main() 