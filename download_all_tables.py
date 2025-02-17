import wrds
import configparser
import sys
import traceback
import os
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import time
import random

def print_progress_header():
    """打印進度標題"""
    print("\n" + "="*80)
    print("WRDS 資料下載進度")
    print("="*80)

def print_step(step_number, total_steps, description):
    """打印步驟信息"""
    print(f"\n[步驟 {step_number}/{total_steps}] {description}")
    print("-"*50)

def format_size(size_bytes):
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"

def print_error(error_msg, error_obj=None):
    """打印錯誤信息"""
    print("\n" + "="*50)
    print("錯誤信息:")
    print("-" * 20)
    print(error_msg)
    if error_obj:
        print("\n詳細錯誤:")
        print("-" * 20)
        print(traceback.format_exc())
    print("="*50 + "\n")

def get_wrds_connection():
    try:
        print_step(1, 5, "連接到 WRDS 數據庫")
        # 讀取配置文件
        config = configparser.ConfigParser()
        config.read('config.ini')
        
        # 從配置文件獲取認證信息
        username = config['WRDS']['username']
        password = config['WRDS']['password']
        
        print(f"正在使用帳號 {username} 連接到 WRDS...")
        db = wrds.Connection(wrds_username=username, 
                           wrds_password=password, 
                           autoconnect=True)
        print("✓ 成功連接到 WRDS 數據庫")
        return db
    except Exception as e:
        print_error(f"錯誤: 連接 WRDS 失敗", e)
        sys.exit(1)

def check_access(db, library):
    """檢查是否有訪問權限"""
    try:
        sql = f"""
        SELECT 1
        FROM information_schema.schemata
        WHERE schema_name = '{library}'
        AND has_schema_privilege(current_user, schema_name, 'USAGE')
        """
        result = db.raw_sql(sql)
        return len(result) > 0
    except:
        return False

def get_table_list(db, library):
    """獲取資料庫中的表格列表"""
    try:
        # 首先獲取表格列表
        sql = f"""
        SELECT table_name
        FROM information_schema.tables 
        WHERE table_schema = '{library}'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        tables = db.raw_sql(sql)
        
        result = []
        # 對每個表格分別獲取大小和行數
        for table in tables.values:
            table_name = table[0]
            try:
                # 獲取表格大小
                size_sql = f"""
                SELECT pg_size_pretty(pg_total_relation_size('{library}.{table_name}')) as size,
                       pg_total_relation_size('{library}.{table_name}') as size_bytes
                """
                size_result = db.raw_sql(size_sql)
                
                # 獲取行數
                count_sql = f"""
                SELECT COUNT(*) as row_count 
                FROM {library}.{table_name}
                """
                count_result = db.raw_sql(count_sql)
                
                if not size_result.empty and not count_result.empty:
                    size_pretty = size_result.iloc[0]['size']
                    size_bytes = size_result.iloc[0]['size_bytes']
                    row_count = count_result.iloc[0]['row_count']
                    result.append((table_name, size_pretty, size_bytes, row_count))
                    
            except Exception as table_error:
                print(f"  - 警告: 獲取表格 {library}.{table_name} 的詳細信息時出錯: {str(table_error)}")
                continue
                
        return result
    except Exception as e:
        print(f"獲取 {library} 的表格列表時出錯: {str(e)}")
        return []

def check_existing_download(output_dir, library, table):
    """檢查表格是否已經下載過"""
    try:
        db_dir = os.path.join(output_dir, library)
        if not os.path.exists(db_dir):
            return False, None
            
        # 檢查是否有該表格的任何下載文件
        files = [f for f in os.listdir(db_dir) if f.startswith(f"{table}_") and f.endswith(".csv")]
        if not files:
            return False, None
            
        # 獲取最新的下載文件
        latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(db_dir, x)))
        file_path = os.path.join(db_dir, latest_file)
        
        # 檢查文件是否有效
        try:
            df = pd.read_csv(file_path, nrows=1)  # 只讀取第一行來驗證文件
            if df is not None and not df.empty:
                return True, file_path
        except:
            return False, None
            
        return False, None
    except Exception as e:
        print(f"  - 警告: 檢查已存在的下載時出錯: {str(e)}")
        return False, None

def get_random_delay():
    """獲取隨機延遲時間"""
    return random.uniform(1.5, 3.5)

def should_take_break(downloads_count):
    """判斷是否需要休息"""
    return downloads_count > 0 and downloads_count % 5 == 0

def take_break():
    """休息一段時間"""
    delay = random.uniform(30, 60)
    print(f"\n為避免頻繁訪問，休息 {delay:.1f} 秒...")
    time.sleep(delay)

def get_next_download_time(last_download_time):
    """獲取下次允許下載的時間"""
    min_interval = timedelta(seconds=2)
    return last_download_time + min_interval

def download_table(db, library, table, output_dir, total_rows=None, max_rows=None, last_download_time=None):
    """下載指定的表格"""
    try:
        # 檢查是否需要等待
        if last_download_time:
            next_download_time = get_next_download_time(last_download_time)
            wait_time = (next_download_time - datetime.now()).total_seconds()
            if wait_time > 0:
                print(f"  - 等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)

        # 檢查是否已經下載過
        already_exists, existing_file = check_existing_download(output_dir, library, table)
        if already_exists:
            print(f"  - 表格已存在: {existing_file}")
            try:
                df = pd.read_csv(existing_file)
                return True, existing_file, len(df), datetime.now()
            except:
                print("  - 警告: 現有文件可能已損壞，將重新下載")
        
        # 如果沒有下載過或文件損壞，執行下載
        # 創建資料庫目錄
        db_dir = os.path.join(output_dir, library)
        os.makedirs(db_dir, exist_ok=True)
        
        # 設定輸出文件名
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(db_dir, f"{table}_{current_time}.csv")
        
        # 構建查詢
        if max_rows:
            sql = f"""
            SELECT *
            FROM {library}.{table}
            LIMIT {max_rows}
            """
        else:
            sql = f"""
            SELECT *
            FROM {library}.{table}
            """
        
        # 執行查詢並保存
        print(f"  - 正在查詢數據 ({total_rows:,} 行)...")
        df = db.raw_sql(sql)
        
        # 如果數據為空，返回失敗
        if df is None or df.empty:
            print("  - 警告: 查詢返回空數據")
            return False, None, 0, datetime.now()
            
        print(f"  - 正在保存到文件...")
        df.to_csv(output_file, index=False)
        
        # 添加隨機延遲
        delay = get_random_delay()
        print(f"  - 延遲 {delay:.1f} 秒...")
        time.sleep(delay)
        
        return True, output_file, len(df), datetime.now()
    except Exception as e:
        print_error(f"下載表格 {library}.{table} 時出錯", e)
        return False, None, 0, datetime.now()

def save_catalog(output_dir, catalog_data):
    """保存資料庫目錄信息"""
    try:
        catalog_file = os.path.join(output_dir, "wrds_catalog.csv")
        df = pd.DataFrame(catalog_data, columns=[
            "資料庫", "表格名稱", "大小", "行數", "上次掃描時間"
        ])
        df.to_csv(catalog_file, index=False, encoding='utf-8-sig')
        print(f"資料庫目錄已更新: {catalog_file}")
    except Exception as e:
        print(f"保存資料庫目錄時出錯: {str(e)}")

def load_catalog(output_dir):
    """讀取資料庫目錄信息"""
    try:
        catalog_file = os.path.join(output_dir, "wrds_catalog.csv")
        if os.path.exists(catalog_file):
            df = pd.read_csv(catalog_file, encoding='utf-8-sig')
            catalog_data = df.values.tolist()
            print(f"已載入現有資料庫目錄: {catalog_file}")
            return catalog_data
        return None
    except Exception as e:
        print(f"讀取資料庫目錄時出錯: {str(e)}")
        return None

def update_catalog(db, output_dir, force_update=False):
    """更新資料庫目錄"""
    try:
        # 檢查現有目錄
        existing_catalog = load_catalog(output_dir)
        if existing_catalog and not force_update:
            print("使用現有資料庫目錄")
            return existing_catalog
            
        print("開始更新資料庫目錄...")
        libraries = sorted(db.list_libraries())
        if not libraries:
            print("錯誤: 沒有找到任何資料庫")
            return None
            
        accessible_libraries = [lib for lib in libraries if check_access(db, lib)]
        if not accessible_libraries:
            print("錯誤: 沒有可訪問的資料庫")
            return None
            
        print(f"發現 {len(libraries)} 個資料庫，其中 {len(accessible_libraries)} 個可訪問")
        
        catalog_data = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for lib in tqdm(accessible_libraries, desc="掃描資料庫"):
            tables = get_table_list(db, lib)
            for table_info in tables:
                table_name, size_str, size_bytes, row_count = table_info
                catalog_data.append([
                    lib,
                    table_name,
                    size_str,
                    row_count,
                    current_time
                ])
        
        # 保存目錄
        save_catalog(output_dir, catalog_data)
        return catalog_data
        
    except Exception as e:
        print_error(f"更新資料庫目錄時出錯", e)
        return None

def download_all_tables():
    try:
        print_progress_header()
        
        # 步驟 1: 創建輸出目錄
        print_step(1, 5, "準備下載環境")
        output_dir = "wrds_data"
        os.makedirs(output_dir, exist_ok=True)
        print("✓ 創建輸出目錄完成")
        
        # 步驟 2: 連接到 WRDS
        print_step(2, 5, "連接到 WRDS 數據庫")
        db = get_wrds_connection()
        
        # 步驟 3: 獲取或更新資料庫目錄
        print_step(3, 5, "獲取資料庫目錄")
        catalog_data = update_catalog(db, output_dir)
        if not catalog_data:
            print("錯誤: 無法獲取資料庫目錄")
            return
            
        print(f"目錄中共有 {len(catalog_data)} 個表格")
        
        # 步驟 4: 檢查已下載的表格
        print_step(4, 5, "檢查下載狀態")
        all_tables = []
        total_size = 0
        skipped_tables = []
        
        for lib, table_name, size_str, row_count, _ in catalog_data:
            # 檢查是否已下載
            already_exists, existing_file = check_existing_download(output_dir, lib, table_name)
            if already_exists:
                skipped_tables.append((lib, table_name, existing_file))
                continue
                
            all_tables.append((lib, table_name, row_count))
        
        # 顯示跳過的表格信息
        if skipped_tables:
            print("\n以下表格已存在，將被跳過:")
            for lib, table, file in skipped_tables:
                print(f"  - {lib}.{table} -> {file}")
        
        if not all_tables:
            print("\n所有表格都已下載完成")
            return
            
        print(f"\n需要下載 {len(all_tables)} 個表格")
        
        # 步驟 5: 開始下載
        print_step(5, 5, "開始下載數據")
        download_log = []
        successful_downloads = 0
        total_rows_downloaded = 0
        last_download_time = None
        
        for i, (lib, table_name, row_count) in enumerate(all_tables, 1):
            print(f"\n表格 {i}/{len(all_tables)}: {lib}.{table_name}")
            print(f"預計行數: {row_count:,}")
            
            # 檢查是否需要休息
            if should_take_break(i):
                take_break()
            
            try:
                success, file_path, downloaded_rows, download_time = download_table(
                    db, lib, table_name, output_dir, 
                    total_rows=row_count,
                    last_download_time=last_download_time
                )
                last_download_time = download_time
                
                status = "成功" if success else "失敗"
                if success:
                    successful_downloads += 1
                    total_rows_downloaded += downloaded_rows
                
                # 計算總進度
                progress = (i / len(all_tables)) * 100
                print(f"總進度: {progress:.1f}% ({i}/{len(all_tables)})")
                
                # 記錄下載信息
                download_log.append([
                    lib,
                    table_name,
                    f"{downloaded_rows:,}",
                    status,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    file_path if success else ""
                ])
                
            except Exception as e:
                print(f"下載失敗: {str(e)}")
                download_log.append([
                    lib,
                    table_name,
                    "0",
                    "失敗",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    str(e)
                ])
        
        if download_log:  # 只有在有下載記錄時才保存日誌
            # 保存下載日誌
            log_file = os.path.join(output_dir, f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            pd.DataFrame(download_log, columns=[
                "資料庫", "表格名稱", "下載行數", "狀態", "下載時間", "文件路徑/錯誤信息"
            ]).to_csv(log_file, index=False, encoding='utf-8-sig')
            
            # 打印最終統計
            print("\n" + "="*50)
            print("下載完成統計")
            print("="*50)
            print(f"跳過表格數: {len(skipped_tables)}")
            print(f"下載表格數: {len(all_tables)}")
            print(f"成功下載: {successful_downloads}")
            print(f"失敗數量: {len(all_tables) - successful_downloads}")
            print(f"成功率: {(successful_downloads/len(all_tables)*100):.1f}%")
            print(f"總行數: {total_rows_downloaded:,}")
            print(f"\n下載日誌已保存到: {log_file}")
        else:
            print("\n沒有任何新表格需要下載")
        
    except Exception as e:
        print_error("程序執行過程中發生錯誤", e)
    finally:
        try:
            db.close()
            print("\n資料庫連接已關閉")
        except:
            pass

if __name__ == "__main__":
    try:
        # 檢查是否需要強制更新目錄
        force_update = "--update-catalog" in sys.argv
        if force_update:
            print("將強制更新資料庫目錄")
        
        download_all_tables()
    except KeyboardInterrupt:
        print("\n\n程序被用戶中斷")
        sys.exit(0)
    except Exception as e:
        print_error("未預期的錯誤", e)
        sys.exit(1) 