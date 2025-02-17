import wrds
import configparser
from tabulate import tabulate
import sys
import traceback
import csv
from datetime import datetime
import os

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
        # 讀取配置文件
        config = configparser.ConfigParser()
        config.read('config.ini')
        
        # 從配置文件獲取認證信息
        username = config['WRDS']['username']
        password = config['WRDS']['password']
        
        print(f"\n嘗試連接到 WRDS (用戶名: {username})...")
        db = wrds.Connection(wrds_username=username, 
                           wrds_password=password, 
                           autoconnect=True)
        print("成功連接到 WRDS 數據庫\n")
        return db
    except FileNotFoundError:
        print_error("錯誤: 找不到 config.ini 文件")
        sys.exit(1)
    except KeyError:
        print_error("錯誤: config.ini 文件中缺少 WRDS 配置信息")
        sys.exit(1)
    except Exception as e:
        print_error(f"錯誤: 連接 WRDS 失敗", e)
        sys.exit(1)

def check_access(db, library):
    """檢查是否有訪問權限"""
    try:
        # 嘗試執行一個簡單的查詢來測試訪問權限
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
        sql = f"""
        SELECT table_name, 
               pg_size_pretty(pg_total_relation_size(quote_ident('{library}') || '.' || quote_ident(table_name))) as size
        FROM information_schema.tables 
        WHERE table_schema = '{library}'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        tables = db.raw_sql(sql)
        return [(table[0], table[1]) for table in tables.values]
    except Exception as e:
        print(f"獲取 {library} 的表格列表時出錯: {str(e)}")
        return []

def save_to_csv(data, filename):
    """保存數據到CSV文件"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerows(data)
        print(f"數據已保存到: {filename}")
    except Exception as e:
        print_error(f"保存CSV文件時出錯: {filename}", e)

def list_all_libraries():
    try:
        # 創建輸出目錄
        output_dir = "wrds_info"
        os.makedirs(output_dir, exist_ok=True)
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 連接到 WRDS
        db = get_wrds_connection()
        
        # 獲取所有資料庫列表
        print("正在獲取資料庫列表和訪問權限...")
        try:
            libraries = sorted(db.list_libraries())  # 排序資料庫列表
        except Exception as e:
            print_error("獲取資料庫列表失敗", e)
            return
        
        if not libraries:
            print("\n沒有找到可用的資料庫")
            return
            
        # 格式化輸出
        print("\n您的 WRDS 資料庫訪問權限:")
        print("=" * 80)
        
        # 準備資料庫列表數據
        libraries_data = [["編號", "資料庫名稱", "描述", "訪問權限"]]  # CSV標題行
        accessible_count = 0
        
        for i, lib in enumerate(libraries, 1):
            # 檢查訪問權限
            has_access = check_access(db, lib)
            if has_access:
                accessible_count += 1
            
            # 獲取資料庫描述
            try:
                description = db.get_library_info(lib)
            except:
                description = ""
            
            # 添加訪問狀態
            access_status = "可訪問" if has_access else "無權限"
            row = [i, lib, description, access_status]
            libraries_data.append(row)
            
            # 打印到終端
            print(f"處理中: {lib} ({access_status})")
        
        # 保存資料庫列表
        libraries_file = os.path.join(output_dir, f"wrds_libraries_{current_time}.csv")
        save_to_csv(libraries_data, libraries_file)
        
        # 準備表格列表數據
        tables_data = [["資料庫名稱", "表格名稱", "大小"]]  # CSV標題行
        
        # 獲取每個可訪問資料庫的表格
        for lib in libraries:
            if check_access(db, lib):
                tables = get_table_list(db, lib)
                for table_name, size in tables:
                    tables_data.append([lib, table_name, size])
        
        # 保存表格列表
        tables_file = os.path.join(output_dir, f"wrds_tables_{current_time}.csv")
        save_to_csv(tables_data, tables_file)
        
        # 打印統計信息
        print(f"\n統計信息:")
        print(f"總資料庫數量: {len(libraries)}")
        print(f"可訪問數量: {accessible_count}")
        print(f"訪問權限比例: {(accessible_count/len(libraries)*100):.1f}%")
        print(f"總表格數量: {len(tables_data)-1}")
        
        # 保存統計信息
        stats_data = [
            ["統計項目", "數值"],
            ["總資料庫數量", len(libraries)],
            ["可訪問數量", accessible_count],
            ["訪問權限比例", f"{(accessible_count/len(libraries)*100):.1f}%"],
            ["總表格數量", len(tables_data)-1]
        ]
        stats_file = os.path.join(output_dir, f"wrds_stats_{current_time}.csv")
        save_to_csv(stats_data, stats_file)
        
        # 關閉連接
        try:
            db.close()
            print("\n連接已關閉")
        except:
            pass
        
        print("\n所有數據已保存到 wrds_info 目錄")
        
    except Exception as e:
        print_error("程序執行過程中發生錯誤", e)
        sys.exit(1)

if __name__ == "__main__":
    try:
        list_all_libraries()
    except KeyboardInterrupt:
        print("\n\n程序被用戶中斷")
        sys.exit(0)
    except Exception as e:
        print_error("未預期的錯誤", e)
        sys.exit(1) 