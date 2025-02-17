import wrds
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import time
import sys
from sqlalchemy import text

def read_authorized_databases():
    """從 CSV 文件讀取授權的數據庫列表"""
    try:
        # 先讀取 CSV 並顯示列名
        df = pd.read_csv('Wharton Research Data Services.csv')
        print("CSV 文件的列名:", df.columns.tolist())
        
        # 使用第一列作為數據庫代碼
        if len(df.columns) > 0:
            product_codes = df.iloc[:, 0].tolist()
            # 清理數據：移除空值和空白字符
            product_codes = [str(code).strip() for code in product_codes if pd.notna(code) and str(code).strip()]
            return product_codes
            
    except Exception as e:
        print(f"錯誤：無法讀取授權數據庫列表")
        print(f"錯誤詳情: {str(e)}")
        print("\n嘗試讀取 CSV 文件的前幾行：")
        try:
            with open('Wharton Research Data Services.csv', 'r', encoding='utf-8') as f:
                print(f.readline())  # 顯示標題行
                print(f.readline())  # 顯示第一行數據
        except Exception as read_error:
            print(f"讀取文件時發生錯誤: {str(read_error)}")
        sys.exit(1)

def get_wrds_connection():
    """建立 WRDS 連接"""
    try:
        # 直接使用帳號密碼（注意：這些認證信息應該保密）
        conn = wrds.Connection(
            wrds_username='crysta_hwg',  # 您的 WRDS 用戶名
            wrds_password='Aa123456!',   # 您的 WRDS 密碼
            connect_args={'sslmode': 'require'},
            engine_kwargs={'future': True}
        )
        
        try:
            test_query = text("SELECT 1")
            conn.raw_sql(test_query)
            print("成功連接到 WRDS 數據庫")
            return conn
        except Exception as e:
            print(f"錯誤：連接測試失敗")
            print(f"錯誤詳情: {str(e)}")
            sys.exit(1)
            
    except Exception as e:
        print(f"錯誤：無法連接到 WRDS 數據庫")
        print(f"錯誤詳情: {str(e)}")
        sys.exit(1)

def get_tables_for_database(conn, database):
    """獲取指定數據庫中的所有表"""
    try:
        query = text("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = :schema
            AND tablename NOT LIKE 'pg_%'
        """)
        df = conn.raw_sql(query, params={'schema': database})
        return df['tablename'].tolist()
    except Exception as e:
        print(f"錯誤：無法獲取數據庫 {database} 的表格列表")
        print(f"錯誤詳情: {str(e)}")
        return []

def download_table_data(conn, schema, table, base_dir):
    """下載指定表格的數據"""
    try:
        print(f"\n開始處理表格: {schema}.{table}")
        
        sql_query = text(f"""
            SELECT *
            FROM {schema}.{table}
            LIMIT 1000
        """)
        
        df = None
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = conn.raw_sql(sql_query)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"錯誤：無法下載表格 {schema}.{table}")
                    print(f"錯誤詳情: {str(e)}")
                    return
                print(f"重試下載 {schema}.{table} (嘗試 {attempt + 1}/{max_retries})")
                time.sleep(2)
        
        if df is None or df.empty:
            print(f"警告：{schema}.{table} 返回空數據集")
            return
        
        schema_dir = os.path.join(base_dir, schema)
        os.makedirs(schema_dir, exist_ok=True)
        
        current_date = datetime.now().strftime('%Y%m%d')
        output_filename = os.path.join(schema_dir, f'{table}_{current_date}.csv')
        df.to_csv(output_filename, index=False)
        
        print(f"成功下載並保存到: {output_filename}")
        time.sleep(1)
        
    except Exception as e:
        print(f"錯誤：處理表格 {schema}.{table} 時發生錯誤")
        print(f"錯誤詳情: {str(e)}")

def fetch_authorized_data():
    """下載所有授權數據庫的數據"""
    authorized_dbs = read_authorized_databases()
    print(f"授權的數據庫: {authorized_dbs}")
    
    try:
        conn = get_wrds_connection()
        base_dir = 'wrds_data'
        os.makedirs(base_dir, exist_ok=True)
        
        for db in authorized_dbs:
            print(f"\n處理數據庫: {db}")
            tables = get_tables_for_database(conn, db)
            
            if not tables:
                print(f"警告：數據庫 {db} 中沒有找到可用的表格")
                continue
                
            print(f"發現 {len(tables)} 個表格")
            for table in tables:
                download_table_data(conn, db, table, base_dir)
                
    except Exception as e:
        print(f"錯誤：程序執行過程中發生未預期的錯誤")
        print(f"錯誤詳情: {str(e)}")
        sys.exit(1)
    finally:
        if conn and hasattr(conn, 'close'):
            try:
                conn.close()
                print("\n已關閉數據庫連接")
            except:
                pass

if __name__ == "__main__":
    fetch_authorized_data() 