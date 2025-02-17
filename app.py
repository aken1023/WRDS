from flask import Flask, render_template, request, send_file, jsonify
import wrds
import pandas as pd
from datetime import datetime
import os
import threading
import time
import sys
from waitress import serve
import configparser
import traceback
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, text
from collections import deque

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# 讀取配置文件
config = configparser.ConfigParser()
config.read('config.ini')

# 載入環境變量
load_dotenv()

# 全局變數來追蹤下載進度
download_status = {
    'status': 'idle',
    'step': 0,
    'progress': 0,
    'filename': '',
    'error': None
}

# 創建錯誤日誌列表（最多保存100條記錄）
error_logs = deque(maxlen=100)

def log_error(error_msg, error_obj=None):
    """記錄錯誤到日誌"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    details = traceback.format_exc() if error_obj else None
    
    error_logs.append({
        'timestamp': timestamp,
        'message': error_msg,
        'details': details
    })
    
    # 同時輸出到控制台
    print("\n" + "="*50)
    print("錯誤發生時間:", timestamp)
    print("錯誤訊息:", error_msg)
    if details:
        print("\n詳細錯誤信息:")
        print(details)
    print("="*50 + "\n")

def reset_status():
    global download_status
    download_status.update({
        'status': 'idle',
        'step': 0,
        'progress': 0,
        'filename': '',
        'error': None
    })

def update_status(step, status='processing', **kwargs):
    global download_status
    download_status.update({
        'status': status,
        'step': step,
        **kwargs
    })

def get_wrds_connection():
    try:
        # 從環境變量獲取連接信息
        username = os.getenv('WRDS_USERNAME')
        password = os.getenv('WRDS_PASSWORD')
        
        if not username or not password:
            raise ValueError("未設置 WRDS 帳號或密碼")
            
        # 建立連接
        conn = psycopg2.connect(
            host='wrds-pgdata.wharton.upenn.edu',
            port=9737,  # 修改為正確的端口
            database='wrds',
            user=username,
            password=password,
            sslmode='require'
        )
        
        print(f"成功連接到 WRDS 數據庫 (用戶名: {username})")
        return conn
        
    except Exception as e:
        error_msg = f"WRDS 連接錯誤: {str(e)}"
        log_error(error_msg, e)
        return None

def fetch_wrds_data(table_name):
    try:
        print(f"\n開始下載表格: {table_name}")
        
        # 檢查表格名稱格式
        if '.' not in table_name:
            error_msg = "請使用 'schema.table_name' 格式（例如：'ciqsamp.capstrct'）"
            update_status(0, status='error', error=error_msg)
            raise Exception(error_msg)
        
        schema_name, table_name = table_name.split('.')
        
        # 步驟 1: 連接到 WRDS
        update_status(0, progress=10, status='連接到 WRDS 數據庫...')
        engine = create_engine(f"postgresql://{os.getenv('WRDS_USERNAME')}:{os.getenv('WRDS_PASSWORD')}@{os.getenv('WRDS_HOST')}:{os.getenv('WRDS_PORT')}/{os.getenv('WRDS_DB')}")
        
        # 步驟 2: 檢查表格是否存在
        update_status(1, progress=20, status='檢查表格是否存在...')
        check_query = text("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = :schema 
                AND table_name = :table
            )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(check_query, {"schema": schema_name, "table": table_name}).scalar()
            if not result:
                error_msg = f"表格 {schema_name}.{table_name} 不存在"
                update_status(0, status='error', error=error_msg)
                raise Exception(error_msg)
            
            # 步驟 3: 獲取表格大小信息
            update_status(2, progress=30, status='獲取表格信息...')
            size_query = text("""
                SELECT COUNT(*) 
                FROM {}.{}
            """.format(schema_name, table_name))
            
            total_rows = conn.execute(size_query).scalar()
            print(f"表格總行數: {total_rows}")
            
            # 步驟 4: 分批下載數據
            update_status(3, progress=40, status=f'開始下載數據 (總計 {total_rows:,} 行)...')
            
            # 設置批次大小
            batch_size = 100000  # 每次下載10萬行
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            all_data = []
            downloaded_rows = 0
            for batch_num in range(total_batches):
                offset = batch_num * batch_size
                batch_query = text(f"""
                    SELECT * 
                    FROM {schema_name}.{table_name}
                    LIMIT :batch_size OFFSET :offset
                """)
                
                print(f"下載批次 {batch_num + 1}/{total_batches}")
                batch_df = pd.read_sql_query(
                    batch_query, 
                    conn, 
                    params={"batch_size": batch_size, "offset": offset}
                )
                all_data.append(batch_df)
                
                # 更新已下載行數和進度
                downloaded_rows += len(batch_df)
                progress = min(90, 40 + (downloaded_rows / total_rows * 50))
                update_status(3, 
                            progress=progress, 
                            status=f'已下載 {downloaded_rows:,}/{total_rows:,} 行 ({(downloaded_rows/total_rows*100):.1f}%)')
                
                # 強制更新狀態
                time.sleep(0.1)
            
            # 步驟 5: 合併數據
            update_status(4, progress=90, status=f'合併 {len(all_data)} 個數據批次...')
            df = pd.concat(all_data, ignore_index=True)
            print(f"數據下載完成，總計 {len(df):,} 行")
            
            # 步驟 6: 保存文件
            update_status(5, progress=95, status=f'保存 {len(df):,} 行數據到文件...')
            current_date = datetime.now().strftime('%Y%m%d')
            output_filename = f'{schema_name}_{table_name}_{current_date}.csv'
            
            # 獲取完整的下載路徑
            downloads_dir = os.path.abspath('downloads')
            output_path = os.path.join(downloads_dir, output_filename)
            
            os.makedirs('downloads', exist_ok=True)
            df.to_csv(output_path, index=False)
            
            # 完成，包含完整的文件路徑信息
            update_status(6, 
                         status='complete', 
                         filename=output_filename,
                         filepath=output_path,  # 添加完整路徑
                         total_rows=len(df),    # 添加總行數
                         progress=100)
            
            print(f"\n下載完成！")
            print(f"文件名稱: {output_filename}")
            print(f"保存位置: {output_path}")
            print(f"數據行數: {len(df):,}")
            
            return output_path, output_filename
        
    except Exception as e:
        error_msg = f"數據下載錯誤: {str(e)}"
        log_error(error_msg, e)
        update_status(0, status='error', error=str(e))
        raise Exception(error_msg)

def get_available_tables():
    try:
        engine = create_engine(f"postgresql://{os.getenv('WRDS_USERNAME')}:{os.getenv('WRDS_PASSWORD')}@{os.getenv('WRDS_HOST')}:{os.getenv('WRDS_PORT')}/{os.getenv('WRDS_DB')}")
        with engine.connect() as conn:
            query = text("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """)
            result = pd.read_sql_query(query, conn)
            return result
    except Exception as e:
        error_msg = f"獲取表格列表錯誤: {str(e)}"
        log_error(error_msg, e)
        raise

@app.route('/', methods=['GET'])
def index():
    reset_status()
    return render_template('index.html')

@app.route('/start_download', methods=['POST'])
def start_download():
    table_name = request.form.get('table_name')
    if not table_name:
        return jsonify({'error': 'Please enter a table name'}), 400
    
    # 重置狀態
    reset_status()
    
    # 在背景執行下載
    thread = threading.Thread(target=fetch_wrds_data, args=(table_name,))
    thread.start()
    
    return jsonify({'status': 'processing'})

@app.route('/progress')
def progress():
    return jsonify(download_status)

@app.route('/download_file')
def download_file():
    if download_status['status'] != 'complete':
        return jsonify({'error': 'No completed download available'}), 400
    
    filename = download_status['filename']
    file_path = os.path.join('downloads', filename)
    
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(
        file_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

@app.route('/tables')
def get_tables():
    try:
        tables = get_available_tables()
        return jsonify(tables.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/wrds_libraries')
def get_wrds_libraries():
    try:
        conn = get_wrds_connection()
        if conn is None:
            return jsonify({
                'status': 'error',
                'error': '無法連接到 WRDS 數據庫，請檢查連接設置'
            })
        
        cursor = conn.cursor()
        
        # 使用子查詢來處理排序
        query = """
            WITH library_info AS (
                SELECT DISTINCT 
                    schemaname AS library_name,
                    CASE 
                        WHEN schemaname LIKE 'comp%' THEN 'Compustat'
                        WHEN schemaname LIKE 'crsp%' THEN 'CRSP'
                        WHEN schemaname LIKE 'ibes%' THEN 'IBES'
                        WHEN schemaname LIKE 'tfn%' THEN 'Thomson'
                        ELSE 'Other'
                    END AS type
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    AND schemaname NOT LIKE 'pg%'
            )
            SELECT 
                library_name,
                type
            FROM library_info
            ORDER BY 
                CASE 
                    WHEN library_name LIKE 'comp%' THEN 1
                    WHEN library_name LIKE 'crsp%' THEN 2
                    WHEN library_name LIKE 'ibes%' THEN 3
                    WHEN library_name LIKE 'tfn%' THEN 4
                    ELSE 5
                END,
                library_name;
        """
        
        cursor.execute(query)
        libraries = [{'library_name': row[0], 'type': row[1]} for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'libraries': libraries
        })
        
    except Exception as e:
        error_msg = f"獲取 WRDS 數據庫列表錯誤: {str(e)}"
        log_error(error_msg, e)
        return jsonify({
            'status': 'error',
            'error': error_msg
        })

@app.route('/database_tables/<schema_name>')
def get_database_tables(schema_name):
    try:
        conn = get_wrds_connection()
        
        # 查詢指定數據庫中的所有表格
        query = text("""
            SELECT 
                table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name))) as size,
                obj_description((quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass, 'pg_class') as description
            FROM information_schema.tables 
            WHERE table_schema = :schema
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        result = pd.read_sql_query(query, conn, params={"schema": schema_name})
        conn.close()
        
        return jsonify({
            'status': 'success',
            'tables': result.to_dict('records'),
            'total_count': len(result)
        })
        
    except Exception as e:
        error_msg = f"獲取表格列表錯誤: {str(e)}"
        log_error(error_msg, e)
        return jsonify({
            'status': 'error',
            'error': error_msg
        }), 500

@app.route('/error_log')
def error_log():
    return render_template('error_log.html', errors=list(error_logs))

if __name__ == '__main__':
    print("\n" + "="*50)
    print("啟動 WRDS 數據下載服務器...")
    print(f"服務器運行在: http://localhost:5006")
    print("="*50 + "\n")
    
    # 測試連接
    try:
        conn = get_wrds_connection()
        conn.close()
        print("WRDS 連接測試成功！")
    except Exception as e:
        print(f"WRDS 連接測試失敗：{str(e)}")
    
    serve(app, host='0.0.0.0', port=5006) 