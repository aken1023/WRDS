import wrds
import pandas as pd
from datetime import datetime, timedelta

def fetch_wrds_data(table_name):
    try:
        # Connect to WRDS with credentials
        conn = wrds.Connection(wrds_username='crysta_hwg', wrds_password='Aa123456!')
        
        # Construct the SQL query
        sql_query = f"""
            SELECT *
            FROM {table_name}
        """
        
        print(f"Fetching data from: {table_name}")
        
        # Execute the query and load into DataFrame
        df = conn.raw_sql(sql_query)
        
        # Save to CSV
        current_date = datetime.now().strftime('%Y%m%d')
        table_name_clean = table_name.replace('.', '_')
        output_filename = f'{table_name_clean}_{current_date}.csv'
        df.to_csv(output_filename, index=False)
        
        print(f"Data successfully downloaded and saved to {output_filename}")
        
        # Close the connection
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    # Get table name from user input
    print("Please enter the WRDS table name (e.g., comp_na_daily_all.company):")
    table_name = input().strip()
    
    if table_name:
        fetch_wrds_data(table_name)
    else:
        print("No table name provided. Please run the script again with a valid table name.") 