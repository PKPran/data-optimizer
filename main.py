import psycopg2
import xlsxwriter
from datetime import datetime
import concurrent.futures
import threading
import time
import os

def fetch_chunk(connection, start_id, end_id):
    """Fetch a chunk of data using server-side cursor with batch fetch"""
    cursor_name = f"cursor_{start_id}_{end_id}"
    with connection.cursor(name=cursor_name) as cursor:
        cursor.itersize = 50000  # Optimize PostgreSQL batch size
        cursor.execute(
            """
            SELECT 
                id::text,
                col1::text,
                col2::text,
                col3::text,
                col4::text,
                col5::text,
                col6::text,
                col7::text,
                col8::text,
                col9::text,
                col10::text
            FROM test_table 
            WHERE id >= %s AND id < %s
            ORDER BY id
            """,
            (start_id, end_id)
        )
        return cursor.fetchall()

def process_and_save_data():
    try:
        # Enable faster TCP keepalives
        connection = psycopg2.connect(
            host="localhost", 
            database="test_db", 
            user="user", 
            password="password",
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        
        # Ultra aggressive settings
        CHUNK_SIZE = 1000000  # 1 million rows per chunk
        MAX_WORKERS = min(32, os.cpu_count() * 4)  # More aggressive threading
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = f"large_data_export_{timestamp}.xlsx"

        # Optimized workbook settings
        workbook = xlsxwriter.Workbook(
            excel_file, 
            {
                'constant_memory': True,
                'strings_to_urls': False,
                'strings_to_formulas': False,
                'strings_to_numbers': False,
                'default_format_properties': {'font_size': 10}
            }
        )
        worksheet = workbook.add_worksheet('Data')

        # Pre-format columns for better performance
        worksheet.set_column(0, 10, 15)  # Set width for all columns at once

        headers = ['id', 'col1', 'col2', 'col3', 'col4', 'col5', 
                  'col6', 'col7', 'col8', 'col9', 'col10']
        worksheet.write_row(0, 0, headers)

        start_time = time.time()

        # Get row count and ID range
        with connection.cursor() as cursor:
            cursor.execute("SELECT MIN(id), MAX(id) FROM test_table")
            min_id, max_id = cursor.fetchone()

        # Calculate chunk boundaries
        total_range = max_id - min_id + 1
        chunk_size = total_range // MAX_WORKERS
        chunks = [
            (min_id + i * chunk_size, min_id + (i + 1) * chunk_size)
            for i in range(MAX_WORKERS - 1)
        ]
        chunks.append((min_id + (MAX_WORKERS - 1) * chunk_size, max_id + 1))

        current_row = 1
        row_lock = threading.Lock()

        def process_and_write_chunk(chunk_range):
            nonlocal current_row
            chunk_data = fetch_chunk(connection, chunk_range[0], chunk_range[1])
            with row_lock:
                nonlocal worksheet
                row = current_row
                current_row += len(chunk_data)
                for idx, data in enumerate(chunk_data):
                    worksheet.write_row(row + idx, 0, data)
                return len(chunk_data)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_and_write_chunk, chunk) for chunk in chunks]
            
            for future in concurrent.futures.as_completed(futures):
                rows_processed = future.result()
                print(f"Processed {rows_processed} rows")

        workbook.close()
        elapsed = time.time() - start_time
        print(f"\nExport completed in {elapsed:.2f} seconds")
        print(f"Average speed: {(current_row-1)/elapsed:.0f} rows/second")
        print(f"Data exported to: {excel_file}")

    except Exception as error:
        print("Error:", error)
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    process_and_save_data()
