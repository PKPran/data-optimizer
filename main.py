import psycopg2
from psycopg2 import Error
import pandas as pd
from datetime import datetime
import concurrent.futures
from io import StringIO
import time


def fetch_chunk(connection, offset, chunk_size):
    """Fetch a chunk of data using server-side cursor"""
    with connection.cursor("server_side_cursor") as cursor:
        cursor.execute(
            """
            SELECT * 
            FROM your_table 
            ORDER BY id  -- Replace 'id' with your primary key
            LIMIT %s OFFSET %s
        """,
            (chunk_size, offset),
        )
        chunk_data = cursor.fetchall()
        if chunk_data:
            # Get column names from cursor description
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(chunk_data, columns=columns)
    return None


def process_and_save_data():
    try:
        # Connection parameters
        connection = psycopg2.connect(host="localhost", database="test_db", user="user")

        start_time = time.time()

        # Configure chunk size and parallel workers
        CHUNK_SIZE = 100000  # Adjust based on your memory capacity
        MAX_WORKERS = 4  # Adjust based on your CPU cores

        # Create Excel writer with performance options
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = f"large_data_export_{timestamp}.xlsx"
        writer = pd.ExcelWriter(
            excel_file, engine="xlsxwriter", options={"constant_memory": True}
        )

        # Get total count of rows
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM your_table")
            total_rows = cursor.fetchone()[0]

        print(f"Total rows to process: {total_rows}")

        # Process data in chunks using parallel execution
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for offset in range(0, total_rows, CHUNK_SIZE):
                futures.append(
                    executor.submit(fetch_chunk, connection, offset, CHUNK_SIZE)
                )

            # Process completed chunks and write to Excel
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                chunk_df = future.result()
                if chunk_df is not None:
                    # Write chunk to Excel file
                    start_row = i * CHUNK_SIZE
                    chunk_df.to_excel(
                        writer,
                        sheet_name="Data",
                        startrow=start_row + 1 if i > 0 else 0,
                        header=True if i == 0 else False,
                        index=False,
                    )

                    print(
                        f"Processed chunk {i+1}: rows {start_row} to {start_row + len(chunk_df)}"
                    )

        # Save and close Excel file
        writer.close()

        end_time = time.time()
        print(f"\nExport completed in {end_time - start_time:.2f} seconds")
        print(f"Data exported to: {excel_file}")

    except (Exception, Error) as error:
        print("Error:", error)

    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection closed")


if __name__ == "__main__":
    process_and_save_data()
