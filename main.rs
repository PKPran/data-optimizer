use chrono::Local;
use futures::stream::StreamExt;
use postgres::{Client, Error, NoTls, Row};
use rayon::prelude::*;
use std::time::Instant;
use tokio;
use xlsxwriter::*;

struct ChunkData {
    rows: Vec<Row>,
    chunk_index: usize,
    columns: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    // Database connection
    let conn_str = "host=localhost user=user dbname=test_db";
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls).await?;

    // Handle connection in background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Configure chunk size and workers
    const CHUNK_SIZE: i64 = 100_000;
    const MAX_WORKERS: usize = 4;

    // Create Excel file with timestamp
    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let excel_file = format!("large_data_export_{}.xlsx", timestamp);
    let workbook = Workbook::new(&excel_file);
    let mut sheet = workbook.add_worksheet(Some("Data"))?;

    // Get total count
    let row = client
        .query_one("SELECT COUNT(*) FROM your_table", &[])
        .await?;
    let total_rows: i64 = row.get(0);
    println!("Total rows to process: {}", total_rows);

    // Process chunks in parallel
    let mut futures = Vec::new();
    for chunk_index in 0..((total_rows + CHUNK_SIZE - 1) / CHUNK_SIZE) {
        let offset = chunk_index * CHUNK_SIZE;
        futures.push(fetch_chunk(&client, offset, CHUNK_SIZE, chunk_index as usize));
    }

    // Process and write chunks
    for (i, future) in futures.into_iter().enumerate() {
        if let Ok(chunk) = future.await {
            let start_row = i * CHUNK_SIZE as usize;
            
            // Write headers for first chunk
            if i == 0 {
                for (col_idx, column_name) in chunk.columns.iter().enumerate() {
                    sheet.write_string(0, col_idx as u16, column_name, None)?;
                }
            }

            // Write data
            for (row_idx, row) in chunk.rows.iter().enumerate() {
                for col_idx in 0..chunk.columns.len() {
                    let value = row.get::<_, String>(col_idx);
                    sheet.write_string(
                        (start_row + row_idx + 1) as u32,
                        col_idx as u16,
                        &value,
                        None,
                    )?;
                }
            }

            println!(
                "Processed chunk {}: rows {} to {}",
                i + 1,
                start_row,
                start_row + chunk.rows.len()
            );
        }
    }

    // Close workbook
    workbook.close()?;

    let duration = start_time.elapsed();
    println!("\nExport completed in {:.2?}", duration);
    println!("Data exported to: {}", excel_file);

    Ok(())
}

async fn fetch_chunk(
    client: &tokio_postgres::Client,
    offset: i64,
    chunk_size: i64,
    chunk_index: usize,
) -> Result<ChunkData, Error> {
    let rows = client
        .query(
            "SELECT * FROM your_table ORDER BY id LIMIT $1 OFFSET $2",
            &[&chunk_size, &offset],
        )
        .await?;

    // Get column names from the first query
    let columns = if chunk_index == 0 {
        client
            .query(
                "SELECT column_name FROM information_schema.columns 
                 WHERE table_name = 'your_table' 
                 ORDER BY ordinal_position",
                &[],
            )
            .await?
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect()
    } else {
        Vec::new()
    };

    Ok(ChunkData {
        rows,
        chunk_index,
        columns,
    })
}
