use postgres::{Client, NoTls};
use std::time::Instant;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::io::BufRead;
use rust_xlsxwriter::{Workbook, Format};
use rayon::prelude::*;
use std::sync::Mutex;

const CHUNK_SIZE: usize = 250_000;
const BUFFER_CAPACITY: usize = 32768;
const EXCEL_BATCH_SIZE: usize = 5000;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let conn_str = "postgresql://user:password@localhost/test_db";
    
    const TOTAL_ROWS: usize = 1_000_000;
    println!("Total rows in database: {}", TOTAL_ROWS);

    // Create workbook and wrap in Mutex for thread safety
    let workbook = Arc::new(Mutex::new(Workbook::new()));
    
    // Simplified header format
    let header_format = Format::new().set_bold();

    let headers = ["id", "col1", "col2", "col3", "col4", "col5", 
                  "col6", "col7", "col8", "col9", "col10"];

    // Progress counter
    let row_counter = Arc::new(AtomicU32::new(0));
    let progress_interval = (TOTAL_ROWS / 20) as u32;

    // Configure thread pool for optimal performance
    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build_global()?;

    // Process database in chunks
    let chunks: Vec<_> = (0..4)
        .map(|i| {
            let start = i * CHUNK_SIZE + 1;
            let end = if i == 3 { TOTAL_ROWS + 1 } else { (i + 1) * CHUNK_SIZE + 1 };
            (start, end)
        })
        .collect();

    // Process chunks in parallel
    chunks.par_iter().enumerate().try_for_each(|(sheet_idx, &(start_id, end_id))| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut worksheet_data: Vec<(u32, u16, String)> = Vec::with_capacity(CHUNK_SIZE * headers.len());
        
        // Connect to database and fetch rows
        let mut conn = Client::connect(conn_str, NoTls)?;

        let copy_sql = format!(
            "COPY (
                SELECT id::text, col1::text, col2::text, col3::text, col4::text, 
                       col5::text, col6::text, col7::text, col8::text, col9::text, 
                       col10::text 
                FROM test_table 
                WHERE id >= {} AND id < {}
                ORDER BY id
            ) TO STDOUT WITH (FORMAT CSV)", 
            start_id, end_id
        );

        let mut current_row = 1;
        let reader = std::io::BufReader::with_capacity(
            BUFFER_CAPACITY,
            conn.copy_out(&copy_sql)?
        );

        // Pre-allocate string buffer
        let mut line_buffer = String::with_capacity(256);

        for line in reader.lines() {
            line_buffer.clear();
            line_buffer.push_str(&line?);
            
            for (col, value) in line_buffer.split(',').enumerate() {
                worksheet_data.push((current_row, col as u16, value.to_string()));
            }
            current_row += 1;

            let count = row_counter.fetch_add(1, Ordering::SeqCst);
            if count % progress_interval == 0 {
                println!("Progress: {:.1}%", (count as f64 / TOTAL_ROWS as f64) * 100.0);
            }
        }

        // Write to Excel in one batch
        let mut workbook = workbook.lock().map_err(|_| "Failed to lock workbook")?;
        let worksheet = workbook.add_worksheet();
        worksheet.set_name(&format!("Sheet_{}", sheet_idx + 1))?;

        // Write headers with minimal formatting
        for (col, &header) in headers.iter().enumerate() {
            worksheet.write_string_with_format(0, col as u16, header, &header_format)?;
        }

        // Write data in larger batches
        for chunk in worksheet_data.chunks(EXCEL_BATCH_SIZE) {
            for &(row, col, ref value) in chunk {
                worksheet.write_string(row, col, value)?;
            }
        }

        println!("Completed sheet {} ({} rows)", sheet_idx + 1, current_row - 1);
        Ok(())
    })?;

    // Save the workbook
    println!("Saving Excel file...");
    let mut workbook_guard = workbook.lock().map_err(|_| "Failed to lock workbook")?;
    workbook_guard.save("output.xlsx")?;

    let duration = start.elapsed();
    println!("Export completed in {:.2} seconds", duration.as_secs_f64());

    Ok(())
}