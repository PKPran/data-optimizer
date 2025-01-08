use postgres::{Client, NoTls};
use rust_xlsxwriter::{Workbook, Format, Color};
use rayon::prelude::*;
use std::time::Instant;
use std::io::BufRead;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();

    // Connection settings
    let conn_str = "postgresql://user:password@localhost/test_db";
    
    const TOTAL_ROWS: usize = 1_000_000;
    const CHUNK_SIZE: usize = 250_000; // 4 chunks total
    
    println!("Total rows in database: {}", TOTAL_ROWS);
    
    // Pre-allocate with exact size
    let mut all_rows: Vec<Vec<String>> = Vec::with_capacity(TOTAL_ROWS);
    
    // Create 4 fixed chunks with exact ranges
    let chunks = vec![
        (1, 250_001),         // 1 to 250k
        (250_001, 500_001),   // 250k to 500k
        (500_001, 750_001),   // 500k to 750k
        (750_001, 1_000_001)  // 750k to 1M
    ];

    let processed_chunks = std::sync::atomic::AtomicUsize::new(0);
    
    // Use COPY command for faster data transfer
    chunks.par_iter()
        .flat_map(|(start_id, end_id)| {
            let mut conn = Client::connect(conn_str, NoTls).unwrap();
            
            // Pre-allocate chunk size
            let mut chunk_rows = Vec::with_capacity(CHUNK_SIZE);
            
            // Use COPY for faster data transfer
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
            
            conn.copy_out(&copy_sql)
                .unwrap()
                .lines()
                .filter_map(Result::ok)
                .map(|line| line.split(',').map(String::from).collect())
                .for_each(|row| chunk_rows.push(row));

            let current = processed_chunks.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            println!("Progress: {}/4 chunks processed", current);

            chunk_rows
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|row| all_rows.push(row));

    println!("\nFetched {} rows, writing to Excel...", all_rows.len());

    // Create workbook and write data
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    
    // Add headers
    let headers = ["id", "col1", "col2", "col3", "col4", "col5", 
                  "col6", "col7", "col8", "col9", "col10"];
    
    let header_format = Format::new()
        .set_bold()
        .set_background_color(Color::RGB(0xD8E4BC));
    
    for (col, header) in headers.iter().enumerate() {
        worksheet.write_string_with_format(0, col as u16, *header, &header_format)?;
        worksheet.set_column_width(col as u16, 15.0)?;
    }

    // Write data
    for (row_idx, row_data) in all_rows.iter().enumerate() {
        for (col_idx, value) in row_data.iter().enumerate() {
            worksheet.write_string((row_idx + 1) as u32, col_idx as u16, value)?;
        }
    }

    workbook.save("output.xlsx")?;

    let duration = start.elapsed();
    println!("Export completed in {:.2} seconds", duration.as_secs_f64());

    Ok(())
}