use postgres::{Client, NoTls};
use rust_xlsxwriter::{Workbook, Format, Color};
use rayon::prelude::*;
use std::time::Instant;
use std::io::BufRead;

// Custom type for storing row data more efficiently
type RowData = Vec<Box<str>>;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let conn_str = "postgresql://user:password@localhost/test_db";
    
    const TOTAL_ROWS: usize = 1_000_000;
    const CHUNK_SIZE: usize = 200_000; // 5 chunks for better memory distribution
    
    println!("Total rows in database: {}", TOTAL_ROWS);
    
    // Pre-calculate chunk ranges
    let chunks: Vec<_> = (0..5)
        .map(|i| {
            let start = i * CHUNK_SIZE + 1;
            let end = if i == 4 { TOTAL_ROWS + 1 } else { (i + 1) * CHUNK_SIZE + 1 };
            (start, end)
        })
        .collect();

    // Process chunks in parallel with pre-allocated buffers
    let chunks_data: Vec<Vec<RowData>> = chunks.par_iter()
        .map(|&(start_id, end_id)| {
            let mut conn = Client::connect(conn_str, NoTls).unwrap();
            let mut chunk_rows = Vec::with_capacity(CHUNK_SIZE);
            
            let copy_sql = format!(
                "COPY (
                    SELECT id::text, col1::text, col2::text, col3::text, col4::text, 
                           col5::text, col6::text, col7::text, col8::text, col9::text, 
                           col10::text 
                    FROM test_table 
                    WHERE id >= {} AND id < {}
                ) TO STDOUT WITH (FORMAT CSV)", 
                start_id, end_id
            );
            
            // Process rows directly into Box<str> to avoid extra allocations
            conn.copy_out(&copy_sql)
                .unwrap()
                .lines()
                .filter_map(Result::ok)
                .map(|line| {
                    line.split(',')
                        .map(|s| s.to_string().into_boxed_str())
                        .collect()
                })
                .for_each(|row| chunk_rows.push(row));

            println!("Fetched rows {} to {}", start_id, end_id);
            chunk_rows
        })
        .collect();

    println!("\nWriting to Excel...");

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.set_screen_gridlines(false);

    // Write headers
    let headers = ["id", "col1", "col2", "col3", "col4", "col5", 
                  "col6", "col7", "col8", "col9", "col10"];
    
    let header_format = Format::new()
        .set_bold()
        .set_background_color(Color::RGB(0xD8E4BC));
    
    for (col, &header) in headers.iter().enumerate() {
        worksheet.write_string_with_format(0, col as u16, header, &header_format)?;
        worksheet.set_column_width(col as u16, 15.0)?;
    }

    // Write data chunks with progress tracking
    let mut current_row: u32 = 1;
    let progress_interval: u32 = (TOTAL_ROWS / 20) as u32; // Show progress every 5%

    for chunk in chunks_data {
        for row_data in chunk {
            for (col, value) in row_data.iter().enumerate() {
                worksheet.write_string(current_row, col as u16, value.as_ref())
                    .expect("Failed to write cell");
            }
            
            if current_row % progress_interval == 0 {
                println!("Progress: {:.1}%", (current_row as f64 / TOTAL_ROWS as f64) * 100.0);
            }
            
            current_row += 1;
        }
    }

    println!("Saving workbook...");
    workbook.save("output.xlsx")?;

    let duration = start.elapsed();
    println!("Export completed in {:.2} seconds", duration.as_secs_f64());

    Ok(())
}