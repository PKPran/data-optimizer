use postgres::{Client, NoTls};
use rust_xlsxwriter::{Workbook, Format, Color};
use rayon::prelude::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();

    // Connection settings
    let conn_str = "postgresql://user:password@localhost/test_db";
    
    // Get total count and ID ranges
    let mut client = Client::connect(conn_str, NoTls)?;
    let row = client.query_one("SELECT MIN(id), MAX(id) FROM test_table", &[])?;
    let min_id: i32 = row.get(0);
    let max_id: i32 = row.get(1);
    
    // Calculate chunk sizes
    let chunk_size = 50_000;
    let num_chunks = ((max_id - min_id) as f64 / chunk_size as f64).ceil() as i32;
    let chunks: Vec<(i32, i32)> = (0..num_chunks)
        .map(|i| {
            let start = min_id + (i * chunk_size);
            let end = std::cmp::min(start + chunk_size, max_id + 1);
            (start, end)
        })
        .collect();

    println!("Processing {} chunks...", chunks.len());

    // Collect all data in parallel
    let all_rows: Vec<Vec<String>> = chunks.par_iter()
        .flat_map(|(start_id, end_id)| {
            let mut conn = Client::connect(conn_str, NoTls).unwrap();
            conn.query(
                "SELECT id::text, col1::text, col2::text, col3::text, col4::text, 
                        col5::text, col6::text, col7::text, col8::text, col9::text, 
                        col10::text 
                 FROM test_table 
                 WHERE id >= $1 AND id < $2 
                 ORDER BY id",
                &[start_id, end_id],
            ).unwrap()
            .iter()
            .map(|row| (0..11)
                .map(|i| row.get::<_, Option<String>>(i).unwrap_or_default())
                .collect())
            .collect::<Vec<Vec<String>>>()
        })
        .collect();

    println!("Writing {} rows to Excel...", all_rows.len());

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