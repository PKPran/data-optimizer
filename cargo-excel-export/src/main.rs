use postgres::{Client, NoTls};
use std::time::Instant;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::io::BufRead;
use rust_xlsxwriter::{Workbook, Format};
use rayon::prelude::*;
use std::sync::Mutex;
use num_cpus;

// Dynamic configuration based on system
struct Config {
    chunk_size: usize,
    buffer_size: usize,
    batch_size: usize,
    num_threads: usize,
}

impl Config {
    fn new() -> Self {
        let available_memory = sys_info::mem_info()
            .map(|mi| mi.total as usize * 1024)  // Convert to bytes
            .unwrap_or(8 * 1024 * 1024 * 1024);  // Default to 8GB if can't detect

        let cpu_cores = num_cpus::get();
        let memory_per_chunk = available_memory / (4 * cpu_cores); // Use 25% of memory divided by cores

        Self {
            chunk_size: (memory_per_chunk / 1024).min(250_000), // Cap at 250K rows
            buffer_size: (16 * 1024).min(memory_per_chunk / 1000), // Reasonable buffer size
            batch_size: 5000.min(memory_per_chunk / (100 * 1024)), // Excel batch size
            num_threads: cpu_cores,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::new();
    let start = Instant::now();
    let conn_str = "postgresql://user:password@localhost/test_db";
    
    const TOTAL_ROWS: usize = 1_000_000;
    println!("System Configuration:");
    println!("- CPU Cores: {}", config.num_threads);
    println!("- Chunk Size: {} rows", config.chunk_size);
    println!("- Buffer Size: {} bytes", config.buffer_size);
    println!("- Batch Size: {} rows", config.batch_size);
    println!("\nTotal rows in database: {}", TOTAL_ROWS);

    // Configure thread pool based on available CPU cores
    rayon::ThreadPoolBuilder::new()
        .num_threads(config.num_threads)
        .build_global()?;

    // Create workbook and wrap in Mutex for thread safety
    let workbook = Arc::new(Mutex::new(Workbook::new()));
    let header_format = Format::new().set_bold();
    let headers = ["id", "col1", "col2", "col3", "col4", "col5", 
                  "col6", "col7", "col8", "col9", "col10"];

    // Progress counter
    let row_counter = Arc::new(AtomicU32::new(0));
    let progress_interval = (TOTAL_ROWS / 20) as u32;

    // Calculate number of chunks based on total rows and chunk size
    let num_chunks = (TOTAL_ROWS + config.chunk_size - 1) / config.chunk_size;
    let chunks: Vec<_> = (0..num_chunks)
        .map(|i| {
            let start = i * config.chunk_size + 1;
            let end = if i == num_chunks - 1 { 
                TOTAL_ROWS + 1 
            } else { 
                (i + 1) * config.chunk_size + 1 
            };
            (start, end)
        })
        .collect();

    // Process chunks in parallel
    chunks.par_iter().enumerate().try_for_each(|(sheet_idx, &(start_id, end_id))| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut worksheet_data: Vec<(u32, u16, String)> = Vec::with_capacity(config.chunk_size * headers.len());
        
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
            config.buffer_size,
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

        // Write headers
        for (col, &header) in headers.iter().enumerate() {
            worksheet.write_string_with_format(0, col as u16, header, &header_format)?;
        }

        // Write data in batches
        for chunk in worksheet_data.chunks(config.batch_size) {
            for &(row, col, ref value) in chunk {
                worksheet.write_string(row, col, value)?;
            }
        }

        println!("Completed sheet {} ({} rows)", sheet_idx + 1, current_row - 1);
        Ok(())
    })?;

    println!("Saving Excel file...");
    let mut workbook_guard = workbook.lock().map_err(|_| "Failed to lock workbook")?;
    workbook_guard.save("output.xlsx")?;

    let duration = start.elapsed();
    println!("Export completed in {:.2} seconds", duration.as_secs_f64());

    Ok(())
}