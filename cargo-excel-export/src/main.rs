mod excel;
mod db;

use excel::ExcelWriter;
use tokio_postgres::Client;
use chrono::Local;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database connection
    let client: Client = db::connect_db().await?;
    
    // Create Excel file with timestamp
    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let excel_file = format!("data_export_{}.xlsx", timestamp);
    let writer = ExcelWriter::new(&excel_file)?;
    let mut workbook = Workbook::new(&excel_file);
    let mut worksheet = workbook.add_worksheet(Some("Data"))?;

    // Get total count
    let row = client
        .query_one("SELECT COUNT(*) FROM your_table", &[])
        .await?;
    let total_rows: i64 = row.get(0);
    println!("Total rows to process: {}", total_rows);

    // Fetch data
    let rows = client
        .query("SELECT * FROM your_table", &[])
        .await?;

    // Write headers
    if let Some(first_row) = rows.first() {
        for (col_idx, column) in first_row.columns().iter().enumerate() {
            worksheet.write_string(0, col_idx as u16, &column.name().to_string())?;
        }
    }

    // Write data rows
    for (row_idx, row) in rows.iter().enumerate() {
        for col_idx in 0..row.columns().len() {
            let value = row.get::<_, String>(col_idx);
            worksheet.write_string(
                (row_idx + 1) as u32,
                col_idx as u16,
                &value,
            )?;
        }
    }

    println!("Data exported to: {}", excel_file);
    Ok(())
}