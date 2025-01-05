mod excel;
use excel::ExcelWriter;

fn main() -> Result<(), rust_xlsxwriter::XlsxError> {
    let writer = ExcelWriter::new("output.xlsx")?;
    let _workbook = writer.get_workbook();
    println!("Excel file created successfully!");
    Ok(())
}
