use rust_xlsxwriter::{Workbook, XlsxError};

pub struct ExcelWriter {
    workbook: Workbook,
}

impl ExcelWriter { 
    pub fn new(filename: &str) -> Result<Self, XlsxError> {
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        
        worksheet.write_string(0, 0, "Hello")?;
        worksheet.write_string(0, 1, "World")?;

        workbook.save(filename)?;

        Ok(ExcelWriter { workbook })
    }

    pub fn get_workbook(&self) -> &Workbook {
        &self.workbook
    }
}