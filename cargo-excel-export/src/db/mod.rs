use tokio_postgres::{Client, Error, NoTls};
use std::env;

pub async fn connect_db() -> Result<Client, Error> {
    // Get database configuration from environment variables with fallbacks
    let host = env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let user = env::var("DB_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = env::var("DB_PASSWORD").unwrap_or_else(|_| "password".to_string());
    let dbname = env::var("DB_NAME").unwrap_or_else(|_| "postgres".to_string());
    
    let conn_str = format!(
        "host={} user={} password={} dbname={}",
        host, user, password, dbname
    );

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // Spawn the connection handler with more detailed error reporting
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
            eprintln!("Connection process terminated");
        }
    });

    // Test the connection
    match client.execute("SELECT 1", &[]).await {
        Ok(_) => println!("Database connection established successfully"),
        Err(e) => eprintln!("Failed to execute test query: {}", e),
    }

    Ok(client)
}