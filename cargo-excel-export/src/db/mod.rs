use tokio_postgres:: {Client, Error, NoTls};

pub async fn connect_db() -> Result<Client, Error> {
    let conn_str = "host=localhost user=postgres password=password dbname=postgres";
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

    Ok(client)
}