use juzfs::client::Client;

fn usage() -> ! {
    eprintln!("juzfs [options] <command> [args]");
    eprintln!();
    eprintln!("options:");
    eprintln!("  --master <addr>        master address (default: 127.0.0.1:5000)");
    eprintln!("  --chunk-size <bytes>    chunk size in bytes (default: 64MB)");
    eprintln!();
    eprintln!("commands:");
    eprintln!("  create <path>           create file and allocate first chunk");
    eprintln!("  write  <path> <data>    write data at offset 0");
    eprintln!("  read   <path> [off] [len]");
    eprintln!("  append <path> <data>    record append");
    eprintln!("  stream <path>           streaming read");
    std::process::exit(1);
}

fn parse_args() -> (String, u64, Vec<String>) {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut master = "127.0.0.1:5000".to_string();
    let mut chunk_size = juzfs::CHUNK_SIZE;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--master" => {
                master = args.get(i + 1).cloned().unwrap_or_else(|| usage());
                i += 2;
            }
            "--chunk-size" => {
                chunk_size = args
                    .get(i + 1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| usage());
                i += 2;
            }
            _ => break,
        }
    }

    let rest = args[i..].to_vec();
    (master, chunk_size, rest)
}

#[tokio::main]
async fn main() {
    let (master, chunk_size, args) = parse_args();
    if args.is_empty() {
        usage();
    }

    let client = Client::new(master, chunk_size);

    let result = match args[0].as_str() {
        "create" => cmd_create(&client, &args[1..]).await,
        "write" => cmd_write(&client, &args[1..]).await,
        "read" => cmd_read(&client, &args[1..], chunk_size).await,
        "append" => cmd_append(&client, &args[1..]).await,
        "stream" => cmd_stream(&client, &args[1..]).await,
        _ => usage(),
    };

    if let Err(e) = result {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

async fn cmd_create(client: &Client, args: &[String]) -> std::io::Result<()> {
    let path = args.first().unwrap_or_else(|| usage());
    client.create_file(path).await?;
    client.allocate_chunk(path).await?;
    println!("{}", path);
    Ok(())
}

async fn cmd_write(client: &Client, args: &[String]) -> std::io::Result<()> {
    let path = args.first().unwrap_or_else(|| usage());
    let data = args.get(1).unwrap_or_else(|| usage());
    client.write(path, 0, data.as_bytes()).await?;
    println!("{} bytes", data.len());
    Ok(())
}

async fn cmd_read(client: &Client, args: &[String], chunk_size: u64) -> std::io::Result<()> {
    let path = args.first().unwrap_or_else(|| usage());
    let offset: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let length: u64 = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(chunk_size);
    let data = client.read(path, offset, length).await?;
    print!("{}", String::from_utf8_lossy(&data));
    Ok(())
}

async fn cmd_append(client: &Client, args: &[String]) -> std::io::Result<()> {
    let path = args.first().unwrap_or_else(|| usage());
    let data = args.get(1).unwrap_or_else(|| usage());
    let offset = client.append(path, data.as_bytes()).await?;
    println!("offset {}", offset);
    Ok(())
}

async fn cmd_stream(client: &Client, args: &[String]) -> std::io::Result<()> {
    let path = args.first().unwrap_or_else(|| usage());
    let mut rx = client.read_stream(path).await?;
    while let Some(chunk) = rx.recv().await {
        let bytes = chunk?;
        print!("{}", String::from_utf8_lossy(&bytes));
    }
    Ok(())
}
