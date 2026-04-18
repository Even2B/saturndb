use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};

pub async fn listen(ws_port: u16, saturn_port: u16, host: &str) -> anyhow::Result<()> {
    let ws_addr     = format!("{host}:{ws_port}");
    let saturn_addr = format!("127.0.0.1:{saturn_port}");
    let listener  = TcpListener::bind(&ws_addr).await?;
    println!("saturn-ws listening on {ws_addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let saturn_addr   = saturn_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = proxy(stream, &saturn_addr).await {
                eprintln!("ws proxy error: {e}");
            }
        });
    }
}

async fn proxy(stream: TcpStream, saturn_addr: &str) -> anyhow::Result<()> {
    let ws_stream   = accept_async(stream).await?;
    let saturn_stream = TcpStream::connect(saturn_addr).await?;
    saturn_stream.set_nodelay(true)?;

    let (saturn_reader, mut saturn_writer) = saturn_stream.into_split();
    let mut saturn_lines = BufReader::new(saturn_reader).lines();
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    loop {
        tokio::select! {
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        saturn_writer.write_all(text.as_bytes()).await?;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            line = saturn_lines.next_line() => {
                match line? {
                    None => break,
                    Some(line) => {
                        ws_tx.send(Message::Text(format!("{line}\n").into())).await?;
                    }
                }
            }
        }
    }

    Ok(())
}
