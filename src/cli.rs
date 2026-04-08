use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

const DEFAULT_HOST:  &str = "127.0.0.1";
const DEFAULT_PORT:  &str = "7379";
const DEFAULT_TOKEN: &str = "";

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(0);
    }

    let host  = std::env::var("SATURN_HOST").unwrap_or(DEFAULT_HOST.to_string());
    let port  = std::env::var("SATURN_PORT").unwrap_or(DEFAULT_PORT.to_string());
    let token = std::env::var("SATURN_TOKEN").unwrap_or(DEFAULT_TOKEN.to_string());
    let addr  = format!("{host}:{port}");

    let mut conn = match Connection::open(&addr, &token) {
        Ok(c)  => c,
        Err(e) => { eprintln!("error: {e}"); std::process::exit(1); }
    };

    let cmd = args[1].to_lowercase();
    let result = match cmd.as_str() {
        "ping"   => conn.send("PING"),
        "get"    => conn.send(&format!("GET {}", require_arg(&args, 2, "get <stream>"))),
        "del"    => conn.send(&format!("DEL {}", require_arg(&args, 2, "del <stream>"))),
        "keys"   => conn.send(&format!("KEYS {}", require_arg(&args, 2, "keys <pattern>"))),
        "emit"   => {
            let stream  = require_arg(&args, 2, "emit <stream> <json>");
            let payload = args[3..].join(" ");
            if payload.is_empty() {
                eprintln!("usage: saturn emit <stream> <json>");
                std::process::exit(1);
            }
            conn.send(&format!("EMIT {stream} {payload}"))
        }
        "expire" => {
            let stream = require_arg(&args, 2, "expire <stream> <secs>");
            let secs   = require_arg(&args, 3, "expire <stream> <secs>");
            conn.send(&format!("EXPIRE {stream} {secs}"))
        }
        "since"  => {
            let stream = require_arg(&args, 2, "since <stream> <ts>");
            let ts     = require_arg(&args, 3, "since <stream> <ts>");
            conn.send(&format!("SINCE {stream} {ts}"))
        }
        "watch"  => {
            let pattern = require_arg(&args, 2, "watch <pattern>");
            conn.send(&format!("WATCH {pattern}")).ok();
            println!("watching {pattern} — press ctrl+c to stop\n");
            loop {
                match conn.read_line() {
                    Ok(line) => println!("{line}"),
                    Err(_)   => break,
                }
            }
            return;
        }
        other => {
            eprintln!("unknown command: {other}");
            print_usage();
            std::process::exit(1);
        }
    };

    match result {
        Ok(lines) => {
            for line in lines {
                println!("{line}");
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

struct Connection {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Connection {
    fn open(addr: &str, token: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_nodelay(true)?;
        let reader = BufReader::new(stream.try_clone()?);
        let mut conn = Self { stream, reader };
        if !token.is_empty() {
            conn.send(&format!("AUTH {token}"))?;
        }
        Ok(conn)
    }

    fn send(&mut self, line: &str) -> anyhow::Result<Vec<String>> {
        writeln!(self.stream, "{line}")?;
        let first = self.read_line()?;

        // commands that return multiple lines
        if first.starts_with("KEY ") || first.starts_with("EVENT ") {
            let mut lines = vec![first];
            loop {
                match self.read_line() {
                    Ok(l) if l.starts_with("KEY ") || l.starts_with("EVENT ") => lines.push(l),
                    Ok(l)  => { lines.push(l); break; }
                    Err(_) => break,
                }
            }
            return Ok(lines);
        }

        Ok(vec![first])
    }

    fn read_line(&mut self) -> anyhow::Result<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        Ok(line.trim().to_string())
    }
}

fn require_arg(args: &[String], i: usize, usage: &str) -> String {
    if let Some(v) = args.get(i) {
        v.clone()
    } else {
        eprintln!("usage: saturn {usage}");
        std::process::exit(1);
    }
}

fn print_usage() {
    println!("saturn — CLI for SaturnDB database");
    println!();
    println!("usage:");
    println!("  saturn ping");
    println!("  saturn get <stream>");
    println!("  saturn emit <stream> <json>");
    println!("  saturn del <stream>");
    println!("  saturn keys <pattern>");
    println!("  saturn since <stream> <timestamp>");
    println!("  saturn expire <stream> <secs>");
    println!("  saturn watch <pattern>");
    println!();
    println!("env vars:");
    println!("  SATURN_HOST   server host  (default: 127.0.0.1)");
    println!("  SATURN_PORT   server port  (default: 7379)");
    println!("  SATURN_TOKEN  auth token   (default: none)");
}
