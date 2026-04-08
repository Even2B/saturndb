use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Terminal,
};

const MAX_EVENTS: usize = 100;
const TOKEN: &str = "saturn-admin-secret";
const ADDR:  &str = "127.0.0.1:7379";

#[derive(Clone)]
struct LiveEvent {
    time:    String,
    stream:  String,
    payload: String,
}

struct AppState {
    streams:    Vec<(String, String)>,
    events:     VecDeque<LiveEvent>,
    list_state: ListState,
    connected:  bool,
}

impl AppState {
    fn new() -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));
        Self { streams: Vec::new(), events: VecDeque::new(), list_state, connected: false }
    }

    fn push_event(&mut self, stream: String, payload: String) {
        let now = chrono::Local::now().format("%H:%M:%S").to_string();
        self.events.push_front(LiveEvent { time: now, stream, payload });
        if self.events.len() > MAX_EVENTS { self.events.pop_back(); }
    }

    fn scroll_up(&mut self) {
        let i = self.list_state.selected().unwrap_or(0);
        if i > 0 { self.list_state.select(Some(i - 1)); }
    }

    fn scroll_down(&mut self) {
        let len = self.streams.len();
        if len == 0 { return; }
        let i = self.list_state.selected().unwrap_or(0);
        if i < len - 1 { self.list_state.select(Some(i + 1)); }
    }
}

fn open_conn(timeout_ms: u64) -> Option<(TcpStream, BufReader<TcpStream>)> {
    let stream = TcpStream::connect(ADDR).ok()?;
    stream.set_nodelay(true).ok()?;
    stream.set_read_timeout(Some(Duration::from_millis(timeout_ms))).ok()?;
    let reader = BufReader::new(stream.try_clone().ok()?);
    Some((stream, reader))
}

fn auth(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>) -> bool {
    let _ = writeln!(stream, "AUTH {TOKEN}");
    matches!(read_line(reader), Some(l) if l == "OK")
}

fn read_line(reader: &mut BufReader<TcpStream>) -> Option<String> {
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) | Err(_) => None,
        Ok(_) => Some(line.trim().to_string()),
    }
}

fn refresh_streams(state: &Arc<Mutex<AppState>>) {
    let Some((mut stream, mut reader)) = open_conn(2000) else { return };
    if !auth(&mut stream, &mut reader) { return }

    let _ = writeln!(stream, "KEYS *");
    let mut keys = Vec::new();
    loop {
        match read_line(&mut reader) {
            Some(l) if l.starts_with("KEY ") => keys.push(l[4..].to_string()),
            _ => break,
        }
    }

    let mut streams = Vec::new();
    for key in &keys {
        let _ = writeln!(stream, "GET {key}");
        let val = read_line(&mut reader)
            .map(|l| if l.starts_with("VALUE ") { l[6..].to_string() } else { l })
            .unwrap_or_else(|| "—".to_string());
        streams.push((key.clone(), val));
    }

    let mut app = state.lock().unwrap();
    app.streams   = streams;
    app.connected = true;
}

fn main() -> anyhow::Result<()> {
    let state = Arc::new(Mutex::new(AppState::new()));

    // thread 1: watch * for live events (persistent connection, no timeout)
    let watch_state = Arc::clone(&state);
    thread::spawn(move || {
        loop {
            let Some((mut stream, mut reader)) = open_conn(0) else {
                watch_state.lock().unwrap().connected = false;
                thread::sleep(Duration::from_secs(1));
                continue;
            };
            // 0 = blocking, no timeout on watch connection
            stream.set_read_timeout(None).unwrap();

            if !auth(&mut stream, &mut reader) {
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            let _ = writeln!(stream, "WATCH *");
            let _ = read_line(&mut reader);

            watch_state.lock().unwrap().connected = true;

            loop {
                match read_line(&mut reader) {
                    Some(l) if l.starts_with("EVENT ") => {
                        let rest     = &l[6..];
                        let space_at = rest.find(' ').unwrap_or(rest.len());
                        let s        = rest[..space_at].to_string();
                        let p        = rest[space_at..].trim().to_string();
                        watch_state.lock().unwrap().push_event(s, p);
                    }
                    None => {
                        watch_state.lock().unwrap().connected = false;
                        break;
                    }
                    _ => {}
                }
            }

            thread::sleep(Duration::from_millis(500));
        }
    });

    // thread 2: poll keys+values every 2 seconds
    let poll_state = Arc::clone(&state);
    thread::spawn(move || {
        loop {
            refresh_streams(&poll_state);
            thread::sleep(Duration::from_secs(2));
        }
    });

    // TUI
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend  = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;

    loop {
        {
            let mut app = state.lock().unwrap();
            term.draw(|f| render(f, &mut app))?;
        }

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Up   => state.lock().unwrap().scroll_up(),
                    KeyCode::Down => state.lock().unwrap().scroll_down(),
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(term.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

fn render(f: &mut ratatui::Frame, app: &mut AppState) {
    let area   = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let status_color = if app.connected { Color::Green } else { Color::Red };
    let status_text  = if app.connected { "● connected" } else { "○ disconnected" };

    let header = Paragraph::new(Line::from(vec![
        Span::styled(status_text, Style::default().fg(status_color).add_modifier(Modifier::BOLD)),
        Span::raw(format!("   streams: {}   events buffered: {}   q to quit", app.streams.len(), app.events.len())),
    ]))
    .block(Block::default().borders(Borders::ALL).title(" SaturnDB Monitor "));
    f.render_widget(header, chunks[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    let stream_items: Vec<ListItem> = app.streams.iter().map(|(k, v)| {
        let val = if v.len() > 30 { format!("{}…", &v[..30]) } else { v.clone() };
        ListItem::new(Line::from(vec![
            Span::styled(format!("{k:<28}"), Style::default().fg(Color::Cyan)),
            Span::styled(val, Style::default().fg(Color::DarkGray)),
        ]))
    }).collect();

    let streams_list = List::new(stream_items)
        .block(Block::default().borders(Borders::ALL).title(" Streams  ↑↓ navigate "))
        .highlight_style(Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD))
        .highlight_symbol("❯ ");
    f.render_stateful_widget(streams_list, body[0], &mut app.list_state);

    let event_items: Vec<ListItem> = app.events.iter().map(|e| {
        let payload = if e.payload.len() > 25 { format!("{}…", &e.payload[..25]) } else { e.payload.clone() };
        ListItem::new(Line::from(vec![
            Span::styled(format!("{}  ", e.time), Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{:<24}", e.stream), Style::default().fg(Color::Yellow)),
            Span::styled(payload, Style::default().fg(Color::White)),
        ]))
    }).collect();

    let events_list = List::new(event_items)
        .block(Block::default().borders(Borders::ALL).title(" Live Events "));
    f.render_widget(events_list, body[1]);
}
