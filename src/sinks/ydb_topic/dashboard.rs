//! Live view of the sink state: the batch being accumulated, requests in
//! flight, unacknowledged messages and throughput.
//!
//! Drawn as an animated panel when stderr is a terminal, and logged as a
//! periodic summary otherwise.

use std::{
    collections::VecDeque,
    fmt::Write as _,
    io::{IsTerminal, Write as _},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::sinks::prelude::*;

/// Live dashboard of the sink state.
#[configurable_component]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct YdbTopicDashboardConfig {
    /// Shows the batch being accumulated, requests in flight, unacknowledged messages and
    /// throughput.
    ///
    /// When stderr is a terminal, an animated panel is drawn there; run Vector with `--quiet`
    /// to keep log lines from interleaving with it. Otherwise, a summary is logged every
    /// 10 seconds.
    #[serde(default)]
    pub enabled: bool,
}

impl From<bool> for YdbTopicDashboardConfig {
    fn from(enabled: bool) -> Self {
        Self { enabled }
    }
}

/// How often the terminal panel is redrawn.
const REFRESH: Duration = Duration::from_millis(200);
/// How often the summary is logged when stderr is not a terminal.
const LOG_INTERVAL: Duration = Duration::from_secs(10);
/// Number of throughput samples in the sparkline.
const HISTORY: usize = 40;

/// Counters shared by the sink, the service and the writer task.
pub(super) struct Stats {
    epoch: Instant,

    /// Events and in-memory bytes of the batch being accumulated.
    batch_events: AtomicU64,
    batch_bytes: AtomicU64,
    /// When the first event of the current batch arrived, in milliseconds
    /// since `epoch` plus one; zero when the batch is empty.
    batch_started: AtomicU64,

    events_in: AtomicU64,
    flushed_by_events: AtomicU64,
    flushed_by_bytes: AtomicU64,
    flushed_by_timeout: AtomicU64,
    last_batch_events: AtomicU64,
    last_batch_bytes: AtomicU64,

    requests_in_flight: AtomicI64,
    requests_failed: AtomicU64,
    /// Messages handed to the write session and not acknowledged yet.
    unacked: AtomicI64,
    acked: AtomicU64,
    bytes_acked: AtomicU64,

    sessions: AtomicU64,
    connected: AtomicBool,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            batch_events: AtomicU64::new(0),
            batch_bytes: AtomicU64::new(0),
            batch_started: AtomicU64::new(0),
            events_in: AtomicU64::new(0),
            flushed_by_events: AtomicU64::new(0),
            flushed_by_bytes: AtomicU64::new(0),
            flushed_by_timeout: AtomicU64::new(0),
            last_batch_events: AtomicU64::new(0),
            last_batch_bytes: AtomicU64::new(0),
            requests_in_flight: AtomicI64::new(0),
            requests_failed: AtomicU64::new(0),
            unacked: AtomicI64::new(0),
            acked: AtomicU64::new(0),
            bytes_acked: AtomicU64::new(0),
            sessions: AtomicU64::new(0),
            connected: AtomicBool::new(false),
        }
    }
}

impl Stats {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// An event entered the batcher.
    pub(super) fn event_in(&self, size: usize) {
        self.events_in.fetch_add(1, Ordering::Relaxed);
        if self.batch_events.fetch_add(1, Ordering::Relaxed) == 0 {
            self.batch_started
                .store(self.now_ms() + 1, Ordering::Relaxed);
        }
        self.batch_bytes.fetch_add(size as u64, Ordering::Relaxed);
    }

    /// The batcher emitted a batch.
    pub(super) fn batch_out(&self, events: usize, bytes: usize, settings: &BatcherSettings) {
        let (events, bytes) = (events as u64, bytes as u64);
        let left = self.batch_events.fetch_sub(events, Ordering::Relaxed) - events;
        self.batch_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.batch_started.store(
            if left == 0 { 0 } else { self.now_ms() + 1 },
            Ordering::Relaxed,
        );
        self.last_batch_events.store(events, Ordering::Relaxed);
        self.last_batch_bytes.store(bytes, Ordering::Relaxed);
        let reason = if events >= settings.item_limit as u64 {
            &self.flushed_by_events
        } else if bytes + bytes / events.max(1) > settings.size_limit as u64 {
            // The next event would not have fit.
            &self.flushed_by_bytes
        } else {
            &self.flushed_by_timeout
        };
        reason.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn request_started(&self) {
        self.requests_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn request_finished(&self, messages: usize, bytes: usize, ok: bool) {
        self.requests_in_flight.fetch_sub(1, Ordering::Relaxed);
        if ok {
            self.acked.fetch_add(messages as u64, Ordering::Relaxed);
            self.bytes_acked.fetch_add(bytes as u64, Ordering::Relaxed);
        } else {
            self.requests_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn unacked_add(&self, messages: i64) {
        self.unacked.fetch_add(messages, Ordering::Relaxed);
    }

    pub(super) fn session_opened(&self) {
        self.sessions.fetch_add(1, Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);
    }

    pub(super) fn session_closed(&self) {
        self.connected.store(false, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Snapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let started = load(&self.batch_started);
        Snapshot {
            batch_events: load(&self.batch_events),
            batch_bytes: load(&self.batch_bytes),
            batch_age: (started > 0)
                .then(|| Duration::from_millis(self.now_ms().saturating_sub(started - 1))),
            events_in: load(&self.events_in),
            flushed_by_events: load(&self.flushed_by_events),
            flushed_by_bytes: load(&self.flushed_by_bytes),
            flushed_by_timeout: load(&self.flushed_by_timeout),
            last_batch_events: load(&self.last_batch_events),
            last_batch_bytes: load(&self.last_batch_bytes),
            requests_in_flight: self.requests_in_flight.load(Ordering::Relaxed).max(0),
            requests_failed: load(&self.requests_failed),
            unacked: self.unacked.load(Ordering::Relaxed).max(0),
            acked: load(&self.acked),
            bytes_acked: load(&self.bytes_acked),
            sessions: load(&self.sessions),
            connected: self.connected.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Snapshot {
    batch_events: u64,
    batch_bytes: u64,
    batch_age: Option<Duration>,
    events_in: u64,
    flushed_by_events: u64,
    flushed_by_bytes: u64,
    flushed_by_timeout: u64,
    last_batch_events: u64,
    last_batch_bytes: u64,
    requests_in_flight: i64,
    requests_failed: u64,
    unacked: i64,
    acked: u64,
    bytes_acked: u64,
    sessions: u64,
    connected: bool,
}

/// Draws or logs the dashboard until dropped.
pub(super) struct Dashboard {
    task: tokio::task::JoinHandle<()>,
}

impl Dashboard {
    pub(super) fn start(stats: Arc<Stats>, settings: BatcherSettings, title: String) -> Self {
        let terminal = std::io::stderr().is_terminal();
        let task = tokio::spawn(async move {
            let mut view = View::new(settings, title, terminal);
            let interval = if terminal { REFRESH } else { LOG_INTERVAL };
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                view.update(stats.snapshot());
            }
        });
        Self { task }
    }
}

impl Drop for Dashboard {
    fn drop(&mut self) {
        self.task.abort();
        if std::io::stderr().is_terminal() {
            // Show the cursor again.
            let mut stderr = std::io::stderr().lock();
            _ = write!(stderr, "\x1b[?25h");
            _ = stderr.flush();
        }
    }
}

struct View {
    settings: BatcherSettings,
    title: String,
    terminal: bool,
    previous: Snapshot,
    previous_at: Instant,
    history: VecDeque<f64>,
    max_in_flight: i64,
    lines_drawn: usize,
    frame: usize,
}

impl View {
    fn new(settings: BatcherSettings, title: String, terminal: bool) -> Self {
        Self {
            settings,
            title,
            terminal,
            previous: Snapshot::default(),
            previous_at: Instant::now(),
            history: VecDeque::with_capacity(HISTORY),
            max_in_flight: 1,
            lines_drawn: 0,
            frame: 0,
        }
    }

    fn update(&mut self, now: Snapshot) {
        let elapsed = self.previous_at.elapsed().as_secs_f64().max(1e-3);
        let events_rate = (now.events_in - self.previous.events_in) as f64 / elapsed;
        let acked_rate = (now.acked - self.previous.acked) as f64 / elapsed;
        let bytes_rate = (now.bytes_acked - self.previous.bytes_acked) as f64 / elapsed;
        self.previous = now;
        self.previous_at = Instant::now();
        if self.history.len() == HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(acked_rate);
        self.max_in_flight = self.max_in_flight.max(now.requests_in_flight);
        self.frame += 1;

        if self.terminal {
            self.draw(&now, events_rate, acked_rate, bytes_rate);
        } else {
            info!(
                message = "YDB topic sink stats.",
                topic = %self.title,
                connected = now.connected,
                sessions = now.sessions,
                batch_events = now.batch_events,
                batch_bytes = now.batch_bytes,
                in_flight = now.requests_in_flight,
                unacked = now.unacked,
                events_per_sec = events_rate.round(),
                acked_per_sec = acked_rate.round(),
                bytes_per_sec = bytes_rate.round(),
                flushed_by_events = now.flushed_by_events,
                flushed_by_bytes = now.flushed_by_bytes,
                flushed_by_timeout = now.flushed_by_timeout,
                failed_requests = now.requests_failed,
            );
        }
    }

    fn draw(&mut self, now: &Snapshot, events_rate: f64, acked_rate: f64, bytes_rate: f64) {
        const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        const WIDTH: usize = 30;
        let settings = &self.settings;
        let mut out = String::new();

        let session = if now.connected {
            format!("{GREEN}● connected{RESET}")
        } else if now.sessions == 0 {
            format!("{YELLOW}○ not connected yet{RESET}")
        } else {
            format!("{RED}● reconnecting{RESET}")
        };
        _ = writeln!(
            out,
            "{BOLD}{} ydb_topic{RESET} → {}   session #{} {session}",
            SPINNER[self.frame % SPINNER.len()],
            self.title,
            now.sessions
        );

        // Whichever limit is closest to being reached flushes the batch.
        let age = now.batch_age.unwrap_or_default();
        let limits = [
            (
                "events ",
                ratio(now.batch_events as f64, settings.item_limit as f64),
                format!(
                    "{} / {}",
                    count(now.batch_events as f64),
                    count(settings.item_limit as f64)
                ),
            ),
            (
                "bytes  ",
                ratio(now.batch_bytes as f64, settings.size_limit as f64),
                format!(
                    "{} / {}",
                    size(now.batch_bytes as f64),
                    size(settings.size_limit as f64)
                ),
            ),
            (
                "timeout",
                ratio(age.as_secs_f64(), settings.timeout.as_secs_f64()),
                format!(
                    "{:.1}s / {:.1}s",
                    age.as_secs_f64(),
                    settings.timeout.as_secs_f64()
                ),
            ),
        ];
        let closest = limits
            .iter()
            .map(|(_, ratio, _)| *ratio)
            .fold(0.0, f64::max);
        _ = writeln!(
            out,
            "{DIM}current batch (flushes at whichever limit is hit first){RESET}"
        );
        for (name, ratio, label) in &limits {
            let color = if *ratio > 0.0 && *ratio >= closest {
                CYAN
            } else {
                DIM
            };
            _ = writeln!(out, "  {name} {color}{}{RESET} {label}", bar(*ratio, WIDTH));
        }

        let total_flushes = now.flushed_by_events + now.flushed_by_bytes + now.flushed_by_timeout;
        _ = writeln!(
            out,
            "{DIM}flushed{RESET}   by events {}  by bytes {}  by timeout {}   last batch {} ev, {}",
            percent(now.flushed_by_events, total_flushes),
            percent(now.flushed_by_bytes, total_flushes),
            percent(now.flushed_by_timeout, total_flushes),
            count(now.last_batch_events as f64),
            size(now.last_batch_bytes as f64),
        );

        let slots = usize::try_from(self.max_in_flight.clamp(1, 16)).unwrap_or(1);
        let busy = usize::try_from(now.requests_in_flight)
            .unwrap_or(0)
            .min(slots);
        _ = writeln!(
            out,
            "{DIM}in flight{RESET} {GREEN}{}{DIM}{}{RESET} {} requests   unacked {} messages   failed {}",
            "▮".repeat(busy),
            "▯".repeat(slots - busy),
            now.requests_in_flight,
            count(now.unacked as f64),
            now.requests_failed,
        );
        _ = writeln!(
            out,
            "{DIM}rate{RESET}      in {} ev/s   acked {} ev/s   {}/s   total acked {}",
            count(events_rate),
            count(acked_rate),
            size(bytes_rate),
            count(now.acked as f64),
        );
        _ = writeln!(
            out,
            "{DIM}acked/s{RESET}   {CYAN}{}{RESET}",
            sparkline(&self.history)
        );

        let mut stderr = std::io::stderr().lock();
        if self.lines_drawn > 0 {
            // Move back to the top of the previous frame.
            _ = write!(stderr, "\x1b[{}F", self.lines_drawn);
        } else {
            // Hide the cursor while drawing.
            _ = write!(stderr, "\x1b[?25l");
        }
        for line in out.lines() {
            _ = writeln!(stderr, "{line}\x1b[K");
        }
        _ = stderr.flush();
        self.lines_drawn = out.lines().count();
    }
}

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const CYAN: &str = "\x1b[36m";

fn ratio(value: f64, limit: f64) -> f64 {
    if limit > 0.0 {
        (value / limit).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn bar(ratio: f64, width: usize) -> String {
    const PARTS: [char; 8] = ['▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'];
    let eighths = (ratio * (width * 8) as f64).round() as usize;
    let mut bar = "█".repeat(eighths / 8);
    if !eighths.is_multiple_of(8) {
        bar.push(PARTS[eighths % 8 - 1]);
    }
    let filled = bar.chars().count();
    bar.push_str(&"·".repeat(width.saturating_sub(filled)));
    format!("[{bar}]")
}

fn sparkline(history: &VecDeque<f64>) -> String {
    const LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let max = history.iter().copied().fold(0.0, f64::max);
    history
        .iter()
        .map(|value| {
            if max <= 0.0 {
                LEVELS[0]
            } else {
                LEVELS[((value / max) * 7.0).round() as usize]
            }
        })
        .collect()
}

fn percent(part: u64, total: u64) -> String {
    if total == 0 {
        "-".into()
    } else {
        format!("{:.0}%", part as f64 * 100.0 / total as f64)
    }
}

fn count(value: f64) -> String {
    if value >= 1e6 {
        format!("{:.1}M", value / 1e6)
    } else if value >= 1e4 {
        format!("{:.1}k", value / 1e3)
    } else {
        format!("{value:.0}")
    }
}

fn size(bytes: f64) -> String {
    if bytes >= 1024.0 * 1024.0 {
        format!("{:.1} MiB", bytes / 1024.0 / 1024.0)
    } else if bytes >= 1024.0 {
        format!("{:.0} KiB", bytes / 1024.0)
    } else {
        format!("{bytes:.0} B")
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    fn settings() -> BatcherSettings {
        BatcherSettings::new(
            Duration::from_secs(1),
            NonZeroUsize::new(1000).unwrap(),
            NonZeroUsize::new(10).unwrap(),
        )
    }

    #[test]
    fn tracks_the_batch_being_accumulated() {
        let stats = Stats::default();
        for _ in 0..10 {
            stats.event_in(100);
        }
        let snapshot = stats.snapshot();
        assert_eq!((snapshot.batch_events, snapshot.batch_bytes), (10, 1000));
        assert!(snapshot.batch_age.is_some());

        stats.batch_out(10, 1000, &settings());
        let snapshot = stats.snapshot();
        assert_eq!((snapshot.batch_events, snapshot.batch_bytes), (0, 0));
        assert!(snapshot.batch_age.is_none());
        assert_eq!(snapshot.flushed_by_events, 1);
    }

    #[test]
    fn classifies_flush_reasons() {
        let stats = Stats::default();
        stats.event_in(950);
        stats.batch_out(1, 950, &settings());
        stats.event_in(10);
        stats.batch_out(1, 10, &settings());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.flushed_by_bytes, 1);
        assert_eq!(snapshot.flushed_by_timeout, 1);
    }

    #[test]
    fn draws_bars() {
        assert_eq!(bar(0.0, 4), "[····]");
        assert_eq!(bar(0.5, 4), "[██··]");
        assert_eq!(bar(1.0, 4), "[████]");
        assert_eq!(sparkline(&VecDeque::from([0.0, 50.0, 100.0])), "▁▅█");
    }
}
