//! Opt-in, node-local aggregates. No URL, job payload, or transport error is stored.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use serde::Serialize;

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    HttpAdmission,
    HttpTransfer,
    CpuAdmission,
    Decode,
    Parse,
    Publication,
}

#[derive(Clone, Copy)]
pub(crate) enum Activity {
    Http,
    Cpu,
    Owned,
}

#[derive(Default, Serialize)]
struct Timing {
    count: u64,
    total_ns: u128,
    max_ns: u128,
}

#[derive(Default, Serialize)]
struct Gauge {
    active: usize,
    peak: usize,
    active_ns: u128,
    #[serde(skip)]
    updated: Option<Instant>,
}

impl Gauge {
    fn integrate(&mut self, now: Instant) {
        if let Some(previous) = self.updated {
            self.active_ns += now.duration_since(previous).as_nanos() * self.active as u128;
        }
        self.updated = Some(now);
    }
}

#[derive(Serialize)]
struct Totals {
    #[serde(skip)]
    started: Instant,
    elapsed_ns: u128,
    http_admission: Timing,
    http_transfer: Timing,
    cpu_admission: Timing,
    decode: Timing,
    parse: Timing,
    publication: Timing,
    http: Gauge,
    cpu: Gauge,
    owned: Gauge,
}

impl Totals {
    fn timing(&mut self, stage: Stage) -> &mut Timing {
        match stage {
            Stage::HttpAdmission => &mut self.http_admission,
            Stage::HttpTransfer => &mut self.http_transfer,
            Stage::CpuAdmission => &mut self.cpu_admission,
            Stage::Decode => &mut self.decode,
            Stage::Parse => &mut self.parse,
            Stage::Publication => &mut self.publication,
        }
    }

    fn gauge(&mut self, activity: Activity) -> &mut Gauge {
        match activity {
            Activity::Http => &mut self.http,
            Activity::Cpu => &mut self.cpu,
            Activity::Owned => &mut self.owned,
        }
    }
}

/// Disabled by default: no clock reads, allocation, or locking on the hot path.
/// Clones share one aggregate; report only after the node's graceful drain.
#[derive(Clone, Default)]
pub struct Diagnostics(Option<Arc<Mutex<Totals>>>);

impl Diagnostics {
    pub fn enabled() -> Self {
        Self(Some(Arc::new(Mutex::new(Totals {
            started: Instant::now(),
            elapsed_ns: 0,
            http_admission: Timing::default(),
            http_transfer: Timing::default(),
            cpu_admission: Timing::default(),
            decode: Timing::default(),
            parse: Timing::default(),
            publication: Timing::default(),
            http: Gauge::default(),
            cpu: Gauge::default(),
            owned: Gauge::default(),
        }))))
    }

    pub(crate) fn timer(&self, stage: Stage) -> Timer {
        Timer {
            diagnostics: self.clone(),
            stage,
            started: self.0.as_ref().map(|_| Instant::now()),
        }
    }

    pub(crate) fn enter(&self, activity: Activity) -> Active {
        if let Some(totals) = &self.0 {
            let mut totals = totals.lock().unwrap_or_else(|error| error.into_inner());
            let gauge = totals.gauge(activity);
            gauge.integrate(Instant::now());
            gauge.active += 1;
            gauge.peak = gauge.peak.max(gauge.active);
        }
        Active {
            diagnostics: self.clone(),
            activity,
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::to_value(&*self.0.as_ref().unwrap().lock().unwrap()).unwrap()
    }

    /// One fixed-schema JSON line on stderr, separate from progress and WebStats.
    /// Utilization = http.active_ns / (10 * elapsed_ns), including startup/idle/drain.
    pub fn report(&self) {
        if let Some(totals) = &self.0 {
            let mut totals = totals.lock().unwrap_or_else(|error| error.into_inner());
            let now = Instant::now();
            totals.elapsed_ns = now.duration_since(totals.started).as_nanos();
            for activity in [Activity::Http, Activity::Cpu, Activity::Owned] {
                totals.gauge(activity).integrate(now);
            }
            eprintln!(
                "swarmcrawl_metrics {}",
                serde_json::to_string(&*totals).expect("numeric metrics")
            );
        }
    }
}

pub(crate) struct Timer {
    diagnostics: Diagnostics,
    stage: Stage,
    started: Option<Instant>,
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let (Some(totals), Some(started)) = (&self.diagnostics.0, self.started) {
            let ns = started.elapsed().as_nanos();
            let mut totals = totals.lock().unwrap_or_else(|error| error.into_inner());
            let timing = totals.timing(self.stage);
            timing.count += 1;
            timing.total_ns += ns;
            timing.max_ns = timing.max_ns.max(ns);
        }
    }
}

pub(crate) struct Active {
    diagnostics: Diagnostics,
    activity: Activity,
}

impl Drop for Active {
    fn drop(&mut self) {
        if let Some(totals) = &self.diagnostics.0 {
            let mut totals = totals.lock().unwrap_or_else(|error| error.into_inner());
            let gauge = totals.gauge(self.activity);
            gauge.integrate(Instant::now());
            gauge.active -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_has_no_clock_and_enabled_aggregates_guards() {
        assert!(Diagnostics::default().timer(Stage::Parse).started.is_none());
        let diagnostics = Diagnostics::enabled();
        let first = diagnostics.enter(Activity::Http);
        let second = diagnostics.enter(Activity::Http);
        drop(first);
        drop(second);
        drop(diagnostics.timer(Stage::Parse));
        let totals = diagnostics.0.as_ref().unwrap().lock().unwrap();
        assert_eq!((totals.http.active, totals.http.peak), (0, 2));
        assert_eq!(totals.parse.count, 1);
        let json = serde_json::to_value(&*totals).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 10);
    }
}
