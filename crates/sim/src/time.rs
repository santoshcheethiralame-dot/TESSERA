use std::ops::Add;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Time(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Duration(pub u64);

impl Time {
    pub const ZERO: Time = Time(0);

    pub fn as_nanos(self) -> u64 {
        self.0
    }

    pub fn as_millis(self) -> u64 {
        self.0 / 1_000_000
    }

    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1e9
    }
}

impl Duration {
    pub fn as_nanos(self) -> u64 {
        self.0
    }

    pub fn as_millis(self) -> u64 {
        self.0 / 1_000_000
    }
}

pub const fn nanos(n: u64) -> Duration {
    Duration(n)
}

pub const fn micros(n: u64) -> Duration {
    Duration(n * 1_000)
}

pub const fn millis(n: u64) -> Duration {
    Duration(n * 1_000_000)
}

pub const fn secs(n: u64) -> Duration {
    Duration(n * 1_000_000_000)
}

impl Add<Duration> for Time {
    type Output = Time;

    fn add(self, rhs: Duration) -> Time {
        Time(self.0 + rhs.0)
    }
}

impl Add<Duration> for Duration {
    type Output = Duration;

    fn add(self, rhs: Duration) -> Duration {
        Duration(self.0 + rhs.0)
    }
}
