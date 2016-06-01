/// piggy-back off a community library. It uses u64 internally--as do
/// hrdhistogram wrappers--so, uh, we'll just covert as needed. Adapting the
/// dependent library to be generic shouldn't be hard, I don't think.

use histogram;

pub struct Histogram {
    inner_hist: histogram::Histogram,
    scaling_factor: f64,
}

impl Histogram {
    pub fn new() -> Histogram {
        let mut c = histogram::HistogramConfig::new();
        c.max_value(60_000_000_000).precision(4);
        let h = histogram::Histogram::configured(c).unwrap();
        Histogram {
            inner_hist: h,
            scaling_factor: 10000.0,
        }
    }

    pub fn increment(&mut self, value: f64) -> Result<(), &'static str> {
        let u64_val = (value * self.scaling_factor).round() as u64;
        self.inner_hist.increment(u64_val)
    }

    pub fn percentile(&self, percent: f64) ->  Result<f64, &'static str> {
        match self.inner_hist.percentile(percent) {
            Result::Ok(val) => Result::Ok( (((val as f64) / self.scaling_factor) * 100.0).round() / 100.0 ),
            Result::Err(why) => Result::Err(why)
        }
    }

    pub fn min(&self) ->  Result<f64, &'static str> {
        self.percentile(0.0)
    }

    pub fn max(&self) ->  Result<f64, &'static str> {
        self.percentile(100.0)
    }

    pub fn mean(&self) -> Result<f64, &'static str> {
        match self.inner_hist.mean() {
            Result::Ok(val) => Result::Ok( (((val as f64) / self.scaling_factor) * 100.0).round() / 100.0 ),
            Result::Err(why) => Result::Err(why)
        }
    }
}

/// Tests
///
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_percentiles() {
        let mut hist = Histogram::new();

        for i in 100..200 {
            hist.increment(i as f64).ok().expect("error");
        }

        assert_eq!(hist.percentile(0.0).unwrap(), 100.0);
        assert_eq!(hist.percentile(10.0).unwrap(), 109.0);
        assert_eq!(hist.percentile(25.0).unwrap(), 123.99);
        assert_eq!(hist.percentile(50.0).unwrap(), 150.0);
        assert_eq!(hist.percentile(75.0).unwrap(), 175.0);
        assert_eq!(hist.percentile(90.0).unwrap(), 189.99);
        assert_eq!(hist.percentile(95.0).unwrap(), 194.99);
        assert_eq!(hist.percentile(100.0).unwrap(), 199.0);

        assert_eq!(hist.min().unwrap(), 100.0);
        assert_eq!(hist.max().unwrap(), 199.0);
        assert_eq!(hist.mean().unwrap(), 149.49);
    }
}
