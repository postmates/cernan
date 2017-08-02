use metric::AggregationMethod;
use quantiles::ckms::CKMS;
use quantiles::histogram::Histogram;
use std::ops::AddAssign;

// AddAssign for Value is an interesting thing. We have to obey the following
// rules:
//
//   * right hand side addition sets the aggregation
//   * data _can_ be lost in the conversion
//   * where possible we should minimize data loss
//
impl AddAssign for Value {
    fn add_assign(&mut self, rhs: Value) {
        match rhs.kind {
            AggregationMethod::Set => {
                self.set_kind(AggregationMethod::Set);
                self.insert(rhs.single.expect("EMPTY SINGLE ADD_ASSIGN"))
            }
            AggregationMethod::Sum => {
                self.set_kind(AggregationMethod::Sum);
                self.insert(rhs.single.expect("EMPTY SINGLE ADD_ASSIGN"))
            }
            AggregationMethod::Summarize => {
                self.set_kind(AggregationMethod::Summarize);
                self.merge(rhs.many.expect("EMPTY MANY ADD_ASSIGN"))
            }
        }
    }
}

impl SoftValue {
    pub fn set(mut self) -> SoftValue {
        self.kind = Some(AggregationMethod::Set);
        self
    }

    pub fn sum(mut self) -> SoftValue {
        self.kind = Some(AggregationMethod::Sum);
        self
    }

    pub fn quantile(mut self) -> SoftValue {
        self.kind = Some(AggregationMethod::Summarize);
        self
    }

    /// Set the error for the quantile
    pub fn error(mut self, error: f64) -> SoftValue {
        self.error = Some(error);
        self
    }

    pub fn histogram(mut self) -> SoftValue {
        self.kind = Some(AggregationMethod::Histogram);
        self
    }

    /// Set the bins for a histogram.
    pub fn bounds(mut self, bounds: Vec<f64>) -> SoftValue {
        bounds.sort_by(|a, b| a.partial_cmp(b).unwrap());
        self.bounds = bounds;
        self
    }

    pub fn harden(self) -> Result<Value, Error> {
        if let Some(kind) = self.kind {
            match kind {
                AggregationMethod::Set | AggregationMethod::Sum => Ok(Value::Set(0.0)),
                AggregationMethod::Summarize => {
                    if let Some(error) = self.error {
                        if error >= 1.0 {
                            return Err(Error::ErrorTooLarge);
                        }
                        Ok(Value::Quantiles(CKMS::new(error)))
                    } else {
                        Err(Error::NoError)
                    }
                }
                AggregationMethod::Histogram => {
                    if let Some(bounds) = self.bounds {
                        Ok(Value::Histogram(Histogram::new(bounds)))
                    } else {
                        Err(Error::NoBounds)
                    }
                }
            }
        } else {
            Err(Error::NoKind)
        }
    }
}

#[derive(Debug)]
pub enum Error {
    ErrorTooLarge,
    NoBounds,
    NoKind,
}


impl Value {
    pub fn new() -> SoftValue {
        SoftValue {
            kind: None,
            bins: None,
        }
    }

    pub fn kind(&self) -> AggregationMethod {
        self.kind
    }

    pub fn set(&mut self, value: f64) -> () {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => {
                self.single = Some(value)
            }
            AggregationMethod::Summarize => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(value);
                self.quantiles = Some(ckms);
            }
        }
    }

    pub fn value(&self) -> Option<f64> {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => self.single,
            AggregationMethod::Summarize => {
                match self.quantiles {
                    Some(ref ckms) => ckms.query(1.0).map(|x| x.1),
                    None => None,
                }
            }
        }
    }

    pub fn into_vec(self) -> Vec<f64> {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => {
                vec![self.single.unwrap()]
            }
            AggregationMethod::Summarize => self.quantiles.unwrap().into_vec(),
        }
    }

    pub fn insert(&mut self, value: f64) -> () {
        match self.kind {
            AggregationMethod::Set => self.single = Some(value),
            AggregationMethod::Sum => {
                let sum = self.single.unwrap_or(0.0);
                self.single = Some(sum + value);
            }
            AggregationMethod::Summarize => {
                match self.quantiles.as_mut() {
                    None => {}
                    Some(ckms) => ckms.insert(value),
                };
            }
        }
    }

    // fn merge(&mut self, value: CKMS<f64>) -> () {
    //     match self.kind {
    //         AggregationMethod::Set => self.single = value.last(),
    //         AggregationMethod::Sum => {
    //             let single_none = self.single.is_none();
    //             let value_none = value.sum().is_none();

    //             if single_none && value_none {
    //                 self.single = None
    //             } else {
    //                 let sum = self.single.unwrap_or(0.0);
    //                 self.single = Some(value.sum().unwrap_or(0.0) + sum);
    //             }
    //         }
    //         AggregationMethod::Summarize => {
    //             match self.quantiles.as_mut() {
    //                 None => {}
    //                 Some(ckms) => *ckms += value,
    //             };
    //         }
    //         AggregationMethod::Histogram => {
    //             match self.histogram.as_mut() {
    //                 None => {}
    //                 Some(histo) => *histo += value,
    //             };
    //         }
    //     }
    // }

    pub fn sum(&self) -> Option<f64> {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => self.single,
            AggregationMethod::Summarize => {
                match self.quantiles {
                    Some(ref ckms) => ckms.sum(),
                    None => None,
                }
            }
        }
    }

    pub fn count(&self) -> usize {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => 1,
            AggregationMethod::Summarize => {
                match self.quantiles {
                    Some(ref ckms) => ckms.count(),
                    None => 0,
                }
            }
        }
    }

    pub fn mean(&self) -> Option<f64> {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => self.single,
            AggregationMethod::Summarize => {
                match self.quantiles {
                    Some(ref x) => x.cma(),
                    None => None,
                }
            }
        }
    }

    pub fn query(&self, query: f64) -> Option<(usize, f64)> {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => {
                Some((1, self.single.expect("NOT SINGLE IN METRICVALUE QUERY")))
            }
            AggregationMethod::Summarize => {
                match self.quantiles {
                    Some(ref ckms) => ckms.query(query),
                    None => None,
                }
            }
        }
    }
}
