
use metric::AggregationMethod;
use quantiles::ckms::CKMS;
use std::ops::AddAssign;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Value {
    kind: AggregationMethod,
    single: Option<f64>,
    many: Option<CKMS<f64>>,
}

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

impl Value {
    pub fn new() -> Value {
        Value {
            kind: AggregationMethod::Set,
            single: None,
            many: None,
        }
    }

    pub fn kind(&self) -> AggregationMethod {
        self.kind
    }

    pub fn set_kind(&mut self, aggr: AggregationMethod) -> () {
        match (self.kind, aggr) {
            (AggregationMethod::Set, AggregationMethod::Set) => {}
            (AggregationMethod::Sum, AggregationMethod::Sum) => {}
            (AggregationMethod::Summarize, AggregationMethod::Summarize) => {}
            (AggregationMethod::Summarize, AggregationMethod::Sum) => {
                if let Some(ref ckms) = self.many {
                    self.single = ckms.sum();
                }
                self.many = None;
                self.kind = AggregationMethod::Sum;
            }
            (AggregationMethod::Summarize, AggregationMethod::Set) => {
                if let Some(ref ckms) = self.many {
                    self.single = ckms.last();
                }
                self.many = None;
                self.kind = AggregationMethod::Set;
            }
            (AggregationMethod::Sum, AggregationMethod::Set) => {
                self.kind = AggregationMethod::Set;
            }
            (AggregationMethod::Sum, AggregationMethod::Summarize) => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(self.single.unwrap());
                self.many = Some(ckms);
                self.kind = AggregationMethod::Summarize;
            }
            (AggregationMethod::Set, AggregationMethod::Summarize) => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(self.single.unwrap());
                self.many = Some(ckms);
                self.kind = AggregationMethod::Summarize;
            }
            (AggregationMethod::Set, AggregationMethod::Sum) => {
                self.kind = AggregationMethod::Sum;
            }
        }
    }

    pub fn set(&mut self, value: f64) -> () {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => {
                self.single = Some(value)
            }
            AggregationMethod::Summarize => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(value);
                self.many = Some(ckms);
            }
        }
    }

    pub fn value(&self) -> Option<f64> {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => self.single,
            AggregationMethod::Summarize => {
                match self.many {
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
            AggregationMethod::Summarize => self.many.unwrap().into_vec(),
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
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => ckms.insert(value),
                };
            }
        }
    }

    fn merge(&mut self, value: CKMS<f64>) -> () {
        match self.kind {
            AggregationMethod::Set => self.single = value.last(),
            AggregationMethod::Sum => {
                let single_none = self.single.is_none();
                let value_none = value.sum().is_none();

                if single_none && value_none {
                    self.single = None
                } else {
                    let sum = self.single.unwrap_or(0.0);
                    self.single = Some(value.sum().unwrap_or(0.0) + sum);
                }
            }
            AggregationMethod::Summarize => {
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => *ckms += value,
                };
            }
        }
    }

    pub fn sum(&self) -> Option<f64> {
        match self.kind {
            AggregationMethod::Set | AggregationMethod::Sum => self.single,
            AggregationMethod::Summarize => {
                match self.many {
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
                match self.many {
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
                match self.many {
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
                match self.many {
                    Some(ref ckms) => ckms.query(query),
                    None => None,
                }
            }
        }
    }
}
