use quantiles::ckms::CKMS;
use std::ops::AddAssign;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
enum ValueKind {
    Single,
    Many,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Value {
    kind: ValueKind,
    single: Option<f64>,
    many: Option<CKMS<f64>>,
}

impl AddAssign for Value {
    fn add_assign(&mut self, rhs: Value) {
        match rhs.kind {
            ValueKind::Single => {
                self.insert(rhs.single.expect("EMPTY SINGLE ADD_ASSIGN"))
            }
            ValueKind::Many => self.merge(rhs.many.expect("EMPTY MANY ADD_ASSIGN")),
        }
    }
}

impl Value {
    pub fn new(value: f64) -> Value {
        Value {
            kind: ValueKind::Single,
            single: Some(value),
            many: None,
        }
    }

    pub fn into_vec(self) -> Vec<f64> {
        match self.kind {
            ValueKind::Single => vec![self.single.unwrap()],
            ValueKind::Many => self.many.unwrap().into_vec(),
        }
    }

    pub fn insert(&mut self, value: f64) -> () {
        match self.kind {
            ValueKind::Single => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(self.single.expect("NOT SINGLE IN METRICVALUE INSERT"));
                ckms.insert(value);
                self.many = Some(ckms);
                self.single = None;
                self.kind = ValueKind::Many;
            }
            ValueKind::Many => {
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => ckms.insert(value),
                };
            }
        }
    }

    fn merge(&mut self, mut value: CKMS<f64>) -> () {
        match self.kind {
            ValueKind::Single => {
                value.insert(self.single.expect("NOT SINGLE IN METRICVALUE MERGE"));
                self.many = Some(value);
                self.single = None;
                self.kind = ValueKind::Many;
            }
            ValueKind::Many => {
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => *ckms += value,
                };
            }
        }
    }

    pub fn last(&self) -> Option<f64> {
        match self.kind {
            ValueKind::Single => self.single,
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.last(),
                    None => None,
                }
            }
        }
    }

    pub fn sum(&self) -> Option<f64> {
        match self.kind {
            ValueKind::Single => self.single,
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.sum(),
                    None => None,
                }
            }
        }
    }

    pub fn count(&self) -> usize {
        match self.kind {
            ValueKind::Single => 1,
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.count(),
                    None => 0,
                }
            }
        }
    }

    pub fn mean(&self) -> Option<f64> {
        match self.kind {
            ValueKind::Single => self.single,
            ValueKind::Many => {
                match self.many {
                    Some(ref x) => x.cma(),
                    None => None,
                }
            }
        }
    }

    pub fn query(&self, query: f64) -> Option<(usize, f64)> {
        match self.kind {
            ValueKind::Single => {
                Some((1, self.single.expect("NOT SINGLE IN METRICVALUE QUERY")))
            }
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.query(query),
                    None => None,
                }
            }
        }
    }
}
