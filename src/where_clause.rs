use crate::sql_value::SQLValue;
use crate::BoolKind;

#[derive(Clone)]
pub struct WhereClauses {
    clauses: Vec<(String, SQLValue, BoolKind)>,
    multi_clauses: Vec<(String, Vec<SQLValue>)>,
}

impl WhereClauses {
    pub fn new() -> Self {
        Self {
            clauses: vec![],
            multi_clauses: vec![],
        }
    }

    pub fn push(&mut self, clause: impl Into<String>, value: impl Into<SQLValue>, kind: BoolKind) {
        self.clauses.push((clause.into(), value.into(), kind));
    }

    pub fn push_multi(&mut self, clause: impl Into<String>, value: Vec<SQLValue>) {
        self.multi_clauses.push((clause.into(), value));
    }

    pub fn parts(self) -> (String, Vec<SQLValue>) {
        if self.clauses.is_empty() && self.multi_clauses.is_empty() {
            return ("".to_string(), vec![]);
        }

        // Build up where clauses
        let mut out = " where ".to_string();

        for (i, (s, _, kind)) in self.clauses.iter().enumerate() {
            out.push_str(s.as_str());
            if i != self.clauses.len() - 1 {
                out.push_str(" ");
                out.push_str(kind.as_str());
                out.push_str(" ");
            }
        }

        println!("here");
        for (i, (s, _)) in self.multi_clauses.iter().enumerate() {
            println!("in multi clause");
            out.push_str(s.as_str());
            if i != self.multi_clauses.len() - 1 {
                out.push_str(" and ");
            }
        }

        (
            out,
            self.clauses
                .into_iter()
                .map(|(_, v, _)| v)
                .chain(self.multi_clauses.into_iter().flat_map(|(_, v)| v))
                .collect(),
        )
    }
}
