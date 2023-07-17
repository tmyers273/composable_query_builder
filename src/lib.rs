//! # Overview
//!
//! composable_query_builder is a library that provides composable query builders for SQLx and
//! makes building dynamic queries easier.
//!
//! SQLx's built-in query builder is not composable and does not allow for easy
//! dynamic query building.
//!
//! This is currently only tested on Postgres.
//!
//! # Examples
//! ```rust
//! use composable_query_builder::ComposableQueryBuilder;
//! let query = ComposableQueryBuilder::new()
//!     .table("users")
//!     .where_clause("id = ?", 1)
//!     .where_clause("status_id = ?", 2)
//!     .into_builder();
//!
//! let sql = query.sql();
//! assert_eq!("select * from users where id = $1 and status_id = $2", sql);
//! ```
//!
//! Or with a bit more dynamicism:
//! ```rust
//! let status_id = Some(2);
//! use composable_query_builder::ComposableQueryBuilder;
//! let query = ComposableQueryBuilder::new()
//!     .table("users")
//!     .where_clause("id = ?", 1);
//!
//! let query = match status_id {
//!     Some(status_id) => query.where_clause("status_id = ?", status_id),
//!     None => query,
//! };
//! let query = query.into_builder();
//!
//! let sql = query.sql();
//! assert_eq!("select * from users where id = $1 and status_id = $2", sql);
//! ```
mod order;

use chrono::NaiveDateTime;
use itertools::{EitherOrBoth, Itertools};
use sqlx::{Postgres, QueryBuilder};

pub use order::OrderDir;

#[derive(Clone)]
pub enum TableType {
    Simple(String),
    Complex(String, Vec<ComposableQueryBuilder>),
}

#[derive(Clone)]
pub struct ComposableQueryBuilder {
    table: TableType,
    select: Vec<String>,
    group_by: Vec<String>,
    joins: Vec<String>,
    where_clause: WhereClauses,
    limit: Option<u64>,
    offset: Option<u64>,
    order_by: Option<(String, OrderDir)>,
}

impl ComposableQueryBuilder {
    pub fn new() -> Self {
        Self {
            table: TableType::Simple(String::new()),
            select: vec![],
            group_by: vec![],
            joins: vec![],
            where_clause: WhereClauses::new(),
            limit: None,
            offset: None,
            order_by: None,
        }
    }

    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.table = TableType::Simple(table.into());
        self
    }

    pub fn complex_table(
        mut self,
        complex_table: impl Into<String>,
        parts: Vec<ComposableQueryBuilder>,
    ) -> Self {
        self.table = TableType::Complex(complex_table.into(), parts);
        self
    }

    pub fn select(mut self, select: impl Into<String>) -> Self {
        self.select.push(select.into());
        self
    }

    pub fn select_many(mut self, select: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.select.extend(select.into_iter().map(|s| s.into()));
        self
    }

    pub fn group_by(mut self, group_by: impl Into<String>) -> Self {
        self.group_by.push(group_by.into());
        self
    }

    pub fn group_by_many(mut self, group_by: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.group_by.extend(group_by.into_iter().map(|s| s.into()));
        self
    }

    pub fn join(mut self, join: impl Into<String>) -> Self {
        self.joins.push(join.into());
        self
    }

    pub fn where_clause(mut self, where_clause: impl Into<String>, v: impl Into<SQLValue>) -> Self {
        self.where_clause.push(where_clause.into(), v);
        self
    }

    pub fn where_if(mut self, condition: bool, cb: impl Fn() -> (String, SQLValue)) -> Self {
        if !condition {
            return self;
        }

        let (s, v) = cb();
        self.where_clause.push(s, v);

        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn order_by(mut self, col: impl ToString, dir: OrderDir) -> Self {
        self.order_by = Some((col.to_string(), dir));
        self
    }

    pub fn parts(self) -> (String, Vec<SQLValue>) {
        let mut vals = vec![];

        let mut str = "select ".to_string();
        // let mut str = "select\n    ".to_string();

        if self.select.is_empty() {
            str.push('*');
        } else {
            str.push_str(&self.select.join(", "));
        }
        str.push_str(" from ");
        // str.push_str("\nfrom ");

        match self.table {
            TableType::Simple(s) => str.push_str(&s),
            TableType::Complex(s, parts) => {
                let table_parts = s.split('?');

                for pair in table_parts.zip_longest(parts) {
                    match pair {
                        EitherOrBoth::Both(table_part, qb) => {
                            str.push_str(table_part);
                            let (s, parts) = qb.parts();
                            str.push_str(s.as_str());
                            vals.extend(parts);
                        }
                        EitherOrBoth::Left(table_part) => {
                            str.push_str(table_part);
                        }
                        EitherOrBoth::Right(qb) => {
                            let (s, parts) = qb.parts();
                            str.push_str(s.as_str());
                            vals.extend(parts);
                        }
                    }
                }
            }
        }

        for j in self.joins {
            str.push(' ');
            // str.push('\n');
            str.push_str(&j);
        }
        let (where_str, str_values) = self.where_clause.parts();
        str.push_str(&where_str);
        vals.extend(str_values);
        if !self.group_by.is_empty() {
            str.push_str(" group by ");
            // str.push_str("\ngroup by\n    ");
            str.push_str(&self.group_by.join(", "));
        }

        match self.order_by {
            Some((col, dir)) => {
                str.push_str(" order by ");
                str.push_str(&col);
                str.push(' ');
                str.push_str(dir.as_str());
                str.push(' ');
            }
            None => {}
        }

        match self.limit {
            Some(limit) => {
                str.push_str(" limit ");
                vals.push(SQLValue::U64(limit));
            }
            None => {}
        }
        match self.offset {
            Some(offset) => {
                str.push_str(" offset ");
                vals.push(SQLValue::U64(offset));
            }
            None => {}
        }

        (str, vals)
    }

    pub fn into_builder<'args>(self) -> QueryBuilder<'args, Postgres> {
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("");

        let (p, v) = self.parts();
        let parts = p.split('?');

        for pair in parts.zip_longest(v) {
            match pair {
                EitherOrBoth::Both(part, v) => {
                    qb.push(part);
                    v.push_bind(&mut qb);
                }
                EitherOrBoth::Left(part) => {
                    qb.push(part);
                }
                EitherOrBoth::Right(v) => {
                    v.push_bind(&mut qb);
                }
            }
        }

        qb
    }
}

#[derive(Clone)]
pub struct WhereClauses {
    clauses: Vec<(String, SQLValue)>,
}

impl WhereClauses {
    pub fn new() -> Self {
        Self { clauses: vec![] }
    }

    pub fn push(&mut self, clause: impl Into<String>, value: impl Into<SQLValue>) {
        self.clauses.push((clause.into(), value.into()));
    }

    pub fn parts(self) -> (String, Vec<SQLValue>) {
        if self.clauses.is_empty() {
            return ("".to_string(), vec![]);
        }

        // Build up where clauses
        let mut out = " where ".to_string();
        // let mut out = "\nwhere\n".to_string();
        for (i, (s, _)) in self.clauses.iter().enumerate() {
            // out.push_str("    ");
            out.push_str(s.as_str());
            if i != self.clauses.len() - 1 {
                out.push_str(" and ");
                // out.push_str(" and\n");
            }
        }

        (out, self.clauses.into_iter().map(|(_, v)| v).collect())
    }

    // Not sure what this was for?
    // pub fn inject(self, qb: &mut QueryBuilder<Postgres>) {
    //     if !self.clauses.is_empty() {
    //         // Build up where clauses
    //         let mut out = "\nwhere\n".to_string();
    //         for (i, (s, _)) in self.clauses.iter().enumerate() {
    //             out.push_str("    ");
    //             out.push_str(s.as_str());
    //             if i != self.clauses.len() - 1 {
    //                 out.push_str(" and\n");
    //             }
    //         }
    //
    //         let parts = out.split('?');
    //         for pair in parts.zip_longest(self.clauses.into_iter().map(|(_, v)| v)) {
    //             match pair {
    //                 EitherOrBoth::Both(part, v) => {
    //                     qb.push(part);
    //                     v.push_bind(qb);
    //                 }
    //                 EitherOrBoth::Left(part) => {
    //                     qb.push(part);
    //                 }
    //                 EitherOrBoth::Right(v) => {
    //                     v.push_bind(qb);
    //                 }
    //             }
    //         }
    //     }
    // }
}

#[derive(Debug, Clone)]
pub enum SQLValue {
    I16(i16),
    I32(i32),
    I64(i64),
    U64(u64),
    DateTime(NaiveDateTime),
    VecI64(Vec<i64>),
}

impl SQLValue {
    pub fn push_bind(&self, qb: &mut QueryBuilder<Postgres>) {
        match self {
            SQLValue::I16(v) => qb.push_bind(*v),
            SQLValue::I32(v) => qb.push_bind(*v),
            SQLValue::I64(v) => qb.push_bind(*v),
            SQLValue::U64(v) => qb.push_bind(*v as i64),
            SQLValue::DateTime(v) => qb.push_bind(*v),
            SQLValue::VecI64(v) => qb.push_bind(v.clone()),
        };
    }
}

impl From<i16> for SQLValue {
    fn from(v: i16) -> Self {
        SQLValue::I16(v)
    }
}

impl From<i32> for SQLValue {
    fn from(v: i32) -> Self {
        SQLValue::I32(v)
    }
}

impl From<i64> for SQLValue {
    fn from(v: i64) -> Self {
        SQLValue::I64(v)
    }
}

impl From<NaiveDateTime> for SQLValue {
    fn from(v: NaiveDateTime) -> Self {
        SQLValue::DateTime(v)
    }
}

impl From<Vec<i64>> for SQLValue {
    fn from(v: Vec<i64>) -> Self {
        SQLValue::VecI64(v)
    }
}

impl From<u64> for SQLValue {
    fn from(v: u64) -> Self {
        SQLValue::U64(v)
    }
}

#[cfg(test)]
mod composable_query_builder_tests {
    use chrono::{Days, Utc};
    use itertools::{EitherOrBoth, Itertools};
    use sqlx::{Postgres, QueryBuilder};

    use crate::{ComposableQueryBuilder, OrderDir};

    #[test]
    fn limit_works() {
        let q = ComposableQueryBuilder::new()
            .table("users")
            .limit(10)
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users limit $1", query);
    }

    #[test]
    fn offset_works() {
        let q = ComposableQueryBuilder::new()
            .table("users")
            .offset(10)
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users offset $1", query);
    }

    #[test]
    fn order_by_works() {
        let q = ComposableQueryBuilder::new()
            .table("users")
            .order_by("email", OrderDir::Desc)
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users order by email desc ", query);

        let q = ComposableQueryBuilder::new()
            .table("users")
            .order_by("email", OrderDir::Asc)
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users order by email asc ", query);
    }

    #[test]
    fn basic_where() {
        let q = ComposableQueryBuilder::new()
            .where_clause("id = ?", 123)
            .where_clause("status_id = ?", 3)
            .into_builder();
        let query = q.sql();

        println!("{}", query);
    }

    #[test]
    fn qb_test() {
        let profile_id = 123;
        let lookback_days = Some(30i32);
        let ad_group_ids = vec![1, 2, 3];

        let perf_fields: Vec<String> = vec![];

        let targets = ComposableQueryBuilder::new()
            .table("target_performances")
            .select_many(perf_fields.clone())
            .select("target_performances.search_term")
            .select("target_performances.ad_group_id")
            .select("target_performances.target_id")
            .select("target_performances.campaign_id")
            .select("target_performances.profile_id")
            .select("targets.bid")
            .join("left join targets on target_performances.target_id = targets.target_id")
            .join(format!(
                "right join (values ({})) vals(v1) on (target_performances.ad_group_id = v1)",
                ad_group_ids.iter().map(|x| x.to_string()).join("), (")
            ))
            .group_by_many(vec![
                "target_performances.search_term",
                "target_performances.target_id",
                "target_performances.ad_group_id",
                "target_performances.campaign_id",
                "target_performances.profile_id",
                "targets.bid",
            ])
            .where_clause("target_performances.profile_id = ?", profile_id)
            .where_if(lookback_days.is_some(), || {
                (
                    "date >= ?".into(),
                    Utc::now()
                        .naive_utc()
                        .checked_sub_days(Days::new(lookback_days.unwrap() as u64))
                        .unwrap()
                        .into(),
                )
            });

        let keywords = ComposableQueryBuilder::new()
            .table("keyword_performances")
            .select_many(perf_fields)
            .select("keyword_performances.search_term")
            .select("keyword_performances.ad_group_id")
            .select("keyword_performances.keyword_id")
            .select("keyword_performances.campaign_id")
            .select("keyword_performances.profile_id")
            .select("keywords.bid")
            .join("left join keywords on keyword_performances.keyword_id = keywords.keyword_id")
            .join(format!(
                "right join (values ({})) vals(v1) on (keyword_performances.ad_group_id = v1)",
                ad_group_ids.iter().map(|x| x.to_string()).join("), (")
            ))
            .group_by_many(vec![
                "keyword_performances.search_term",
                "keyword_performances.keyword_id",
                "keyword_performances.ad_group_id",
                "keyword_performances.campaign_id",
                "keyword_performances.profile_id",
                "keywords.bid",
            ])
            .where_clause("keyword_performances.profile_id = ?", profile_id)
            .where_if(lookback_days.is_some(), || {
                (
                    "date >= ?".into(),
                    Utc::now()
                        .naive_utc()
                        .checked_sub_days(Days::new(lookback_days.unwrap() as u64))
                        .unwrap()
                        .into(),
                )
            });

        let f = ComposableQueryBuilder::new()
            .complex_table("((?)\nunion\n(?)) t", vec![targets, keywords])
            .select_many(vec![
                "array_agg(coalesce(bid,0)) as bid_array",
                "array_agg(sales) as sales_Array",
                "array_agg(ad_group_id) as ad_group_id_array",
                "sum(cost) as cost",
                "sum(impressions) as impressions",
                "sum(clicks) as clicks",
                "sum(sales) as sales",
                "sum(orders) as orders",
                "case sum(sales) when 0 then 0 else sum(cost) / sum(sales) * 100.0 end as acos",
                "case sum(cost) when 0 then 0 else sum(sales) / sum(cost) end as roas",
                "case sum(clicks) when 0 then 0 else sum(cost) / sum(clicks) end as cpc",
                "case sum(impressions) when 0 then 0 else sum(clicks) / sum(impressions) * 100.0 end as ctr",
                "case sum(clicks) when 0 then 0 else sum(orders) / sum(clicks) * 100.0 end as cvr",
                "case sum(orders) when 0 then 0 else sum(cost) / sum(orders) * 100.0 end as cac",
                "search_term",

                // Some magic here to resolve the bid. If there are no sales for that search term:
                // - we are doing a simple average
                // otherwise:
                // - we are doing a weighted average based on the sales
                //
                // example 1:
                // - we have no sales for a search term
                // - 2 keywords that shows up for that search term with a bid of $0.10 and $0.15,
                // - 1 target that shows up for that search term with a bid of $0.25
                // - the resolved bid will be ($0.10 + $0.15 + $0.25 = $0.50) / 3 = $0.1666 = $0.17
                //
                // example 2:
                // - we have $500 in sales for a search term
                // - 1 keyword that shows up for that search term with a bid of $0.10 and $100 in sales,
                // - 1 target that shows up for that search term with a bid of $0.20 and $200 in sales,
                // - 1 target that shows up for that search term with a bid of $0.50 and $200 in sales,
                // - the resolved bid will be ($0.10*$100 + $0.20*$200 + $0.50*$200 = $150) / $500 = $0.30
                "case sum(sales) when 0 then round(sum(bid)/count(bid))::integer else round(sum(bid*sales)/sum(sales))::integer end as bid",
            ]).group_by("search_term");

        let final_query = ComposableQueryBuilder::new().complex_table("(?) as u", vec![f]);
        // .where_if(|| None);

        let (p, v) = final_query.parts();

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("");
        let parts = p.split('?');
        for pair in parts.zip_longest(v) {
            match pair {
                EitherOrBoth::Both(part, v) => {
                    qb.push(part);
                    v.push_bind(&mut qb);
                }
                EitherOrBoth::Left(part) => {
                    qb.push(part);
                }
                EitherOrBoth::Right(v) => {
                    v.push_bind(&mut qb);
                }
            }
        }

        println!("{}", qb.sql());
    }
}
