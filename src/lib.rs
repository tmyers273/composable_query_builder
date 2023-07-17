//! # Overview
//!
//! composable_query_builder is a library that provides composable query builders for SQLx and
//! makes building dynamic queries easier.
//!
//! # Motivation
//!
//! This library is meant to sit somewhere between a typical ORM and raw SQL, similar in
//! nature to Golang's [squirrel](https://github.com/Masterminds/squirrel) package. Think
//! of it as a giant Builder pattern for SQL.
//!
//!
//! SQLx's built-in query builder has a few limitations that make for a painful developer
//! experience.
//!   1. it is not composable
//!   2. does not allow for easy dynamic query building
//!   3. the order of the query builder methods is important
//!
//! composer_query_builders aims to solve all these problems.
//!
//! This is currently only tested with Postgres.
//!
//! ### Query is not type checked
//!
//! It is your responsibility to ensure that you produce a syntactically correct query here,
//! this API has no way to check it for you.
//!
//! ### Status: This is a work in progress.
//! We currently use it in production, but the API is still subject to breaking changes.
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
//!
//! let query = ComposableQueryBuilder::new()
//!     .table("users")
//!     .where_clause("id = ?", 1);
//! // We'll had a where clause to the status_id field if it's Some
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

    /// Sets the table name for the query.
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

    /// Adds a single column to the select clause.
    pub fn select(mut self, select: impl Into<String>) -> Self {
        self.select.push(select.into());
        self
    }

    /// Adds multiple columns to the select clause.
    pub fn select_many(mut self, select: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.select.extend(select.into_iter().map(|s| s.into()));
        self
    }

    /// Adds a single group by clause
    pub fn group_by(mut self, group_by: impl Into<String>) -> Self {
        self.group_by.push(group_by.into());
        self
    }

    /// Adds multiple group by clause
    pub fn group_by_many(mut self, group_by: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.group_by.extend(group_by.into_iter().map(|s| s.into()));
        self
    }

    /// Adds a single join clause
    /// ```rust
    /// use composable_query_builder::ComposableQueryBuilder;
    /// let query = ComposableQueryBuilder::new()
    ///    .table("users")
    ///    .join("left join subscriptions on subscriptions.user_id = users.id")
    ///    .into_builder();
    /// let sql = query.sql();
    ///
    /// assert_eq!("select * from users left join subscriptions on subscriptions.user_id = users.id", sql);
    pub fn join(mut self, join: impl Into<String>) -> Self {
        self.joins.push(join.into());
        self
    }

    /// Adds a single where clause. Values are expected to be denoted via a `?` placeholder.
    ///
    /// ```rust
    /// use composable_query_builder::ComposableQueryBuilder;
    /// let query = ComposableQueryBuilder::new()
    ///   .table("users")
    ///   .where_clause("id = ?", 1);
    /// let sql = query.sql();
    ///
    /// assert_eq!("select * from users where id = $1", sql);
    /// ```
    pub fn where_clause(mut self, where_clause: impl Into<String>, v: impl Into<SQLValue>) -> Self {
        self.where_clause.push(where_clause.into(), v);
        self
    }

    /// Conditionally add a [where_clause](ComposableQueryBuilder::where_clause). The given
    /// callback is lazily evaluated, so it's only called if the condition is true.
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

    pub fn limit_opt(mut self, limit: Option<u64>) -> Self {
        match limit {
            Some(limit) => self.limit = Some(limit),
            None => self.limit = None,
        }
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn offset_opt(mut self, offset: Option<u64>) -> Self {
        match offset {
            Some(offset) => self.offset = Some(offset),
            None => self.offset = None,
        }
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
struct WhereClauses {
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
}

/// SQLValue is an enum wrapper around the various types that can be bound to a query.
///
/// This allows us to do some fairly magic looking things with the query builder, in
/// particular with the where clause. For example, the same [where_clause](ComposableQueryBuilder::where_clause)
/// can be used for both a string and int column.
///
/// ```rust
/// use composable_query_builder::ComposableQueryBuilder;
/// let query = ComposableQueryBuilder::new()
///     .table("users")
///     .where_clause("status_id = ?", 2) // an int
///     .where_clause("email = ?", "test@example") // a string
///     .into_builder();
///
/// let sql = query.sql();
/// assert_eq!("select * from users where status_id = $1 and email = $2", sql);
/// ```
#[derive(Debug, Clone)]
pub enum SQLValue {
    I16(i16),
    I32(i32),
    I64(i64),
    U64(u64),
    F64(f64),
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
            SQLValue::F64(v) => qb.push_bind(*v),
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

impl From<f64> for SQLValue {
    fn from(v: f64) -> Self {
        SQLValue::F64(v)
    }
}

#[cfg(test)]
mod composable_query_builder_tests {
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
    fn limit_opt_works() {
        let q = ComposableQueryBuilder::new()
            .table("users")
            .limit_opt(Some(10))
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users limit $1", query);

        let q = ComposableQueryBuilder::new()
            .table("users")
            .limit_opt(None)
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users", query);
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
    fn offset_opt_works() {
        let q = ComposableQueryBuilder::new()
            .table("users")
            .offset_opt(Some(10))
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users offset $1", query);

        let q = ComposableQueryBuilder::new()
            .table("users")
            .offset_opt(None)
            .into_builder();
        let query = q.sql();

        assert_eq!("select * from users", query);
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
}
