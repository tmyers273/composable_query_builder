#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum OrderDir {
    Asc,
    Desc,
}

impl OrderDir {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderDir::Asc => "asc",
            OrderDir::Desc => "desc",
        }
    }
}

impl ToString for OrderDir {
    fn to_string(&self) -> String {
        match self {
            OrderDir::Asc => "asc".to_string(),
            OrderDir::Desc => "desc".to_string(),
        }
    }
}
