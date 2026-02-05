pub mod error;
pub mod tensor;

pub use tensor::*;

pub use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
