#![feature(get_mut_unchecked)]
#![feature(once_cell)]
#![feature(fn_traits)]

pub mod core;
pub mod message;
mod utils;


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

