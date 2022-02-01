#![feature(async_closure)]
#![feature(fn_traits)]
#![feature(type_alias_impl_trait)]


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

