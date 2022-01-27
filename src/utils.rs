use rand::Rng;
use uuid::Uuid;


pub(crate) fn get_uuid_as_string() -> String {
    let my_uuid = Uuid::new_v4();
    my_uuid.to_string()
}

pub(crate) fn get_random_number(max:usize) -> usize{
    let mut rng = rand::thread_rng();
    rng.gen_range(0..max)
}