pub mod proto {
    pub mod storage {
        pub mod v1 {
            tonic::include_proto!("storage.v1");
        }
    }
}

fn main() {
    println!("Hello, world!");
}
