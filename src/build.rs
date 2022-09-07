use std::io::Result;

fn main() -> Result<()> {
    // 编译 protoc
    std::env::set_var("PROTOC", protobuf_src::protoc());
    // 使用 protoc 编译 proto 文件
    prost_build::compile_protos(&["proto/message.proto"], &["proto/"])?;
    // tonic_build::compile_protos("proto/message.proto")?;
    Ok(())
}