// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::Path;

fn main() -> Result<(), String> {
    use std::io::Write;

    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    let version = rustc_version::version().unwrap();
    println!("cargo:rustc-env=RUSTC_VERSION={version}");

    let path = "src/proto/generated/protobuf.rs";

    // We don't include the proto files in releases so that downstreams
    // do not need to have PROTOC included
    if Path::new("src/proto/datafusion_ray.proto").exists() {
        println!("cargo:rerun-if-changed=src/proto/datafusion_common.proto");
        println!("cargo:rerun-if-changed=src/proto/datafusion.proto");
        println!("cargo:rerun-if-changed=src/proto/datafusion_ray.proto");
        tonic_build::configure()
            .extern_path(".datafusion", "::datafusion_proto::protobuf")
            .extern_path(".datafusion_common", "::datafusion_proto::protobuf")
            .compile_protos(&["src/proto/datafusion_ray.proto"], &["src/proto"])
            .map_err(|e| format!("protobuf compilation failed: {e}"))?;
        let generated_source_path = out.join("datafusion_ray.protobuf.rs");
        let code = std::fs::read_to_string(generated_source_path).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();
        file.write_all(code.as_str().as_ref()).unwrap();
    }

    Ok(())
}
