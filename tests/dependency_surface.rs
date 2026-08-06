#[test]
fn manifest_names_dotos_and_exact_producers_without_local_generation() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read manifest");
    assert!(manifest.contains("dotos-text"));
    assert!(manifest.contains("1c7e2e0209cd0e27ce01c290e3318a20905e1142"));
    assert!(manifest.contains("c349f1252f0544fced18912abdb845bfc95ba826"));
    assert!(!manifest.contains("schema-rust"));
    assert!(!manifest.contains("branch ="));
}
