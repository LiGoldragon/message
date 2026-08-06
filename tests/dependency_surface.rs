#[test]
fn manifest_names_dotos_and_exact_producers_without_local_generation() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read manifest");
    let lockfile = include_str!("../Cargo.lock");
    assert!(manifest.contains("dotos-text"));
    assert!(manifest.contains("dff3fbf3f9e2cd018f06bcf96a06c8367d3e7f31"));
    assert!(manifest.contains("96099d5f240cf16235d35a78cde80d384ec28158"));
    assert!(!manifest.contains("schema-rust"));
    assert!(!manifest.contains("branch ="));
    assert!(!lockfile.contains("?branch="));
    assert_eq!(lockfile.matches("name = \"signal-frame\"").count(), 1);
    assert_eq!(lockfile.matches("name = \"schema-rust\"").count(), 1);
    assert!(lockfile.contains(
        "signal-frame.git?rev=8aa0bcaeb29fe9e461a11706a469638d2fd109ac#8aa0bcaeb29fe9e461a11706a469638d2fd109ac"
    ));
    assert!(lockfile.contains(
        "schema-rust.git?rev=664335240a40728826cfaa09e3100cd867031912#664335240a40728826cfaa09e3100cd867031912"
    ));
}
