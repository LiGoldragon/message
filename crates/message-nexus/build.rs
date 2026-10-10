use ethos_zero::{Actualizing, File, Generating, Potential};

fn check(root: &std::path::Path, stem: &str) {
    let source_path = root.join(format!("ethos/{stem}.ethos"));
    let generated_path = root.join(format!("src/generated/{stem}.rs"));
    println!("cargo:rerun-if-changed={}", source_path.display());
    println!("cargo:rerun-if-changed={}", generated_path.display());
    let source = std::fs::read_to_string(source_path).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .unwrap_or_else(|_| panic!("read {stem}"));
    let generated = file
        .generate()
        .unwrap_or_else(|_| panic!("generate {stem}"));
    assert_eq!(
        generated,
        std::fs::read_to_string(generated_path).expect("generated")
    );
}

fn main() {
    let root = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"),
    );
    check(&root, "memory");
    check(&root, "operation");
}
