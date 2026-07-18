use std::path::{Path, PathBuf};
use std::process::Command;

/// The messenger daemon renders agent-facing delivery text (the PTY
/// programmatic-input leg), so nota is a genuine runtime dependency since
/// packet 3.2a — the former binary-only/no-nota split is deliberately
/// retired.
#[test]
fn runtime_dependency_tree_carries_nota_for_the_delivery_text_edge() {
    let manifest = CargoManifest::from_environment();
    let tree = manifest.cargo_tree(&["--edges", "normal"]);

    assert!(
        tree.contains("nota"),
        "runtime dependency tree must contain nota for PTY delivery rendering:\n{tree}"
    );
}

#[test]
fn nota_text_runtime_dependency_tree_contains_nota() {
    let manifest = CargoManifest::from_environment();
    let tree = manifest.cargo_tree(&["--edges", "normal", "--features", "nota-text"]);

    assert!(
        tree.contains("nota"),
        "nota-text runtime dependency tree must contain nota:\n{tree}"
    );
}

struct CargoManifest {
    path: PathBuf,
}

impl CargoManifest {
    fn from_environment() -> Self {
        Self {
            path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
        }
    }

    fn cargo_tree(&self, arguments: &[&str]) -> String {
        let output = Command::new("cargo")
            .arg("tree")
            .arg("--manifest-path")
            .arg(self.path())
            .args(arguments)
            .output()
            .expect("run cargo tree");

        assert!(
            output.status.success(),
            "cargo tree failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8(output.stdout).expect("cargo tree stdout is utf8")
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }
}
