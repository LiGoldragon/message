//! Architectural-truth witnesses for message's actor
//! discipline.
//!
//! - Public actor nouns are data-bearing — `mem::size_of::<X>() > 0`.
//! - No shared `Arc<Mutex<_>>` / `Arc<RwLock<_>>` between actors
//!   (per `~/primary/skills/actor-systems.md` §"No shared locks").
//!
//! A future refactor that collapses an actor noun to a marker
//! ZST, or wires shared locks between actors, breaks these
//! witnesses.

use std::fs;
use std::path::{Path, PathBuf};

use message::daemon::MessageDaemonRoot;
use message::supervision::SupervisionPhase;

#[test]
fn public_actor_nouns_carry_data() {
    assert!(std::mem::size_of::<MessageDaemonRoot>() > 0);
    assert!(std::mem::size_of::<SupervisionPhase>() > 0);
}

#[test]
fn actor_source_does_not_share_locks_between_actors() {
    let forbidden = [
        ("Arc<Mutex", "shared mutex state between actors"),
        ("Arc < Mutex", "shared mutex state between actors"),
        ("RwLock", "shared read-write lock state between actors"),
    ];

    let mut violations: Vec<String> = Vec::new();
    for path in production_source_files() {
        let text = fs::read_to_string(&path).expect("read source file");
        for (fragment, reason) in forbidden {
            for (index, line) in text.lines().enumerate() {
                if line.contains(fragment) {
                    violations.push(format!(
                        "{}:{}: {reason} ({line})",
                        path.display(),
                        index + 1,
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "shared-lock violations in actor source:\n{}",
        violations.join("\n"),
    );
}

fn production_source_files() -> Vec<PathBuf> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = crate_root.join("src");
    let mut output = Vec::new();
    collect_rust_files(&src, &mut output);
    output
}

fn collect_rust_files(directory: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}
