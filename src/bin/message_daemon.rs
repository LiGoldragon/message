use message::{DaemonEntry, MessageDaemon};

fn main() -> std::process::ExitCode {
    MessageDaemon::run_to_exit_code()
}
