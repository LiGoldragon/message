use message::MessageDaemon;

fn main() -> std::process::ExitCode {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(path) = arguments.next() else {
        eprintln!("message-daemon: expected one binary configuration path");
        return std::process::ExitCode::FAILURE;
    };
    if arguments.next().is_some() {
        eprintln!("message-daemon: expected one binary configuration path");
        return std::process::ExitCode::FAILURE;
    }
    match MessageDaemon::from_configuration_path(std::path::Path::new(&path)).and_then(|d| d.run())
    {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("message-daemon: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
