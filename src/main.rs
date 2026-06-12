use message::command::CommandLine;

fn main() {
    let command_line = CommandLine::from_env();
    if let Err(error) = command_line.run(std::io::stdout().lock()) {
        eprintln!("message: {error}");
        std::process::exit(1);
    }
}
