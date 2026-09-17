use message::command::CommandLine;
use std::io::Read;

fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let command_line = if arguments.is_empty() {
        let mut input = String::new();
        if let Err(error) = std::io::stdin().read_to_string(&mut input) {
            eprintln!("message: read inline Datom from stdin: {error}");
            std::process::exit(1);
        }
        CommandLine::from_arguments([input.trim()])
    } else {
        CommandLine::from_arguments(arguments)
    };
    if let Err(error) = command_line.run(std::io::stdout().lock()) {
        eprintln!("message: {error}");
        std::process::exit(1);
    }
}
