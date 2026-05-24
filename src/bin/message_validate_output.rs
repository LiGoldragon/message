use message::Result;
use message::output_validator::OutputValidatorCommandLine;

fn main() -> Result<()> {
    OutputValidatorCommandLine::from_environment().run()
}
