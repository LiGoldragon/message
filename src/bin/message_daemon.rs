use message::Result;
use message::daemon::MessageDaemon;
use nota_config::ConfigurationSource;
use signal_message::MessageDaemonConfiguration;

fn main() -> Result<()> {
    let configuration: MessageDaemonConfiguration = ConfigurationSource::from_argv()?.decode()?;
    MessageDaemon::from_configuration(configuration).run()
}
