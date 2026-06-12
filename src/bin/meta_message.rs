use message::meta::MetaMessageCommand;

#[tokio::main]
async fn main() {
    if let Err(error) = MetaMessageCommand::from_env()
        .run(std::io::stdout().lock())
        .await
    {
        eprintln!("meta-message: {error}");
        std::process::exit(1);
    }
}
