use std::{fs, io::Write, os::unix::fs::OpenOptionsExt, path::{Path, PathBuf}, process::Command};

use serde::Deserialize;
use serde_json::Value;
use signal_message::Query;

use crate::{Error, Result, client::MessageSocket};

/// The ordinary Message CLI is a direct Datom view of the producer contract.
/// It does not own a friendlier request or reply vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLine {
    arguments: Vec<String>,
}

impl CommandLine {
    pub fn from_env() -> Self {
        Self {
            arguments: std::env::args().skip(1).collect(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn decode_query(&self) -> Result<Query> {
        let text = crate::text::sole_argument(&self.arguments)?;
        Ok(crate::text::read::<Query>(text)?)
    }

    pub fn run(&self, mut output: impl Write) -> Result<()> {
        if let [command, rendered] = self.arguments.as_slice() && command == "cluster" {
            let verified = crate::cluster::canonical(rendered)
                .map_err(|detail| Error::InvalidCommandArgument { detail })?;
            writeln!(output, "{verified}")?;
            return Ok(());
        }
        if let [command, header, body_flag, body_path, routes_flag, routes_path, to_flag, target] = self.arguments.as_slice()
            && command == "cluster" && body_flag == "--body-file" && routes_flag == "--route-config" && to_flag == "--to"
        {
            let body = fs::read_to_string(body_path)?;
            let canonical = crate::cluster::verify(header, &body)
                .map_err(|detail| Error::InvalidCommandArgument { detail })?;
            let (flow_identifier, session_identifier) = target.split_once('@').filter(|(flow, session)| !flow.is_empty() && !session.is_empty())
                .ok_or_else(|| Error::InvalidCommandArgument { detail: "--to must be FLOW@SESSION".into() })?;
            let routes = FlowRoutes::read(Path::new(routes_path))?;
            let route = routes.exact(flow_identifier, session_identifier)?;
            let header_file = TemporaryHeader::write(&canonical)?;
            let receipt = route.deliver(&header_file.path, Path::new(body_path), session_identifier)?;
            writeln!(output, "{}", serde_json::to_string(&receipt).map_err(|error| Error::InvalidCommandArgument { detail: error.to_string() })?)?;
            return Ok(());
        }
        let socket = MessageSocket::from_environment().ok_or(Error::SignalMessageSocketMissing)?;
        let reply = socket.client().submit(self.decode_query()?)?;
        writeln!(output, "{}", crate::text::write(&reply))?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct FlowRoutes { routes: Vec<FlowRoute> }
#[derive(Deserialize)]
struct FlowRoute { flow_identifier: String, session_identifier: String, harness: String, readiness: String, endpoint: PathBuf }
impl FlowRoutes {
    fn read(path: &Path) -> Result<Self> { serde_json::from_str(&fs::read_to_string(path)?).map_err(|error| Error::InvalidCommandArgument { detail: format!("parse Flow route config {}: {error}", path.display()) }) }
    fn exact(&self, flow: &str, session: &str) -> Result<&FlowRoute> {
        let matches = self.routes.iter().filter(|route| route.flow_identifier == flow && route.session_identifier == session).collect::<Vec<_>>();
        let [route] = matches.as_slice() else { return Err(Error::InvalidCommandArgument { detail: "Flow route config must contain exactly one selected target".into() }); };
        if route.harness != "claude" || route.readiness != "idle" { return Err(Error::InvalidCommandArgument { detail: "selected Flow route is not an idle configured Claude prompt-relay bridge".into() }); }
        Ok(route)
    }
}
impl FlowRoute {
    fn deliver(&self, header: &Path, body: &Path, session: &str) -> Result<Value> {
        let output = Command::new(&self.endpoint).args(["claude", "--source"]).arg(body).args(["--source-format", "peer-file", "--datom-file"]).arg(header).args(["--session-short", session]).output()
            .map_err(|error| Error::InvalidCommandArgument { detail: format!("start configured prompt-relay {}: {error}", self.endpoint.display()) })?;
        if !output.status.success() { return Err(Error::InvalidCommandArgument { detail: format!("configured prompt-relay refused: {}", String::from_utf8_lossy(&output.stderr)) }); }
        let receipt: Value = serde_json::from_slice(&output.stdout).map_err(|error| Error::InvalidCommandArgument { detail: format!("parse prompt-relay receipt: {error}") })?;
        if receipt.get("kind").and_then(Value::as_str) != Some("claude-bytes-written-to-pty") || receipt.get("session_id").and_then(Value::as_str) != Some(session) { return Err(Error::InvalidCommandArgument { detail: "configured prompt-relay did not acknowledge the selected PTY write".into() }); }
        Ok(receipt)
    }
}
struct TemporaryHeader { path: PathBuf }
impl TemporaryHeader {
    fn write(header: &str) -> Result<Self> {
        let path = std::env::temp_dir().join(format!("message-cluster-header-{}", std::process::id()));
        let mut file = fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path)?;
        file.write_all(header.as_bytes())?;
        Ok(Self { path })
    }
}
impl Drop for TemporaryHeader { fn drop(&mut self) { let _ = fs::remove_file(&self.path); } }
