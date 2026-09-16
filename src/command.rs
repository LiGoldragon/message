use std::io::Write;

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
        let socket = MessageSocket::from_environment().ok_or(Error::SignalMessageSocketMissing)?;
        let reply = socket.client().submit(self.decode_query()?)?;
        writeln!(output, "{}", crate::text::write(&reply))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_message::{
        FlowDeliveryRequest, PromptInterpretationSelection, PromptVariant, TypedPromptEnvelope,
    };

    /// The CLI owns no friendlier vocabulary than `Query` itself: a
    /// `FlowDeliver` value passed inline decodes exactly like any other
    /// `Query` variant, with no new arg parsing for this operation.
    #[test]
    fn a_flow_deliver_datom_value_decodes_as_the_flow_deliver_query() {
        let command_line = CommandLine::from_arguments([
            r#"FlowDeliver.{ { HumanPrompt «source-event-1» «land this on the target flow» None } 57a7aa }"#,
        ]);
        let expected = Query::FlowDeliver(FlowDeliveryRequest {
            typed_prompt_envelope: TypedPromptEnvelope {
                prompt_variant: PromptVariant::HumanPrompt,
                source_event_identifier: "source-event-1".to_owned(),
                raw_prompt_text: "land this on the target flow".to_owned(),
                prompt_interpretation_selection: PromptInterpretationSelection::None,
            },
            target_flow_name: "57a7aa".to_owned(),
        });
        assert_eq!(command_line.decode_query().unwrap(), expected);
    }
}
