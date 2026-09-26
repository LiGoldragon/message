# Message work

Work here for the Message Nexus and its two CLIs. Read `ARCHITECTURE.md`.

The public Types belong to `signal-message` and `meta-signal-message`; Flow's
delivery Types to `meta-signal-flow`. Use them directly.

Message never writes a pane and never runs Herdr: delivery is Flow's
`Deliver`. `scripts/message-cannot-invoke-herdr` holds that line. The
Nexus-as-a-process tests in `crates/message-nexus/tests` run against a scripted
Flow; a change to delivery or parking gets a witness there.
