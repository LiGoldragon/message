# cf7879 configured relay fanout receipt

Source base: `0b7b90d9d617170f8ebff4a18c0510adab66873e`.

The proposal adds configured, trusted route adapters only. Claude receives a
private peer-file containing the typed `Relay` header plus original body and
requires its matching PTY-write receipt. A configured busy Nexus route parks
the same header-plus-body packet through `FlowDeliver`; its source event key
uses the selected transcript source, separate from the executor.

Local focused process tests: 3 passed (Claude peer-file receipt, busy Nexus
park/drain, existing Codex fanout). Remote focused Nix result: pending.
