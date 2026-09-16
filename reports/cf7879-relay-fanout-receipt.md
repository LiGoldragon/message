# cf7879 configured relay fanout receipt

Source base: `0b7b90d9d617170f8ebff4a18c0510adab66873e`.
Published forward proposal: `bef42911a60d4423c9419c132d06f42920cb4969`.

The proposal adds configured, trusted route adapters only. Claude receives a
private peer-file containing the typed `Relay` header plus original body and
requires its matching PTY-write receipt. The adapter kills a Claude child after
ten seconds or when either captured output file exceeds 64 KiB. A configured
busy Nexus route parks the header-plus-body packet through `FlowDeliver`; its
source event key uses the selected transcript source, separate from the
executor. A failed Claude PTY route remains unavailable and is never promoted
to a busy Nexus route.

Local focused process tests passed: Claude peer-file receipt, busy Nexus
park/drain through a temporary daemon with isolated HOME, and existing Codex
fanout (3/3).

Remote focused Nix passed on Prometheus using `--max-jobs 0 --no-link`:
`message-relay-flow-route-fanout-fixture` and
`message-relay-configured-claude-peer-file-fixture`, each 1/1. The initial
Claude fixture used a host-specific `cp` path and failed remotely; the
published successor uses only shell builtins and the rerun passed.
