# Message branch consolidation, 2026-10-13

Current main uses typed Message Standard2 and Flow-owned delivery. This account resolves obsolete runtime paths explicitly; it does not claim the old subsystems are deployed.

The architecture retirement is commit f0cf9b50cc23e18ef1110e0faa4c5c7a0c695df9: "Retired: the Herdr delivery path, the relay and cluster binaries, the agent registry, inbox, threads and the flow-delivery park (F9)." The source commits remain in merge ancestry; no branches or stores are deleted.

The current store opener now checks the actual Standard2 catalog read-only before opening an Engine writer. Tests are authored, awaiting Field execution with the final pair.

## datom-cli-cf7879

Tip: `21768e59055872953a725d5e5f6b579dffd0f74b`.

Contents: Verify producer ClusterMessage Datoms before adapter delivery.

Keep the current typed Message Query CLI and its producer decoding. Do not reinstall ClusterMessage transport, relay-forward commands or external prompt injection: the root cluster binaries were explicitly retired by f0cf9b50. Their producer and route vocabulary is not the current Send/Address/Request protocol. No old producer-pin downgrade.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## flow-delivery-poc

Tip: `5d917535ec9844a8f2dd78329f9564650a4db935`.

Contents: Make the flow park lose no words: atomic landing, arrival order, conflict refusal; Add the CLI front-end for FlowDeliver and a Rust extraction helper.

Do not recreate Message-owned panes, agent registry, transcript extraction, relay transport, inbox or flow-delivery park: f0cf9b50 explicitly retired them. Preserve typed peer identification and Flow-owned delivery. The paired Flow reconciliation carries an atomic ordered Request queue with exact lease consumption; Message cannot acknowledge native delivery from a parked record.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## flow/message-lifecycle-hold-47764b

Tip: `75356175b8b9913eec0a2f59ed141e3e55dfa7d4`.

Contents: Hold Pending Flow deliveries before transport.

Keep Identify -> Lock -> Deliver with typed refusal propagation in crates/message-nexus/src/service.rs. Do not restore the deleted root nexus_delivery adapter or old Flow-v2/v3 producers. A failed or lapsed lock does not proceed to transport; no pending delivery is silently treated as live.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## flow/message-lifecycle-v3-47764b

Tip: `a283368d104bb2f4a967fa537ec3742cb2e145a0`.

Contents: Consume typed Flow v3 recipient dispositions; Integrate Pending delivery hold with Flow v2 signal; Pin Message to Flow v2 signal.

Keep Identify -> Lock -> Deliver with typed refusal propagation in crates/message-nexus/src/service.rs. Do not restore the deleted root nexus_delivery adapter or old Flow-v2/v3 producers. A failed or lapsed lock does not proceed to transport; no pending delivery is silently treated as live.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## integrated-messenger-poc-34d94e

Tip: `e6eae2538ca4e7c32bf2456a8c377142a40857c0`.

Contents: Rescued work from worktree integrated-messenger-poc-34d94e before cleanup (flow e167d8); Count safe retry attempts and reconcile ambiguous prompt delivery; Retain transport write progress across cancellation; Make prompt dispatch claims atomic; Prove stalled dispatch does not block prompt admission; Separate prompt relay admission from dispatch; Persist source-bound typed prompt relay delivery; Verify source-aware prompt relay receipts; Prove prompt relay ingress across pinned child processes; Persist source identity in relay v2 records; Freeze POC registry after relay bootstrap; Format prompt relay ingress; Exercise kernel-pinned prompt observation ingress; Test kernel-peer prompt relay observation; Authorize typed recipient observation; Add isolated typed prompt relay listener; Authorize typed prompt relay through registered endpoint; Prepare Message for typed prompt relay ingress.

Do not recreate Message-owned panes, agent registry, transcript extraction, relay transport, inbox or flow-delivery park: f0cf9b50 explicitly retired them. Preserve typed peer identification and Flow-owned delivery. The paired Flow reconciliation carries an atomic ordered Request queue with exact lease consumption; Message cannot acknowledge native delivery from a parked record.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## message-store-migration-cf7879

Tip: `6de830a52f70ce5fbe5dd35790c20a3f2817a889`.

Contents: Block v3 rollout on unresolved pending delivery; Clarify v3 store inspector copy requirement; Add offline v3 messenger store migration.

Do not install the schema3/schema5 converter or its old registry/table writers into Standard2. Port the no-write-before-qualification invariant to the current opener (c616f0c); its tests cover foreign/schema3/schema5 unchanged bytes and current Standard2 reopen. Conversion of old relay/agent/pending-delivery records into a configuration-only Standard is not a lossless mapping; no conversion is performed. Original converter, probe, failure-stage and migration implementations remain recoverable in these parents.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## mind-sol-00f95a-recipient-presentation

Tip: `8aa6d7b465c9ba3720eb21f17b833c251b828353`.

Contents: Present hash-free recipient datoms at Herdr boundary; Integrate Pending delivery hold with Flow v2 signal; Pin Message to Flow v2 signal.

Keep current typed Address/Request presentation and Flow-owned pane writes. Do not restore Message src/nexus_delivery.rs or its Flow-v2 pins; recipient presentation belongs at the current Flow boundary, not a second Message Herdr adapter.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## mind-sol-6288d1-message-v2-pending

Tip: `a8c6a924d5d04fbfcbf0a7e2e1b145076d327a53`.

Contents: Integrate Pending delivery hold with Flow v2 signal; Pin Message to Flow v2 signal.

Keep Identify -> Lock -> Deliver with typed refusal propagation in crates/message-nexus/src/service.rs. Do not restore the deleted root nexus_delivery adapter or old Flow-v2/v3 producers. A failed or lapsed lock does not proceed to transport; no pending delivery is silently treated as live.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## mind-sol-6288d1-signal-flow-v2

Tip: `2951857c7c1c5658cb071c6cf32d817f395d21ea`.

Contents: Pin Message to Flow v2 signal.

Keep Identify -> Lock -> Deliver with typed refusal propagation in crates/message-nexus/src/service.rs. Do not restore the deleted root nexus_delivery adapter or old Flow-v2/v3 producers. A failed or lapsed lock does not proceed to transport; no pending delivery is silently treated as live.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-delivery-gate-draft-f72ab7

Tip: `8a6e88ff183cfd6d1e952b661af5f1a407df7ba9`.

Contents: Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-delivery-f72ab7

Tip: `580021b936914b2a0585db1b99cc21a5595d0224`.

Contents: Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-flow-permit-f72ab7

Tip: `f6bf5a1c3e96e919645ab9e35f5b92793f5e2d55`.

Contents: Add typed Message Flow permit bridge seam; Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-flow-permit-transport-f72ab7

Tip: `18e14e3571c549fe6b57fbe8fdf46a1a240ca2c5`.

Contents: Transport Flow permit queries over Message Flow socket; Refuse unauthorized Raw and unavailable FlowLocked delivery; Pin Message to typed delivery refusal producer; Add typed Message Flow permit bridge seam; Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-lock-keys-f72ab7

Tip: `917934a2fa95df0d66db7833f85e7b8a3b83d1ed`.

Contents: Emit resolved Message Git source keys; Add remote Message lock resolver; Preserve delivery submission while selecting visibility; Transport Flow permit queries over Message Flow socket; Refuse unauthorized Raw and unavailable FlowLocked delivery; Pin Message to typed delivery refusal producer; Add typed Message Flow permit bridge seam; Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-lock-resolver-f72ab7

Tip: `94965050a93edfefbde30fcd5a8236caaf5578d3`.

Contents: Add remote Message lock resolver; Preserve delivery submission while selecting visibility; Transport Flow permit queries over Message Flow socket; Refuse unauthorized Raw and unavailable FlowLocked delivery; Pin Message to typed delivery refusal producer; Add typed Message Flow permit bridge seam; Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-producer-pins-f72ab7

Tip: `af8567cea001a2cd6f020dc50944a53b663dd00e`.

Contents: Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-refusal-pin-f72ab7

Tip: `5f85191a01ee82775a5a3f0a75fa30780cae7103`.

Contents: Pin Message to typed delivery refusal producer; Add typed Message Flow permit bridge seam; Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-visibility-borrow-f72ab7

Tip: `f3f03755abe4ce54204403c6974f6a9e8bc1a933`.

Contents: Preserve delivery submission while selecting visibility; Transport Flow permit queries over Message Flow socket; Refuse unauthorized Raw and unavailable FlowLocked delivery; Pin Message to typed delivery refusal producer; Add typed Message Flow permit bridge seam; Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## night-messaging-message-visibility-f72ab7

Tip: `3ba5d946527163a8fd545d9968e4e23db83d0642`.

Contents: Refuse unauthorized Raw and unavailable FlowLocked delivery; Pin Message to typed delivery refusal producer; Add typed Message Flow permit bridge seam; Pin Message to delivery signal successors; Draft durable single-recipient Message delivery attempts; Checkpoint untested in-memory delivery gate draft.

Retain the current Flow-owned lease protocol and explicit Delivered/Queued/Woken outcomes. The old in-memory gate, Raw/FlowLocked permits, visibility transport and remote lock generator targeted the retired root engine/nexus_delivery architecture; do not install a second delivery writer or downgrade the exact current protocol pins. Failed transport releases only the exact lease; no automatic ambiguous replay.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/6852f4-schema3-converter

Tip: `da9792a2f4fc003d2d6c24c2d066e6cd8ce88fcc`.

Contents: Make schema3 publication commit boundary final; Validate schema3 canonical store keys; Harden private schema3 converter publication; Add private schema3 to schema5 converter; Refuse empty schema3 probe inputs; Read schema3 probe requests as typed Datom; Prove schema3 probe against historical populated stores; Contain legacy schema3 decoder panics as refusals; Expose schema3 probe as an explicit binary; Make schema3 probe copy-first with typed outcomes; Probe schema3 stores through a private copy; Add offline exact-graph schema-3 metadata probe.

Do not install the schema3/schema5 converter or its old registry/table writers into Standard2. Port the no-write-before-qualification invariant to the current opener (c616f0c); its tests cover foreign/schema3/schema5 unchanged bytes and current Standard2 reopen. Conversion of old relay/agent/pending-delivery records into a configuration-only Standard is not a lossless mapping; no conversion is performed. Original converter, probe, failure-stage and migration implementations remain recoverable in these parents.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/6852f4-schema3-probe

Tip: `3effa3d1e01a018fbe396d255540e0e5af46620f`.

Contents: Refuse empty schema3 probe inputs; Read schema3 probe requests as typed Datom; Prove schema3 probe against historical populated stores; Contain legacy schema3 decoder panics as refusals; Expose schema3 probe as an explicit binary; Make schema3 probe copy-first with typed outcomes; Probe schema3 stores through a private copy; Add offline exact-graph schema-3 metadata probe.

Do not install the schema3/schema5 converter or its old registry/table writers into Standard2. Port the no-write-before-qualification invariant to the current opener (c616f0c); its tests cover foreign/schema3/schema5 unchanged bytes and current Standard2 reopen. Conversion of old relay/agent/pending-delivery records into a configuration-only Standard is not a lossless mapping; no conversion is performed. Original converter, probe, failure-stage and migration implementations remain recoverable in these parents.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/6852f4-schema3-probe-stage-classifier

Tip: `ef8f09e81cb5b63a46fe6b4b0d9f4f1916458b73`.

Contents: Classify schema3 probe failure stages; Refuse empty schema3 probe inputs; Read schema3 probe requests as typed Datom; Prove schema3 probe against historical populated stores; Contain legacy schema3 decoder panics as refusals; Expose schema3 probe as an explicit binary; Make schema3 probe copy-first with typed outcomes; Probe schema3 stores through a private copy; Add offline exact-graph schema-3 metadata probe.

Do not install the schema3/schema5 converter or its old registry/table writers into Standard2. Port the no-write-before-qualification invariant to the current opener (c616f0c); its tests cover foreign/schema3/schema5 unchanged bytes and current Standard2 reopen. Conversion of old relay/agent/pending-delivery records into a configuration-only Standard is not a lossless mapping; no conversion is performed. Original converter, probe, failure-stage and migration implementations remain recoverable in these parents.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/6852f4-schema3-probe-stage-consumer

Tip: `02a115ce35efca523b4608ae0a0aec47af4c0410`.

Contents: Report typed schema3 probe failure stages; Classify schema3 probe failure stages; Refuse empty schema3 probe inputs; Read schema3 probe requests as typed Datom; Prove schema3 probe against historical populated stores; Contain legacy schema3 decoder panics as refusals; Expose schema3 probe as an explicit binary; Make schema3 probe copy-first with typed outcomes; Probe schema3 stores through a private copy; Add offline exact-graph schema-3 metadata probe.

Do not install the schema3/schema5 converter or its old registry/table writers into Standard2. Port the no-write-before-qualification invariant to the current opener (c616f0c); its tests cover foreign/schema3/schema5 unchanged bytes and current Standard2 reopen. Conversion of old relay/agent/pending-delivery records into a configuration-only Standard is not a lossless mapping; no conversion is performed. Original converter, probe, failure-stage and migration implementations remain recoverable in these parents.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/cf7879-message-cluster-datom-cli

Tip: `5df05aa78a8abdbe158a8ecc23623339d000316f`.

Contents: Verify producer ClusterMessage Datoms before adapter delivery.

Keep the current typed Message Query CLI and its producer decoding. Do not reinstall ClusterMessage transport, relay-forward commands or external prompt injection: the root cluster binaries were explicitly retired by f0cf9b50. Their producer and route vocabulary is not the current Send/Address/Request protocol. No old producer-pin downgrade.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/cf7879-message-cluster-send

Tip: `9d8cdbb29da542fa438292dabfb74c7226bb8b4a`.

Contents: Send verified ClusterMessage Datoms through Flow prompt-relay routes.

Keep the current typed Message Query CLI and its producer decoding. Do not reinstall ClusterMessage transport, relay-forward commands or external prompt injection: the root cluster binaries were explicitly retired by f0cf9b50. Their producer and route vocabulary is not the current Send/Address/Request protocol. No old producer-pin downgrade.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/cf7879-message-cluster-send-forward

Tip: `5e0e0e8a7d1ebc6d6e3185dcbc41758d6f4480f5`.

Contents: Add Codex acknowledgments to Flow ClusterMessage send bridge; Send verified ClusterMessage Datoms through Flow prompt-relay routes.

Keep the current typed Message Query CLI and its producer decoding. Do not reinstall ClusterMessage transport, relay-forward commands or external prompt injection: the root cluster binaries were explicitly retired by f0cf9b50. Their producer and route vocabulary is not the current Send/Address/Request protocol. No old producer-pin downgrade.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/cf7879-message-ordinary-claude-parser-preserved-2ba7a9d3

Tip: `2ba7a9d33505b18cf632c2ae44f833a3f328f675`.

Contents: Exclude serialized relay records from source selection.

Do not resurrect transcript relay parsing. The current service accepts typed Send over a kernel-identified socket and does not select prose from native transcripts. The result entry is a build-output artifact, not current source. Retain original code/history without exposing a second relay ingress.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/cf7879-message-ordinary-claude-parser-preserved-78ad6915

Tip: `78ad691594f03f8faa667c56f15d762672d411b2`.

Contents: Restrict cross-session relay envelope exclusion.

Do not resurrect transcript relay parsing. The current service accepts typed Send over a kernel-identified socket and does not select prose from native transcripts. The result entry is a build-output artifact, not current source. Retain original code/history without exposing a second relay ingress.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## proposal/cf7879-process-boundary-home-isolation-v2

Tip: `b8df67c8926d22c5292d066458262c77e24c05b7`.

Contents: Isolate Message daemon process tests from HOME.

Retain the current isolated child-process fixtures: tests/support/mod.rs already clears the environment and sets private HOME and XDG_RUNTIME_DIR. Do not restore the removed root daemon test or its obsolete package graph.

Originating instruction and historical reason for separation: unknown in the bounded records. Commit subjects do not establish either.

## Prevention proposed to Core

For authorized changes, integration into current main belongs to the same task as implementation. An intermediate check ref is not a completed handoff; report the resulting main or the exact semantic contradiction. This is a proposal for the requested book, not a landed skill change.
