# cf7879 relay loop guard receipt

Source parent: `c60a8f2556dedc8e4124e49a2b87cfba464f096c`.

Follow-up source parent: `f9078b2310c4fcdf2176592fecbd69d90993fa21`.

The relay source selector now excludes a genuine leading prompt-relay JSON
provenance envelope in either a plain string or an `input_text`/`text` part.
The predicate requires all five prompt-relay provenance fields, a nonempty
body after the blank-line delimiter, and a 64-character hexadecimal UTF-8
hash. It also recognizes Relay, Peer, Wake, and System control wrappers.

The follow-up accepts the producer's `source_timestamp: null` form and its
separate Codex `text` header/body parts. The native Claude preamble is now a
marker only when it is immediately followed by a closed
`<cross-session-message>` envelope; ordinary and incomplete discussion stay
selectable.

The completed follow-up reran the same focused remote Nix derivation with
exit status 0 on `ssh-ng://nix-ssh@prometheus.goldragon.criome`; captured
errors: none.

## Producer convergence

The former dual `signal-message` graph came from this direct consumer pin and
meta-signal-message `f98fd105`, which pinned `81f659e`. Published producer
proposal `87a54b0a1cbc9aa02f9ff62e46c6ffca52f9ec25` pins the same
Relay-compatible `a9708f3384af18129cb1c983ffed850c4d631e46` contract as
Message. Its local Datom contract test passed 4/4 and its focused Prometheus
Nix `test-contract-datom` gate exited 0.

Message now pins that producer revision and the full `a9708…` revision
directly. `cargo tree -p message --prefix none` reports one signal-message
source; the second occurrence is Cargo's `(*)` shared dependency marker.
The local relay suite passed 12/12 and the focused Prometheus
`message-relay-prompt-relay-provenance-loop-exclusion` gate exited 0.

Captured local tests:

- `cargo test --bin relay`: 12 passed, 0 failed.
- `cargo test --test relay_process prompt_relay_provenance_record_is_refused_without_socket_write_and_neighbor_is_selectable -- --exact`: 1 passed, 0 failed.

The process fixture uses the sanitized public shape of user record
`611f76ba-f42f-45ba-aefa-4f27369071fc`. It proves that selecting the relayed
source fails before a bound Unix socket receives a connection, and that the
adjacent ordinary user turn is still selectable.

Captured remote Nix result:

`nix build --max-jobs 0 --no-link --option substituters https://cache.nixos.org/ --option connect-timeout 5 '.#checks.x86_64-linux.message-relay-prompt-relay-provenance-loop-exclusion'`

Exit status: 0. The derivation was built on
`ssh-ng://nix-ssh@prometheus.goldragon.criome`; captured errors: none.
