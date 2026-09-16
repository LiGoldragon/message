# cf7879 relay loop guard receipt

Source parent: `c60a8f2556dedc8e4124e49a2b87cfba464f096c`.

The relay source selector now excludes a genuine leading prompt-relay JSON
provenance envelope in either a plain string or an `input_text`/`text` part.
The predicate requires all five prompt-relay provenance fields, a nonempty
body after the blank-line delimiter, and a 64-character hexadecimal UTF-8
hash. It also recognizes Relay, Peer, Wake, and System control wrappers.

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
