{
  description = "Message surface, durable messenger, and ingress daemon.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";

    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system);

      mkContext =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.complete.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
            "rust-analyzer"
            "rust-src"
          ];
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          sourceFilter = path: type:
            type == "directory" || (craneLib.filterCargoSources path type);
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = sourceFilter;
            name = "source";
          };
          commonArgs = {
            inherit src;
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          sourceConstraintCheck =
            name: script:
            pkgs.runCommand name { } ''
              set -euo pipefail

              export PATH=${pkgs.lib.makeBinPath [ pkgs.ripgrep ]}:$PATH
              ${pkgs.bash}/bin/bash ${script} ${./.}

              touch "$out"
            '';
          cargoTestFile =
            testFile: testName: craneLib.cargoTest (
              commonArgs
              // {
                inherit cargoArtifacts;
                nativeBuildInputs = [ pkgs.ripgrep ];
                preCheck = ''
                  rg --fixed-strings ${pkgs.lib.escapeShellArg "fn ${testName}("} \
                    tests/${testFile}.rs
                '';
                cargoTestExtraArgs = "--test ${testFile} ${testName} -- --exact";
              }
            );
          context = {
            inherit
              pkgs
              toolchain
              craneLib
              commonArgs
              cargoArtifacts
              sourceConstraintCheck
              cargoTestFile
              ;
          };
        in
        context;
    in
    {
      packages = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          test-basic = context.pkgs.writeShellScriptBin "message-test-basic" ''
            export PATH=${context.pkgs.lib.makeBinPath [ context.toolchain context.pkgs.nix ]}:$PATH
            exec ${context.pkgs.bash}/bin/bash ${./scripts/test-basic} "$@"
          '';
          default = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              pname = "message";
              meta.mainProgram = "message";
            }
          );
        }
      );

      apps = forSystems (
        system:
        let
          packages = self.packages.${system};
        in
        {
          default = {
            type = "app";
            program = "${packages.default}/bin/message";
          };
          test-basic = {
            type = "app";
            program = "${packages.test-basic}/bin/message-test-basic";
          };
        }
      );

      checks = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
            }
          );
          clippy = context.craneLib.cargoClippy (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings";
            }
          );
          fmt = context.craneLib.cargoFmt { inherit (context.commonArgs) src; };
          doc = context.craneLib.cargoDoc (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              RUSTDOCFLAGS = "-D warnings";
            }
          );
          message-runtime-cannot-reference-retired-terminal-brand =
            context.sourceConstraintCheck "message-runtime-cannot-reference-retired-terminal-brand" ./scripts/message-runtime-cannot-reference-retired-terminal-brand;
          message-component-cannot-own-local-ledger =
            context.sourceConstraintCheck "message-component-cannot-own-local-ledger" ./scripts/message-component-cannot-own-local-ledger;
          message-daemon-reads-no-control-plane-environment-variables =
            context.sourceConstraintCheck "message-daemon-reads-no-control-plane-environment-variables" ./scripts/message-daemon-reads-no-control-plane-environment-variables;
          message-request-frame-is-a-bare-length-prefixed-archive =
            context.cargoTestFile "contract_convergence"
              "a_request_frame_is_a_length_prefix_over_a_bare_contract_archive";
          message-roots-are-distinct-on-the-wire =
            context.cargoTestFile "contract_convergence"
              "a_reply_archive_is_not_readable_as_a_request";
          message-daemon-executes-both-producer-contracts =
            context.cargoTestFile "process_boundary"
              "daemon_executes_both_producer_owned_contracts";
          message-daemon-isolates-flow-marker-home =
            context.cargoTestFile "process_boundary"
              "isolated_nexus_socket_parks_then_drains_on_a_typed_flow_idle_witness";
          message-pty-delivery-speaks-producer-datom =
            context.cargoTestFile "pty_end_to_end"
              "pty_leg_sends_the_producer_inbox_entry_as_datom";
          message-startup-request-round-trips-as-datom =
            context.cargoTestFile "startup_configuration"
              "the_startup_request_round_trips_through_its_own_datom_text";
          message-startup-request-writes-a-loadable-configuration =
            context.cargoTestFile "startup_configuration"
              "writing_the_startup_request_produces_a_configuration_the_daemon_loads";
          message-flow-delivery-parks-until-the-flow-is-idle =
            context.cargoTestFile "flow_delivery"
              "a_delivery_to_a_known_flow_parks_and_is_acknowledged_queued";
          message-flow-delivery-lands-on-idle-with-a-compact-receipt =
            context.cargoTestFile "flow_delivery"
              "an_idle_announce_lands_the_parked_delivery_with_a_compact_receipt";
          message-flow-delivery-repeated-idle-is-idempotent =
            context.cargoTestFile "flow_delivery"
              "repeated_idle_queries_are_acknowledged_without_relanding";
          message-relay-busy-delivery-is-durable-before-socket-write =
            context.cargoTestFile "relay_fixture"
              "busy_delivery_is_persisted_before_any_socket_write";
          message-relay-ordinary-claude-parser = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--bin relay";
            }
          );
          message-relay-ordinary-claude-process-fixture =
            context.cargoTestFile "relay_process"
              "ordinary_claude_turn_reaches_the_typed_relay_header_without_context_or_delivery";
          message-relay-codex-socket-fixture =
            context.cargoTestFile "relay_process"
              "fake_codex_socket_receives_the_exact_header_and_ordinary_claude_body";
          message-relay-flow-route-fanout-fixture =
            context.cargoTestFile "relay_process"
              "flow_route_fixture_fans_out_to_each_codex_target_excludes_source_and_keeps_unavailable_outcome";
          message-relay-codex-rollout-loop-exclusion = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--bin relay tests::consolidated_codex_rollout_relay_record_is_excluded_as_one_record -- --exact";
            }
          );
          message-relay-prompt-relay-provenance-loop-exclusion =
            context.cargoTestFile "relay_process"
              "prompt_relay_provenance_record_is_refused_without_socket_write_and_neighbor_is_selectable";
          message-relay-cross-session-envelope-exclusion = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--bin relay tests::cross_session_markup_requires_a_closed_envelope_at_the_start -- --exact";
            }
          );
          message-relay-ambiguous-and-mismatched-context-refuse =
            context.cargoTestFile "relay_process"
              "ambiguous_or_mismatched_context_source_is_refused_before_delivery";
          message-previous-store-schema-fails-closed =
            context.cargoTestFile "store_migration"
              "a_store_from_the_previous_schema_is_refused_rather_than_re_stamped";
        }
      );

      devShells = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.pkgs.mkShell {
            packages = [
              context.toolchain
              context.pkgs.jujutsu
              context.pkgs.nix
            ];
          };
        }
      );
    };
}
