{
  description = "Message NOTA CLI and ingress daemon.";

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
          toolchain = fenix.packages.${system}.fromToolchainFile {
            file = ./rust-toolchain.toml;
            sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
          };
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          src = craneLib.cleanCargoSource ./.;
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
                cargoTestExtraArgs = "--test ${testFile} ${testName} -- --exact";
              }
            );
          cargoTest = cargoTestFile "message";
          context = {
            inherit
              pkgs
              toolchain
              craneLib
              commonArgs
              cargoArtifacts
              sourceConstraintCheck
              cargoTestFile
              cargoTest
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
          message-runtime-cannot-reference-retired-terminal-brand =
            context.sourceConstraintCheck "message-runtime-cannot-reference-retired-terminal-brand" ./scripts/message-runtime-cannot-reference-retired-terminal-brand;
          message-component-cannot-own-local-ledger =
            context.sourceConstraintCheck "message-component-cannot-own-local-ledger" ./scripts/message-component-cannot-own-local-ledger;
          message-daemon-reads-no-control-plane-environment-variables =
            context.sourceConstraintCheck "message-daemon-reads-no-control-plane-environment-variables" ./scripts/message-daemon-reads-no-control-plane-environment-variables;
          message-component-uses-stable-kameo-lifecycle-reference =
            context.cargoTestFile "actor_runtime_truth" "message_component_uses_stable_kameo_lifecycle_reference";
          message-daemon-uses-shared-triad-multi-listener-runtime =
            context.cargoTestFile "actor_runtime_truth" "message_daemon_uses_shared_triad_multi_listener_runtime";
          message-cli-sends-router-signal-without-local-ledger =
            context.cargoTest "command_line_send_routes_signal_frame_without_writing_local_ledger";
          message-cli-inbox-uses-router-signal-not-local-ledger =
            context.cargoTest "command_line_inbox_routes_signal_frame_without_reading_local_ledger";
          message-cli-requires-message-socket =
            context.cargoTest "command_line_send_requires_message_socket";
          message-daemon-applies-configured-socket-mode =
            context.cargoTest "message_daemon_applies_configured_socket_mode";
          message-daemon-answers-component-supervision-relation =
            context.cargoTest "message_daemon_answers_component_supervision_relation";
          message-frame-codec-rejects-mismatched-signal-verb =
            context.cargoTest "message_frame_codec_rejects_mismatched_signal_verb";
          message-daemon-root-stamps-owner-identity-from-configuration =
            context.cargoTest "message_daemon_root_stamps_owner_identity_from_configuration";
          message-daemon-root-shutdown-returns-terminal-outcome =
            context.cargoTest "message_daemon_root_shutdown_returns_terminal_outcome";
          message-daemon-graceful-stop-releases-message-socket-and-rejects-ingress =
            context.cargoTest "message_daemon_graceful_stop_releases_message_socket_and_rejects_ingress";
          message-daemon-forwards-cli-signal-frame-to-router-socket =
            context.cargoTest "message_daemon_forwards_cli_signal_frame_to_router_socket";
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
