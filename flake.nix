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
          message-runtime-cannot-reference-retired-terminal-brand =
            context.sourceConstraintCheck "message-runtime-cannot-reference-retired-terminal-brand" ./scripts/message-runtime-cannot-reference-retired-terminal-brand;
          message-component-cannot-own-local-ledger =
            context.sourceConstraintCheck "message-component-cannot-own-local-ledger" ./scripts/message-component-cannot-own-local-ledger;
          message-daemon-reads-no-control-plane-environment-variables =
            context.sourceConstraintCheck "message-daemon-reads-no-control-plane-environment-variables" ./scripts/message-daemon-reads-no-control-plane-environment-variables;
          # The emitted daemon spine over a real socket: argv config load ->
          # single working-socket bind -> signal-frame decode -> Nexus decide ->
          # encode -> wire reply. The already-stamped submission replies
          # Unimplemented straight from the Nexus decision, no router needed.
          message-emitted-daemon-replies-unimplemented-for-already-stamped-submission =
            context.cargoTestFile "process_boundary"
              "daemon_replies_unimplemented_for_already_stamped_submission_over_real_socket";
          # The Nexus forward-to-router effect against a stub router: a submit is
          # stamped from the configured owner and forwarded, and the router
          # acceptance is translated back into the Signal output.
          message-daemon-stamps-and-forwards-submission-to-router =
            context.cargoTestFile "forward_to_router"
              "submit_is_stamped_and_forwarded_then_router_acceptance_is_translated_back";
          # The router-unreachable fallback yields a typed Error output rather
          # than a hard failure.
          message-router-unreachable-yields-typed-error =
            context.cargoTestFile "forward_to_router"
              "router_unreachable_yields_typed_error_output";
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
