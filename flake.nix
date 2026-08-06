{
  description = "Dotos Message surface, durable messenger, and ingress daemon.";

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
          cargoTestFileWithFeatures =
            testFile: testName: features: craneLib.cargoTest (
              commonArgs
              // {
                inherit cargoArtifacts;
                nativeBuildInputs = [ pkgs.ripgrep ];
                preCheck = ''
                  rg --fixed-strings ${pkgs.lib.escapeShellArg "fn ${testName}("} \
                    tests/${testFile}.rs
                '';
                cargoTestExtraArgs = "--features ${features} --test ${testFile} ${testName} -- --exact";
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
              cargoTestFileWithFeatures
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
              cargoExtraArgs = "--features dotos-text";
              meta.mainProgram = "message";
            }
          );
          text = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoExtraArgs = "--features dotos-text";
              pname = "message-text";
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
          binary-only = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--all-targets --no-default-features";
            }
          );
          message-runtime-cannot-reference-retired-terminal-brand =
            context.sourceConstraintCheck "message-runtime-cannot-reference-retired-terminal-brand" ./scripts/message-runtime-cannot-reference-retired-terminal-brand;
          message-component-cannot-own-local-ledger =
            context.sourceConstraintCheck "message-component-cannot-own-local-ledger" ./scripts/message-component-cannot-own-local-ledger;
          message-daemon-reads-no-control-plane-environment-variables =
            context.sourceConstraintCheck "message-daemon-reads-no-control-plane-environment-variables" ./scripts/message-daemon-reads-no-control-plane-environment-variables;
          message-consumes-producer-contract-directly =
            context.cargoTestFile "contract_convergence"
              "component_executes_the_producer_contract_by_identity";
          message-has-no-structural-ownership-inputs =
            context.cargoTestFile "contract_convergence"
              "component_has_no_structural_ownership_inputs";
          message-daemon-executes-both-producer-contracts =
            context.cargoTestFileWithFeatures "process_boundary"
              "daemon_executes_both_producer_owned_contracts"
              "dotos-text";
          message-pty-delivery-speaks-producer-dotos =
            context.cargoTestFileWithFeatures "pty_end_to_end"
              "pty_leg_sends_the_producer_inbox_entry_in_dotos"
              "dotos-text";
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
