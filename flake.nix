{
  description = "Message Nexus: durable messages and receipts, delivered through Flow.";

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
            type == "directory"
            || (craneLib.filterCargoSources path type)
            || nixpkgs.lib.hasInfix "/tests/fixtures/" (toString path);
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = sourceFilter;
            name = "source";
          };
          commonArgs = {
            inherit src;
            pname = "message-workspace";
            version = "0.16.0";
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
          context = {
            inherit
              pkgs
              toolchain
              craneLib
              commonArgs
              cargoArtifacts
              sourceConstraintCheck
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
        }
      );

      checks = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          # The workspace tests: the contracts' CLI parsing, and the Message
          # Nexus as a process against a scripted Flow.
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
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
            }
          );
          fmt = context.craneLib.cargoFmt { inherit (context.commonArgs) src; };
          message-cannot-invoke-herdr =
            context.sourceConstraintCheck "message-cannot-invoke-herdr" ./scripts/message-cannot-invoke-herdr;
          message-runtime-cannot-reference-retired-terminal-brand =
            context.sourceConstraintCheck "message-runtime-cannot-reference-retired-terminal-brand" ./scripts/message-runtime-cannot-reference-retired-terminal-brand;
          message-component-cannot-own-local-ledger =
            context.sourceConstraintCheck "message-component-cannot-own-local-ledger" ./scripts/message-component-cannot-own-local-ledger;
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
