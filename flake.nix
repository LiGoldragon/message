{
  description = "Message Nexus: durable messages and receipts, delivered through Flow.";

  inputs.base.url = "github:LiGoldragon/build-base";

  # Cargo.nix is generated from Cargo.lock by the build base's crate2nix and
  # committed, so evaluation imports no derivation:
  #   nix shell github:LiGoldragon/build-base#toolchain github:LiGoldragon/build-base#crate2nix -c crate2nix generate
  #
  # Each member's tests run in its own check (the base's default): the
  # contracts' CLI parsing, and the Message Nexus as a process against a
  # scripted Flow. The Nexus's build script proves its generated contract
  # (src/generated) equals ethos-zero's output from crates/message-nexus/ethos.
  outputs =
    { self, base }:
    let
      members = [
        "message-defaults"
        "message-nexus"
        "message"
        "message-meta"
        "message-mcp-send"
      ];
      outputs = base.lib.workspaceOutputs {
        cargoNix = ./Cargo.nix;

        extraChecks =
          { pkgs, toolchain, ... }:
          let
            lib = pkgs.lib;
            manifest = builtins.fromTOML (builtins.readFile ./Cargo.toml);
            # Every Rust source rustfmt reads, from each crate root through
            # its modules, the build script and the integration tests.
            rust = lib.fileset.toSource {
              root = ./.;
              fileset = lib.fileset.fileFilter (file: file.hasExt "rs") ./crates;
            };
            roots = [
              "crates/message-defaults/src/lib.rs"
              "crates/message-nexus/build.rs"
              "crates/message-nexus/src/lib.rs"
              "crates/message-nexus/src/main.rs"
              "crates/message-nexus/tests/*.rs"
              "crates/message/src/main.rs"
              "crates/message-meta/src/main.rs"
              "crates/message-mcp-send/src/main.rs"
            ];
            # The trait-law checks read every `.rs` file plus the shell they
            # are written in.
            lawSource = lib.cleanSourceWith {
              src = ./.;
              filter =
                path: type:
                (type == "directory")
                || (type == "regular" && lib.hasSuffix ".rs" path)
                || (type == "regular" && lib.hasSuffix ".sh" path);
            };
            sourceConstraintCheck =
              name: script:
              pkgs.runCommand name { } ''
                set -euo pipefail

                export PATH=${lib.makeBinPath [ pkgs.ripgrep ]}:$PATH
                ${pkgs.bash}/bin/bash ${script} ${./.}

                touch "$out"
              '';
          in
          {
            fmt = pkgs.runCommand "message-fmt" { nativeBuildInputs = [ toolchain ]; } ''
              cd ${rust}
              rustfmt --check --edition ${manifest.workspace.package.edition} ${lib.concatStringsSep " " roots}
              touch $out
            '';
            # `fn main()` is the only production free function, and every
            # production method lives in a trait.
            no-free-functions = pkgs.runCommand "message-no-free-functions" {
              src = lawSource;
            } (builtins.readFile ./checks/no-free-functions.sh);
            no-inherent-methods = pkgs.runCommand "message-no-inherent-methods" {
              src = lawSource;
            } (builtins.readFile ./checks/no-inherent-methods.sh);
            message-cannot-invoke-herdr = sourceConstraintCheck "message-cannot-invoke-herdr" ./scripts/message-cannot-invoke-herdr;
            message-runtime-cannot-reference-retired-terminal-brand = sourceConstraintCheck "message-runtime-cannot-reference-retired-terminal-brand" ./scripts/message-runtime-cannot-reference-retired-terminal-brand;
            message-component-cannot-own-local-ledger = sourceConstraintCheck "message-component-cannot-own-local-ledger" ./scripts/message-component-cannot-own-local-ledger;
          };
      };
    in
    outputs
    // {
      apps = builtins.mapAttrs (_: packages: {
        default = {
          type = "app";
          program = "${packages.message}/bin/message";
        };
      }) outputs.packages;
    };
}
