{
  description = "microCMS Content Ops Analytics development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "x86_64-linux"
        "aarch64-linux"
      ];

      forEachSystem =
        f: system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs { inherit system overlays; };
          toolchain = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
            extensions = [
              "rustfmt"
              "clippy"
            ];
          };
        in
        f pkgs toolchain;

      mkDevShell =
        pkgs: toolchain:
        pkgs.mkShell {
          packages = with pkgs; [
            toolchain
            just
            opentofu
            awscli2
            jq
            curl
            openssl
            xxd
            gitleaks
            docker
            docker-compose
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [ libiconv ];

          shellHook = pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
            export LIBRARY_PATH="${pkgs.libiconv}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
          '';
        };
    in
    {
      devShells = nixpkgs.lib.genAttrs systems (
        system:
        forEachSystem (pkgs: toolchain: {
          default = mkDevShell pkgs toolchain;
        }) system
      );
    };
}
