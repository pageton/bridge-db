{
  description = "bridge — database migration CLI (Redis, MongoDB, PostgreSQL, MySQL, MariaDB, CockroachDB, MSSQL, SQLite)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        version = "0.2.0-rc.1";
      in
      {
        packages.default = pkgs.buildGoModule {
          pname = "bridge";
          inherit version;
          src = ./.;
          vendorHash = "sha256-c0nMn6vrZQzvAt/W0MZOCErWCERVTsw6QfjkNhDG44A=";
          ldflags = [
            "-s"
            "-w"
            "-X main.version=${version}"
          ];
          meta = with pkgs.lib; {
            description = "Database migration CLI for Redis, MongoDB, PostgreSQL, MySQL, MariaDB, CockroachDB, MSSQL, and SQLite";
            license = licenses.mit;
            mainProgram = "bridge";
          };
        };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            go
            gopls
            gotools
          ];
        };

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/bridge";
        };
      }
    );
}
