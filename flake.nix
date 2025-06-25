{
  description = "Python application to run main.py";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python3.withPackages (ps: with ps; [
          requests
          boto3
          h5py
          numpy
          pandas
          plotly
          kaleido
          packaging
        ]);
      in {
        apps.default = flake-utils.lib.mkApp {
          drv = pkgs.writeShellApplication {
            name = "show-temps";
            runtimeInputs = [ python ];
            text = ''
              ${python.interpreter} ${./show_temps.py}
            '';
          };
        };

        devShells.default = pkgs.mkShell {
          packages = [ python ];
        };
      });
}
