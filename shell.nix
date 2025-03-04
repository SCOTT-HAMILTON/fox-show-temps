{ pkgs ? import <nixpkgs> {} }:
let
  customPython = pkgs.python3.buildEnv.override {
    extraLibs = with pkgs.python3Packages; [
      requests
      boto3
      h5py
      numpy
      pandas
      plotly
      packaging
    ];
  };
in
with pkgs; mkShell {
  buildInputs = [
    customPython
  ];
  shellHook = ''
    show_temps(){
      ${customPython}/bin/python3 show_temps.py
    }
  '';
}

