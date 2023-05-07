(import ./lib.nix)
  ({ pkgs, ... }:
  let
    inherit (pkgs) hello;
    copyScript = pkgs.writeShellScriptBin "copy-test" ''
      set -e
      PUBKEY=$(cat ${./cache.pk})
      nix copy \
        --option trusted-public-keys "$PUBKEY" \
        --from http://harmonia:5000 \
        --to /root/test-store \
        "$@"
    '';
  in
  {
    name = "t01-signing";

    nodes = {
      harmonia = { ... }:
        {
          imports = [ ../module.nix ];

          services.harmonia = {
            enable = true;
            signKeyPath = "${./cache.sk}";
          };

          networking.firewall.allowedTCPPorts = [ 5000 ];
          environment.systemPackages = [ hello ];
        };

      client01 = { lib, ... }:
        {
          environment.systemPackages = [ copyScript ];
          nix.settings.substituters = lib.mkForce [ "http://harmonia:5000" ];
          nix.extraOptions = ''
            experimental-features = nix-command
          '';
        };
    };

    testScript = ''
      start_all()

      client01.wait_until_succeeds("curl -f http://harmonia:5000/version")
      client01.succeed("curl -f http://harmonia:5000/nix-cache-info")

      client01.wait_until_succeeds("${copyScript}/bin/copy-test ${hello}")
      client01.succeed("${hello}/bin/hello --version")
    '';
  })
