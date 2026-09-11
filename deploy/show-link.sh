#!/usr/bin/env bash
set -euo pipefail

config=/etc/tproxy-server/config.json
profiles=/etc/tproxy-server/profiles.json

if [[ "${EUID}" -ne 0 ]]; then
	echo "run this as root: it reads the secret out of $profiles" >&2
	exit 1
fi
for file in "$config" "$profiles"; do
	if [[ ! -r "$file" ]]; then
		echo "cannot read $file; is this host installed?" >&2
		exit 1
	fi
done

json_string() {
	sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$1" | head -n1
}

hostname="$(json_string "$config" public_hostname)"
base_path="$(json_string "$config" base_path)"
secret="$(json_string "$profiles" secret)"
if [[ -z "$hostname" ]] || [[ -z "$secret" ]]; then
	echo "could not read the hostname or the secret from the installed configuration" >&2
	exit 1
fi

# The encoding deploy/install.sh applies, kept in step with it. Under a base path
# the client secret is base64url of the marker byte 0x70 followed by the raw
# secret, so a client without base path support reports an unsupported proxy type
# instead of accepting a pathless proxy on an empty host.
if [[ -z "$base_path" ]]; then
	server="$hostname"
	proxy_secret="$secret"
else
	server="$hostname/$base_path"
	proxy_secret="$({
		printf '\x70'
		printf "$(printf %s "$secret" | sed 's/../\\x&/g')"
	} | base64 | tr '+/' '-_' | tr -d '=\n')"
fi

echo "Proxy server: $server"
echo "Proxy secret: $proxy_secret"
echo "Proxy link:   tg://webproxy?server=$server&secret=$proxy_secret"
