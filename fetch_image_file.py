#!/usr/bin/env python3
"""Copy one file out of a container image without docker, through the registry HTTP API.

Usage: fetch_image_file.py IMAGE[:TAG] PATH_IN_IMAGE OUTPUT
Example: fetch_image_file.py ghcr.io/ytsaurus/flow:0.1.0 /usr/bin/flow_server ./flow_server

Walks the image layers from the last to the first and takes the file from the first layer that
carries it; a whiteout in a later layer means the file was deleted. Only linux/amd64 images.
"""
import gzip
import io
import json
import os
import sys
import tarfile
import urllib.parse
import urllib.request

MANIFEST_TYPES = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def parse_image(image):
    first, slash, rest = image.partition("/")
    if slash and ("." in first or ":" in first or first == "localhost"):
        registry = first
    else:
        registry, rest = "registry-1.docker.io", image
        if "/" not in rest:
            rest = "library/" + rest
    repo, _, tag = rest.partition(":")
    return registry, repo, tag or "latest"


def get(url, headers, token=None):
    req = urllib.request.Request(url, headers=dict(headers, **({"Authorization": "Bearer " + token} if token else {})))
    return urllib.request.urlopen(req)


def anonymous_token(registry, repo):
    probe = f"https://{registry}/v2/"
    try:
        urllib.request.urlopen(probe)
        return None
    except urllib.error.HTTPError as error:
        challenge = error.headers.get("WWW-Authenticate", "")
    params = dict(part.split("=", 1) for part in challenge[len("Bearer "):].split(","))
    realm = params["realm"].strip('"')
    query = urllib.parse.urlencode({"service": params["service"].strip('"'), "scope": f"repository:{repo}:pull"})
    with urllib.request.urlopen(f"{realm}?{query}") as response:
        return json.load(response)["token"]


def main():
    image, path, output = sys.argv[1:4]
    registry, repo, tag = parse_image(image)
    token = anonymous_token(registry, repo)
    base = f"https://{registry}/v2/{repo}"

    with get(f"{base}/manifests/{tag}", {"Accept": MANIFEST_TYPES}, token) as response:
        manifest = json.load(response)
    if "manifests" in manifest:
        digest = next(m["digest"] for m in manifest["manifests"]
                      if m["platform"]["os"] == "linux" and m["platform"]["architecture"] == "amd64")
        with get(f"{base}/manifests/{digest}", {"Accept": MANIFEST_TYPES}, token) as response:
            manifest = json.load(response)

    member = path.lstrip("/")
    whiteout = os.path.join(os.path.dirname(member), ".wh." + os.path.basename(member))
    for layer in reversed(manifest["layers"]):
        with get(f"{base}/blobs/{layer['digest']}", {}, token) as response:
            blob = response.read()
        if layer["mediaType"].endswith("gzip"):
            blob = gzip.decompress(blob)
        with tarfile.open(fileobj=io.BytesIO(blob)) as archive:
            names = set(archive.getnames())
            if whiteout in names:
                break
            if member in names:
                with archive.extractfile(member) as source, open(output, "wb") as target:
                    target.write(source.read())
                os.chmod(output, 0o755)
                print(f"{image}:{path} -> {output} ({os.path.getsize(output)} bytes)")
                return
    sys.exit(f"{path} not found in {image}")


if __name__ == "__main__":
    main()
