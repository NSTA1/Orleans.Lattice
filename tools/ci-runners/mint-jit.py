"""Mint a single-use (JIT) GitHub Actions runner config.

Runs as the init container of every runner Job execution. It is the only
container given the GitHub App private key: it exchanges the key for a
short-lived installation token, asks GitHub for a just-in-time runner config
bound to this repository and label set, and writes only that config to
/jit/config for the runner container. The config registers one runner for one
job; GitHub deletes the runner when the job ends.

Environment:
  GITHUB_APP_ID, GITHUB_APP_INSTALLATION_ID, GITHUB_APP_KEY (PEM),
  GITHUB_REPOSITORY (owner/name), RUNNER_LABELS (comma-separated),
  JIT_CONFIG_PATH (default /jit/config), RUNNER_NAME_PREFIX (default aca).
"""
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.request

import jwt

API = "https://api.github.com"


def call(method, path, token, scheme, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"{scheme} {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "lattice-ci-runners",
            "Content-Type": "application/json",
        },
    )
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == 4:
                raise SystemExit(f"mint-jit: {method} {path} failed: {e.code} {e.read().decode('utf-8', 'replace')[:300]}")
        except urllib.error.URLError as e:
            if attempt == 4:
                raise SystemExit(f"mint-jit: {method} {path} failed: {e}")
        time.sleep(2 ** attempt)


def main():
    app_id = os.environ["GITHUB_APP_ID"]
    installation = os.environ["GITHUB_APP_INSTALLATION_ID"]
    key = os.environ["GITHUB_APP_KEY"]
    repo = os.environ["GITHUB_REPOSITORY"]
    # A JIT config's labels are the runner's COMPLETE set: GitHub adds no
    # defaults, so a job asking for [self-hosted, <label>] would never match a
    # runner registered with <label> alone.
    labels = ["self-hosted", "linux", "x64"]
    labels += [l.strip() for l in os.environ["RUNNER_LABELS"].split(",") if l.strip() and l.strip() not in labels]
    out = os.environ.get("JIT_CONFIG_PATH", "/jit/config")
    name = f"{os.environ.get('RUNNER_NAME_PREFIX', 'aca')}-{secrets.token_hex(4)}"

    now = int(time.time())
    app_jwt = jwt.encode({"iat": now - 60, "exp": now + 540, "iss": app_id}, key, algorithm="RS256")
    token = call("POST", f"/app/installations/{installation}/access_tokens", app_jwt, "Bearer")["token"]
    jit = call("POST", f"/repos/{repo}/actions/runners/generate-jitconfig", token, "token", {
        "name": name,
        "runner_group_id": 1,
        "labels": labels,
        "work_folder": "_work",
    })

    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="ascii") as f:
        f.write(jit["encoded_jit_config"])
    os.chmod(out, 0o644)
    print(f"mint-jit: runner {name} (id {jit['runner']['id']}) labels={','.join(labels)}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
