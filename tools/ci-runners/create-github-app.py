"""Create the GitHub App the CI runner pool registers runners with.

GitHub only creates an App after its owner confirms it in a browser, so this
uses the App manifest flow to do everything else:

  1. serves an auto-submitting manifest form on http://localhost:53682/ and
     opens it; you click "Create GitHub App" on github.com;
  2. catches GitHub's redirect, exchanges the one-time code for the App's
     credentials (POST /app-manifests/{code}/conversions needs no auth) and
     writes app.json and app-private-key.pem to --out (never the repository);
  3. prints the install link; you install the App on the repository only; the
     script then finds the installation and records installation_id.

Permissions requested: Administration (read and write, to mint single-use
runner registrations), Actions (read, for the scaler's queue polling) and
Metadata (read). No webhook, not public.

Usage: python3 tools/ci-runners/create-github-app.py [--repo owner/name] [--name app-name]
"""
import argparse
import html
import json
import os
import secrets
import sys
import time
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

PORT = 53682


def api(method, path, token, scheme="Bearer"):
    req = urllib.request.Request("https://api.github.com" + path, method=method, headers={
        "Authorization": f"{scheme} {token}", "Accept": "application/vnd.github+json",
        "User-Agent": "lattice-ci-runners"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="NSTA1/Orleans.Lattice")
    ap.add_argument("--name", default="lattice-ci-runners-nsta1")
    ap.add_argument("--out", default=os.path.join(os.path.expanduser("~"), ".lattice-ci-runners"))
    args = ap.parse_args()

    state = secrets.token_urlsafe(16)
    manifest = {
        "name": args.name,
        "url": f"https://github.com/{args.repo}",
        "description": f"Registers single-use self-hosted Actions runners for {args.repo} trusted workflows.",
        "redirect_url": f"http://localhost:{PORT}/callback",
        "hook_attributes": {"url": "https://example.invalid/unused", "active": False},
        "public": False,
        "default_permissions": {"administration": "write", "actions": "read", "metadata": "read"},
        "default_events": [],
    }
    created = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body):
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def do_GET(self):
            if self.path == "/":
                return self._send(200, (
                    "<html><body onload='document.forms[0].submit()'>"
                    f"<form method='post' action='https://github.com/settings/apps/new?state={state}'>"
                    f"<input type='hidden' name='manifest' value='{html.escape(json.dumps(manifest), quote=True)}'>"
                    "<input type='submit' value='Continue to GitHub'></form></body></html>"))
            if self.path.startswith("/callback"):
                q = parse_qs(urlparse(self.path).query)
                if q.get("state", [""])[0] != state or "code" not in q:
                    return self._send(400, "<p>State mismatch or missing code.</p>")
                req = urllib.request.Request(f"https://api.github.com/app-manifests/{q['code'][0]}/conversions",
                                             method="POST", headers={"Accept": "application/vnd.github+json",
                                                                     "User-Agent": "lattice-ci-runners"})
                with urllib.request.urlopen(req, timeout=30) as r:
                    created.update(json.load(r))
                link = f"https://github.com/apps/{created['slug']}/installations/new"
                return self._send(200, f"<p>Created. Install it on {html.escape(args.repo)} only: <a href='{link}'>{link}</a></p>")
            self._send(404, "not found")

    srv = HTTPServer(("127.0.0.1", PORT), Handler)
    srv.timeout = 1
    webbrowser.open(f"http://localhost:{PORT}/")
    print(f"Confirm the App in your browser (http://localhost:{PORT}/).", flush=True)
    deadline = time.time() + 900
    while not created and time.time() < deadline:
        srv.handle_request()
    if not created:
        sys.exit("timed out waiting for GitHub")

    os.makedirs(args.out, exist_ok=True)
    key_path = os.path.join(args.out, "app-private-key.pem")
    with open(key_path, "w", encoding="ascii") as f:
        f.write(created["pem"])
    os.chmod(key_path, 0o600)
    meta = {k: created.get(k) for k in ("id", "slug", "name", "html_url", "client_id")}
    print(f"App {meta['slug']} (id {meta['id']}) created. Install it on {args.repo} only:")
    print(f"  https://github.com/apps/{meta['slug']}/installations/new", flush=True)

    import jwt  # pyjwt[crypto]
    while time.time() < deadline + 900:
        now = int(time.time())
        token = jwt.encode({"iat": now - 60, "exp": now + 540, "iss": str(meta["id"])}, created["pem"], algorithm="RS256")
        installs = api("GET", "/app/installations", token)
        if installs:
            meta["installation_id"] = installs[0]["id"]
            break
        time.sleep(10)
    with open(os.path.join(args.out, "app.json"), "w", encoding="ascii") as f:
        json.dump(meta, f, indent=2)
    print(f"Wrote {args.out} (installation_id={meta.get('installation_id')}).")


if __name__ == "__main__":
    main()
