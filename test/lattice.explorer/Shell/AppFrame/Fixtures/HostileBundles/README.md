# Hostile app bundles (issue #3817)

Browser fixtures for the app frame's isolation, authored with the frame host (X1) and
wired into CI by the Playwright suite (U1, issue #3832). Each folder is one app UI bundle
whose app is actively hostile. None of these run in the .NET unit suite; the unit suite
only checks that every fixture is well formed (`HostileBundleFixtureTests`).

## Harness contract

For each folder, the U1 harness:

1. Reads `fixture.json` and builds an installed app named by `displayName`, declaring
   `trees`, with a UI whose `entry`, `styles`, `scripts` and bridge grants (`bridge`, one
   `{ operation, tree? }` per grant, a missing tree meaning every declared tree) come from
   the fixture. Every file in the folder except `fixture.json` is an asset; each asset's
   SHA-256 and the bundle digest are computed from the files as they are on disk.
2. Serves the assets through the workspace fake. When `serve` maps a path to another
   file, the workspace serves that file's bytes under the pinned path and digest, which is
   how a compromised source is simulated.
3. Opens the app's Open tab and waits for either the frame host's failure state or the
   expected toast.

## Expectations

`expect.failure` is the `AppFrameFailure` the host must show (`null` when the frame must
keep running). `expect.toast` is the exact text the host's toast region must show (the
host prefixes the app's display name). Further keys are assertions the harness makes:

| Fixture | Attack | Expected outcome |
|---|---|---|
| `self-reload` | Reloads its own document after the handshake | `Reloaded`: the port is closed and the frame removed |
| `navigate-top` | Navigates the Explorer page | Explorer URL unchanged; toast `Hostile: top-navigation-blocked` |
| `forge-ready` | Posts extra `lattice.ready` messages | No second `lattice.hello` is sent; toast `Hostile: forged-ready-sent` |
| `flood` | Sends 100 requests at once | At least 76 answered `rate_limited`; the frame keeps working |
| `physical-tree` | Reads a physical tree id and an undeclared tree | Both `denied`; nothing reaches the cluster fake |
| `unknown-operation` | Calls `app.uninstall` and unconsented `context.user` | Both `denied` |
| `exfiltrate` | fetch, XHR, WebSocket, parent DOM, popup, cookie | Every channel `blocked` |
| `entry-script` | Entry fragment embeds `<script>` | `BundleInvalid` before any byte reaches the frame |
| `digest-tamper` | Source serves bytes that do not match the pinned digest | `DigestMismatch` before any byte reaches the frame |
