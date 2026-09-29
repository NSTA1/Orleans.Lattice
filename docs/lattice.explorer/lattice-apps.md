# Lattice Apps in the Explorer

The Explorer treats [Lattice Apps](../lattice.apps/README.md) as first-class. Browsing
sources, reviewing consent and running an app's lifecycle are native Explorer pages.
An app that ships a user interface runs it inside a sandboxed, fully untrusted frame.
This page covers the Apps area, the security model of the frame, and how to write an
app UI.

## The Apps area

`/apps` opens on **Your apps**: the enabled installs in the active tenant where the
signed-in user holds at least one app role. A caller who holds `AppInstall` also sees
the **Catalogue** tab.

| Address | Page |
|---|---|
| `/apps` | Your apps, plus the Catalogue tab for `AppInstall` holders. |
| `/apps/catalogue?source={key\|all}&filter={all\|installed\|available\|updates}&q=` | The catalogue. The query string is the state, so every view can be linked. |
| `/apps/catalogue/{source}/{slug}` | Review before install, or manage an install: consent, lifecycle, upgrade and re-consent. |
| `/apps/{slug}/{tab}` | The app's own pages, built from its manifest: `overview`, `trees`, `roles`, `tools`, `subscriptions`, `replication`, and `consent` (`AppInstall` only). |
| `/apps/{slug}/open[/{path}]` | The app's UI, when it ships one and the caller holds a role. |

With tenancy on, each address is rooted at `/t/{tenant}`.

### Sources

The catalogue lists what every configured [app source](../lattice.apps/README.md#app-sources)
offers. The **source selector** has one entry per source plus "All sources". Each
entry shows the source's kind (`Static` or `Dynamic`) and what it can do. Text search
is enabled only for a source that advertises `Search`. A slug offered by two sources
appears as one row per source, and an install always records the source it came from.

The in-image source (`in-image`) lists the apps the silo registered at start-up. A
dynamic source, such as a NuGet feed, a blob container or a container registry, plugs
in behind the same seam. The install flow already models its `Acquiring` and
`Verifying` stages, so it needs no UI change.

### Consent and lifecycle

Nothing runs until the operator approves it. The review page draws exactly what the
app asks for, against its own `a/{slug}/` namespace, and marks anything outside that
namespace as an exception:

- the trees it creates;
- each role's operations and scopes;
- the capability ceiling;
- change-feed subscriptions, including other apps' trees;
- MCP tools;
- the bridge operations its UI requests.

Installing binds each role to a membership group and confirms the ceiling. Enabling,
disabling, upgrading and uninstalling are native actions with explicit confirmation.
An upgrade shows what changed between the two versions. An install whose ceiling or
bridge grants no longer cover its manifest is shown as needing re-consent.

App-supplied text (names, descriptions, categories) is shown as text only, and icons
only through `<img>`. An app can never inject markup into the Explorer.

## The app frame

An app's UI never runs in the Explorer's own page.

- **Sandbox.** It renders in `<iframe sandbox="allow-scripts">` with no other sandbox
  token. The frame has an opaque origin, so it cannot read the Explorer's DOM, cookies,
  storage or connection.
- **Bootstrap.** Every frame loads the same static bootstrap document from the AppKit
  package, at `{base}/_apps/frame/v1/frame.html`. That document is served with
  `sandbox allow-scripts; default-src 'none'; connect-src 'none'` and related CSP
  directives, so the frame has **no network egress**. It is the only Explorer route
  exempt from `X-Frame-Options: DENY`.
- **Bundle delivery.** The bundle is never served to the frame over HTTP. The
  Explorer fetches it on the signed-in user's credential through
  `ILatticeAppWorkspace` and verifies every SHA-256 and the bundle digest. It then
  hands the bytes to the frame over a single `MessageChannel` port. The frame checks
  the digests again and runs the bundle from `blob:` URLs.
- **Bridge.** The port is the only channel. Every data request goes to the cluster's
  [`ILatticeAppBridge`](../lattice.api.apps/README.md#the-bridge). The bridge allows
  only what the app's own roles grant this user, inside the consented bridge
  operations and the app's own trees. The Explorer broker only validates, rate-limits
  and relays.
- **Credentials.** No credential ever enters the frame. A second load of the frame
  closes its port. An upgrade, disable or uninstall revokes the session.

**Residual risk.** A sandbox cannot guarantee zero egress. For example, WebRTC is not
blocked where the browser does not support the `webrtc` directive. Data handed to a
frame can therefore, in principle, leave it. This is why a frame only ever receives
data within its app's consented scope, and why the user's display name is a
separately consented operation (`context.user`).

## Writing an app UI

An app UI is a bundle declared in the manifest's
[`ui` section](../lattice.apps/README.md#presentation-and-ui) and embedded in the app's
assembly. The Explorer provides everything that runs inside the frame. Your bundle is
a plain HTML fragment, stylesheets and self-contained scripts. The bootstrap defines
`globalThis.lattice` before your first script runs:

| Member | Purpose |
|---|---|
| `lattice.ready` | A promise that resolves once the bridge is connected. |
| `lattice.request(op, args)` | Sends one bridge request, and resolves with its result or rejects with a `LatticeError` whose `code` is one of `denied`, `not_found`, `invalid`, `too_large`, `rate_limited`, `unavailable`, `conflict`. |
| `lattice.on(event, handler)` | Subscribes to `context.changed`, `nav.changed` or `lattice.revoked`. |
| `lattice.assetUrl(path)` | The `blob:` URL of one of your bundle's assets. |

The operations match the manifest's bridge vocabulary:

- `context.read` returns the app, theme, contrast, density, reduced motion, tenant
  display name, and the caller's app `roles`.
- `context.user` returns the display name only.
- `data.read` takes `action` `get` or `scan`, `data.write` takes `set`, and
  `data.delete` takes `delete`. Each names a **logical** tree from your manifest.
  Values are base64, each at most 64 KiB, with scan pages of at most 200 entries.
- `nav.sync` reports your internal path, which the Explorer mirrors into its address.
- `ui.notify` shows a text toast.

For example:

```javascript
await lattice.ready;
const context = await lattice.request("context.read");
const canEdit = Array.isArray(context.roles) && context.roles.includes("editor");
const page = await lattice.request("data.read", { action: "scan", tree: "tasks", prefix: "tasks/", pageSize: 100 });
```

Guidance:

- **Role-driven controls.** Decide which controls to show from the `roles` that
  `context.read` returns. Treat any `denied` as the cluster's final word. Never probe
  permissions with a write: every request is enforced by the cluster, so a wrong guess
  can only hide a control, never grant one.
- **No forms.** The sandbox has no `allow-forms`, so a `<form>` never submits. Use
  buttons and key handlers instead.
- **Styling.** Link `lattice-app.css` through the kit. It gives your UI the Explorer's
  Paper and Board materials, and it follows `context.changed`. Keep layouts fluid:
  the frame fills the content area at every width, down to a phone.
- **Digests.** Pin every asset's digest and the bundle digest in the manifest. The
  [task-board sample](../../samples/Explorer/Apps/TaskBoard/README.md) computes them
  in a test that fails with the correct values whenever a file changes.

## See also

- [Installable apps](../lattice.apps/README.md)
- [App control, catalogue, workspace and bridge facades](../lattice.api.apps/README.md)
- [Task-board sample app](../../samples/Explorer/Apps/TaskBoard/README.md)
- [Explorer](README.md)
