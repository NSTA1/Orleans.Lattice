# Lattice Apps in the Explorer

The Explorer treats [Lattice Apps](../lattice.apps/README.md) as first-class. Browsing
sources, reviewing consent and running an app's lifecycle are native Explorer pages.
An app that ships a user interface runs it inside a sandboxed, fully untrusted frame.
This page covers the Apps area, the security model of the frame, and how to write an
app UI.

## The Apps area

`/apps` opens on **Your apps**: the enabled installs in the active tenant where the
signed-in user holds at least one app role. A caller who holds `AppInstall` also sees
the **Catalogue** view, an **Install app...** control (palette command `apps.install`),
and a call-out for any installed app whose activation failed, with a link to review
and re-consent it. Such a caller also sees **Installed in tenant {tenant}** (or
**Installed apps** without a tenant root): every install in the tenant with its
version and state, and a **Manage** link plus **Disable** and **Uninstall** actions
where the caller may take them. When apps are installed but the caller holds no role
in any of them, Your apps reads "No role in an app" rather than "No apps yet".

For an `AppInstall` holder, the directory badge counts the tenant's installs and Home
reads, for example, "3 apps installed, 1 failed activation, 2 updates available". For
anyone else, the badge counts the caller's own apps and Home reads "N apps available
to you" or "No app is assigned to you yet".

| Address | Page |
|---|---|
| `/apps` | Your apps, plus the Catalogue tab for `AppInstall` holders. |
| `/apps/catalogue?source={key\|all}&filter={all\|installed\|available\|updates}&q=` | The catalogue. The query string is the state, so every view can be linked. |
| `/apps/catalogue/{source}/{slug}[@{version}]` | Review before install, or manage an install: consent, lifecycle, upgrade and re-consent. Without a version, the source's newest is reviewed. |
| `/apps/{slug}/{tab}` | The app's own pages, built from its manifest: `overview`, `trees`, `roles`, `tools`, `subscriptions`, `replication`, and `consent` (`AppInstall` only). The bare `/apps/{slug}` shows the overview in place. |
| `/apps/{slug}/open[/{path}]` | The app's UI, when it ships one and the caller holds a role. Up to four in-app path segments follow `open`; a deeper in-app path travels as `?path=`, and its query as `?query=`. |

With tenancy on, each address is rooted at `/t/{tenant}`.

### Sources

The catalogue lists what every configured [app source](../lattice.apps/README.md#app-sources)
offers. The **source selector** has one entry per source plus "All sources". Each
entry shows the source's name and kind (`Static` or `Dynamic`), and the selector's
hint describes the source in plain language, for example "shipped with the cluster,
one version of each app". Text search is enabled for a selected source only when it
advertises `Search`, and for "All sources" when any source does; otherwise the search
box reads "Search is not available" and says why. A slug offered by two sources appears as one row per source, and
an install always records the source it came from.

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

Installing binds each role to a membership group and confirms the ceiling. The install
is pinned to the manifest digest of the version that was reviewed, so if the source
changes the app between the review and the install - for example to request another
bridge operation - the cluster refuses the install, nothing is recorded, and the page
asks you to review it again. Each
role's group is a [picker](navigation-model.md#pickers) that suggests groups from the
identity directory, or the auth store's own groups when there is no directory, and
accepts any group id. Enabling,
disabling, upgrading and uninstalling are native actions with explicit confirmation;
the palette offers `apps.upgrade.{slug}` for an app with an update and
`apps.disable.{slug}` for an enabled one.
An upgrade shows what changed between the two versions. An install whose ceiling or
bridge grants no longer cover its manifest is shown as needing re-consent.
Reviewing the installed version manages that install rather than offering a new one:
the install steps stay hidden until a change starts, and the trees the install
already owns are not reported as ownership conflicts.

Role bindings can be changed after install. On the manage page of an installed app,
`/apps/catalogue/{source}/{slug}`, **Change role bindings...** lets you pick a
membership group for each declared role; an app's own page links there for an
`AppInstall` holder. A confirmation step shows each role's group now and after the
change, and **Apply bindings...** asks once more before anything is sent. Only the
bindings change: the consent, the ceiling and whether the app is enabled stay as they
are. For an enabled app its access rules are replaced at once, so members of a group
a role leaves lose that role. For an installed or disabled app the bindings take
effect when it is enabled. The change goes through
[`ILatticeAppRoleBindings`](../lattice.api.apps/README.md#re-binding-roles).

### Who holds an app role

A user holds an app role when a membership group they belong to is bound to that role,
and the role confers something within the consented ceiling. That is the only way to
hold one: rights granted through any other rule never add an app role, so an operator
with broad rights of their own sees only the apps whose roles they are bound to, and an
app's UI is never told a role the bridge would then refuse.

The cluster's access rules are consulted only after the binding holds, and only to take
a role away. An explicit deny on a bound member, on the role's trees or cluster-wide,
removes the role from **Your apps**, from the `roles` an app's UI is told, and from the
app's MCP tools. A deny narrower than the role, such as one key of a tree, leaves the
role held; it is enforced on each bridge call, because every bridge call runs under the
user's own identity.

One definition serves every surface, so Your apps, the app frame and the MCP tools
cannot disagree about who holds a role.

Because a role is held only through a group, installing an app does not by itself let
you open it, and the Apps area says so wherever it matters:

- **While binding roles** (at install, and when changing role bindings), each role says
  whether you are in the group it is bound to. When you are not, it offers
  **Add me to {group}**, a link to that group in Access, or binding the role to a group
  you are in, and warns when the bindings would leave you no role at all. The warning is
  advisory and never blocks.
- **After install**, the confirmation names the tenant the app was installed into, shows
  its address there (`/t/{tenant}/apps/{slug}`), and says whether you can open it.
- **On Your apps and on the app's own page**, an installed app you hold no role in says
  so, with its bound groups and the way to fix it - join a bound group, or bind a role to
  a group you are in - where **Open** would otherwise just be missing. **Manage** stays.
- Your apps also points at an install this session made in another tenant, with its
  tenant-rooted address.

Your group membership is read through the auth facade under your own credential. When it
cannot be read - you may not read membership, or you signed in with a token, whose shown
name is not your subject id - it is reported as unknown rather than guessed. None of
this changes who holds a role.

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
  directives, so the frame has no network egress of its own (but see the known
  limitation below). It is the one Explorer response that may be framed: the web
  head sends `X-Frame-Options: DENY` everywhere, and only this route's endpoint
  lifts it, for a file it serves.
- **Bundle delivery.** The bundle is never served to the frame over HTTP. The
  Explorer fetches it on the signed-in user's credential through
  `ILatticeAppWorkspace` and verifies every SHA-256 and the bundle digest. It then
  hands the bytes to the frame over a single `MessageChannel` port. The frame checks
  the digests again and runs the bundle from `blob:` URLs.
- **Bridge.** The port is the only channel. Every data request goes to the cluster's
  [`ILatticeAppBridge`](../lattice.api.apps/README.md#the-bridge). The bridge allows
  only what the app's own roles grant this user, inside the consented bridge
  operations and the app's own trees. The Explorer broker only validates, rate-limits
  and relays. It answers the operations that never reach the cluster
  (`context.read`, `context.user`, `nav.sync` and `ui.notify`) itself, and grants
  them from the launch's bridge set: the grants the operator consented to that the
  installed manifest still requests, which the cluster computes for
  `WorkspaceAppDescriptor.Ui.Bridge`. A grant the manifest requests but the operator
  never consented to is not offered to the frame.
- **Credentials.** No credential ever enters the frame. If the frame loads a new
  page, the Explorer closes it. An upgrade, disable or uninstall revokes the session,
  and the Explorer replaces the frame with a message asking you to open the app again.

**Residual risk.** A sandbox cannot guarantee zero egress. For example, WebRTC is not
blocked where the browser does not support the `webrtc` directive. Data handed to a
frame can therefore, in principle, leave it. This is why a frame only ever receives
data within its app's consented scope, and why the user's display name is a
separately consented operation (`context.user`).

**Known limitation: self-navigation in some WebKit builds.** The sandbox does not
stop a frame navigating itself. Chromium and Firefox always check such a
navigation against the Explorer's own `frame-src 'self'` and refuse a
cross-origin one before any request is sent. WebKit varies by build: some refuse
it the same way, while others let the request out, so a frame there can navigate
itself to another origin, sending whatever it put in the URL. What could leave
that way is bounded by the app's consented bridge scope, and the target's own
framing policy still stops the response rendering in the frame.

## Writing an app UI

An app UI is a bundle declared in the manifest's
[`ui` section](../lattice.apps/README.md#presentation-and-ui) and embedded in the app's
assembly. The Explorer provides everything that runs inside the frame. Your bundle is
a plain HTML fragment, stylesheets and self-contained scripts. The bootstrap defines
`globalThis.lattice` before your first script runs:

| Member | Purpose |
|---|---|
| `lattice.protocol` | The protocol version, `1`. |
| `lattice.ready` | A promise that resolves with the current appearance once the bridge is connected and your entry fragment and stylesheets are in place, before your first script runs. It rejects if the frame fails to load. |
| `lattice.request(op, args, options)` | Sends one bridge request, and resolves with its result or rejects with a `LatticeError` whose `code` is one of `denied`, `not_found`, `invalid`, `too_large`, `rate_limited`, `unavailable`, `conflict`. `options.timeoutMs` defaults to 30 seconds, at most 300 seconds; a timeout rejects with `unavailable`. |
| `lattice.on(event, handler)` | Subscribes to `context.changed`, `nav.changed` or `lattice.revoked`. |
| `lattice.assetUrl(path)` | The `blob:` URL of one of your bundle's assets. |
| `lattice.LatticeError` | The error class every rejection uses. |

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
  `context.read` returns: they are exactly the roles the user holds by binding, less any
  a deny took away. Treat any `denied` as the cluster's final word, since a deny
  narrower than a role still refuses individual calls. Never probe
  permissions with a write: every request is enforced by the cluster, so a wrong guess
  can only hide a control, never grant one.
- **No forms.** The sandbox has no `allow-forms`, so a `<form>` never submits. Use
  buttons and key handlers instead.
- **Styling.** The bootstrap document already loads the kit stylesheet,
  `lattice-app.css`, so your UI starts with the Explorer's Paper and Board materials,
  type and controls without linking anything. The kit sets the theme, contrast,
  density and reduced-motion attributes on `<html>` and updates them on
  `context.changed`. Keep layouts fluid: the frame fills the content area at every
  width, down to a phone.
- **Digests.** Pin every asset's digest and the bundle digest in the manifest. The
  [task-board sample](../../samples/Explorer/Apps/TaskBoard/README.md) computes them
  in a test that fails with the correct values whenever a file changes.

## See also

- [Installable apps](../lattice.apps/README.md)
- [App control, catalogue, workspace and bridge facades](../lattice.api.apps/README.md)
- [Task-board sample app](../../samples/Explorer/Apps/TaskBoard/README.md)
- [Explorer](README.md)
