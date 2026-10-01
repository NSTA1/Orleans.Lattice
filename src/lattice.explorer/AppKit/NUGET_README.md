# Orleans.Lattice.Explorer.AppKit

Static assets for everything that runs **inside** a Lattice App's sandboxed
frame in the Orleans.Lattice Explorer: the app-agnostic bootstrap document, its
loader, the in-frame `lattice` API, the kit stylesheet and fonts, and the frame
protocol schema. The package also carries one .NET type,
`Orleans.Lattice.Explorer.AppKit.AppKitProtocol`, which spells the protocol's
message names, operation names, codes and bounds for the host side.

The Explorer serves the kit from `_content/Orleans.Lattice.Explorer.AppKit/appkit/v1/`,
and maps it for frames at `{base}/_apps/frame/v1/`. The protocol version is the
path segment, so a breaking change ships beside the old version rather than
over it. You do not reference this package directly; the Explorer web head
brings it in.

This package is in progress and has not shipped a release.

## What is in `appkit/v1/`

| File | Role |
|------|------|
| `frame.html` | The static bootstrap document every app frame loads. No inline script or style. |
| `boot.js` | The loader: handshake, bundle verification, materialisation, and `globalThis.lattice`. |
| `lattice-app.css` | The kit stylesheet: tokens, fonts, type, spacing, booktabs tables, buttons, inputs, focus ring. |
| `tokens.css`, `fonts/` | The documentation site's own tokens and fonts, kept as byte-identical copies that a test fails the build on the moment they drift from the site's. |
| `protocol.schema.json` | The JSON Schema of every protocol message, in both directions. |

The `example/` folder beside the package is a minimal manifest-shaped bundle:
a manifest, an entry fragment, one stylesheet and one module.

## Trust model

A frame is untrusted. It is loaded as `<iframe sandbox="allow-scripts">`, so it
has an opaque origin, and it is served with a policy that forbids inline script
and style, every network connection, and every other frame:
`sandbox allow-scripts; default-src 'none'; script-src 'self' blob:; style-src 'self' blob:; img-src 'self' blob: data:; font-src 'self' blob:; connect-src 'none'; frame-src 'none'; form-action 'none'; base-uri 'none'; frame-ancestors 'self'; webrtc 'block'`
(a browser that does not support the `webrtc` directive ignores it).
No credential and no Explorer state ever enters it. An app's bundle is never
served over HTTP: the Explorer fetches it on the user's credential, verifies
every digest, and transfers the bytes over a `MessageChannel` port, and the
bootstrap turns them into `blob:` URLs.

The frame reaches the cluster only through the operations below. Each request
is checked by the Explorer's broker and then authoritatively by the cluster,
against the app's consented bridge operations and the app roles the signed-in
user holds by binding (never the user's other rights), and it then runs under
the user's own identity, so the user's own access rules still apply. The
protocol has **no lifecycle, consent, authentication, cross-app or fetch
operations**, and the frame is **sized by the host**: there is no auto-height
channel.

## Protocol v1

### Handshake

1. The bootstrap posts `{ type: "lattice.ready", protocol: 1 }` to its parent
   window, with target origin `*` (an opaque frame cannot know its parent's
   origin, and the message carries nothing).
2. The host answers with `{ type: "lattice.hello", protocol: 1 }` and exactly
   one transferred `MessagePort`. The bootstrap accepts the **first** hello
   whose source is its parent window, and then ignores every later window
   message from anyone. A hello with another protocol version fails the frame
   with `protocol_unsupported`.
3. Everything else travels over the port.

### Bundle delivery

The host sends the bundle once:

```text
{ type: "lattice.bundle", protocol: 1,
  appearance: { theme: "paper" | "board", contrast: "standard" | "more",
                density: "comfortable" | "compact", reducedMotion: boolean },
  bundle: { entry: "index.html",
            styles: ["app.css"],
            scripts: [{ path: "app.mjs", module: true }],
            bundleDigest: "<64 lower-case hex>",
            assets: { "<path>": { mediaType, digest: "<64 lower-case hex>", bytes: ArrayBuffer | Uint8Array } } } }
```

`assets` carries every asset the manifest lists. The bootstrap checks the
shape (at most 256 assets, 2 MiB each and 16 MiB in all; the entry is
`text/html`, stylesheets `text/css`, scripts `text/javascript`), re-verifies
each asset's SHA-256 and the bundle digest with Web Crypto, creates a `blob:`
URL per asset, sets the appearance attributes, inserts the entry fragment into
`<body>`, appends the stylesheets in order, and then loads the scripts **one at
a time in manifest order**, classic or module as declared. `globalThis.lattice`
exists before the first app script runs. When the last script has loaded it
posts `{ type: "lattice.loaded", protocol: 1 }`.

On any failure it replaces the frame's content with plain text and posts
`{ type: "lattice.failed", protocol: 1, code, message }` (over the port, or to
the parent window if no port exists yet). `code` is one of
`protocol_unsupported`, `bundle_malformed`, `bundle_too_large`,
`asset_missing`, `digest_mismatch`, `bundle_digest_mismatch`,
`crypto_unavailable`, `load_failed` and `internal`.

A stylesheet loaded from a `blob:` URL has no base URL, so `url()` cannot name
another bundle asset. Reach assets from script with `lattice.assetUrl(path)`.

### Requests and responses

A request is `{ id, op, args }` with an increasing integer `id`. A response is
`{ id, ok: true, result }` or `{ id, ok: false, error: { code, message } }`,
and carries no `type` member. `code` is one of `denied`, `not_found`,
`invalid`, `too_large`, `rate_limited`, `unavailable` and `conflict`.

| `op` | `args` | `result` |
|------|--------|----------|
| `context.read` | `{}` | `{ slug, version, protocol, theme, contrast, density, reducedMotion, tenant, roles? }` - `tenant` is a display name, never an id, or null; `roles` is the caller's app role names in this app (authorization metadata, refreshed on each launch; absent from an older host) |
| `context.user` | `{}` | `{ displayName }` - the display name only |
| `data.read` | `{ action: "get", tree, key }` | `{ found, value }` |
| `data.read` | `{ action: "scan", tree, prefix, pageSize?, continuation? }` | `{ entries: [{ key, value }], continuation }` |
| `data.write` | `{ action: "set", tree, key, value }` | `{}` |
| `data.delete` | `{ action: "delete", tree, key }` | `{ deleted }` |
| `nav.sync` | `{ path }` - the frame's internal path, starting with `/` | `{}` |
| `ui.notify` | `{ text }` - text only, 1 to 200 characters | `{}` |

`tree` is always a **logical** tree name the app declared (`^[a-z][a-z0-9_-]*$`,
at most 128 characters), never a physical id. Values are base64. The bounds
are 64 KiB per value, 1 MiB per response, 128 KiB per request, a page size of
at most 200, keys and prefixes of at most 1024 characters, and continuations of
at most 4096. The operation names are exactly the bridge vocabulary of
`Orleans.Lattice.Apps.AppUiBridgeOperations`.

### Host events

The host sends `{ type, data }` over the port:

- `context.changed` - `data` is the appearance object; the bootstrap reapplies
  the attributes before handlers run.
- `nav.changed` - `data` is `{ path }`, when the host moves the frame's
  internal path.
- `lattice.revoked` - `data` is `{ reason }` (`disabled`, `uninstalled`,
  `upgraded`, `revision` or `closed`). The kit rejects every pending request
  with `unavailable`, runs the handlers, closes the port and shows a plain-text
  notice.

## The in-frame API

```text
await lattice.ready;                      // resolves with the appearance once the bundle is verified
const page = await lattice.request("data.read",
    { action: "scan", tree: "notes", prefix: "" }, { timeoutMs: 10000 });
const off = lattice.on("context.changed", appearance => { /* ... */ });
img.src = lattice.assetUrl("images/logo.svg");
```

- `request(op, args, options)` returns a promise of the result, or rejects with
  a `lattice.LatticeError` whose `code` is one of the codes above. Arguments are
  checked locally first, a timeout (30 s by default, `timeoutMs` up to 300 s) or
  a closed port rejects with `unavailable`, and only plain JSON data crosses the
  port.
- `on(event, handler)` subscribes to a host event and returns a function that
  unsubscribes.
- `assetUrl(path)` returns the `blob:` URL of a bundle asset, or throws
  `not_found`.
- `ready` is a promise that resolves before the first app script runs, and
  rejects if the frame fails or is revoked first.

## The kit stylesheet

`lattice-app.css` imports `tokens.css`, declares the two self-hosted fonts, and
keys everything off attributes the bootstrap sets on `<html>`: `data-theme`
(`paper` or `board`, mirrored to `data-bs-theme` for the tokens),
`data-contrast`, `data-density` and `data-reduced-motion`. Its classes use the
`lt-app-` prefix: `lt-app-table` (booktabs), `lt-app-button` with `--quiet` and
`--destructive`, `lt-app-input`, `lt-app-label`, `lt-app-row`, `lt-app-stack`,
`lt-app-muted` and `lt-app-mono`, plus `lt-app-scroll`, a wrapper that lets a
wide table scroll inside itself, and `lt-app-notice`, the plain-text notice the
bootstrap shows in place of the app when the app cannot be loaded, when the
Explorer closes it, or when the frame document is opened outside the Explorer,
which an app may use for its own status line.

The defaults are fluid, because a frame fills the Explorer's content region at
every width down to a 360px phone: controls are 44px touch targets in
comfortable density (never below 24px in compact), media never overflows, and
long data wraps. The kit ships no width queries; an app owns its own layout.
