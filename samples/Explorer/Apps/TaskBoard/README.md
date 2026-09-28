# Task board: the pilot app with an untrusted UI

`task-board` is a small Lattice App that ships its own UI. It is the Explorer
sample's pilot for the whole app path: a manifest with a `presentation` and a
`ui` section, a bundle that runs in a sandboxed frame, and discovery from the
in-image app source. Its cards are JSON values stored under `tasks/{id}` in the
app's one tree, `tasks`.

## What is in the folder

| Path | Role |
|------|------|
| `src/manifest.json` | The manifest: one tree, the `viewer` and `editor` roles, the presentation, and the UI bundle with its digests and bridge operations. |
| `src/ui/index.html` | The entry fragment, inserted into the frame's body. |
| `src/ui/app.css` | The app's stylesheet, layered on the AppKit kit stylesheet (`lattice-app.css`). |
| `src/ui/app.mjs` | The app's one ES module. It is self-contained and talks to the Explorer only through `globalThis.lattice`. |
| `src/ui/icon.svg` | The catalogue icon, shown through an `<img>` element only. |
| `src/TaskBoardApp.cs` | The slug and embedded-resource names the silo registers the app with. |
| `test/` | `TaskBoard.Tests`: the manifest validates, every digest matches, the in-image source enumerates and serves the app, and the bundle stays self-contained. |

There is no build toolchain. The bundle is plain HTML, CSS and one ES module,
embedded into the class library as resources. The digests in `manifest.json`
are checked, not produced: after editing a bundle file, run

```
dotnet test samples/Explorer/Apps/TaskBoard/test/TaskBoard.Tests.csproj
```

and paste the values the failing `The_manifest_digests_match_the_bundle_files`
test prints into `manifest.json`. The bundle files are pinned to LF line
endings, because the digests are over their exact bytes.

## How the sample registers it

The Explorer sample adds the app to the silo's in-image source with one line:

```
silo.AddLatticeApp(TaskBoardApp.Slug, TaskBoardApp.Assembly, TaskBoardApp.ManifestResourceName);
```

Registering an app only makes it *available*. Nothing is installed, consented
or enabled until an administrator does so in the Explorer.

## Walkthrough

Sign in as the demo administrator (the default), then:

1. **Find it in the catalogue.** Open **Apps**. The source selector lists
   `in-image` beside **All sources**; choose `in-image` and the **Available**
   filter. Task board appears with its icon, display name and summary. All of
   that text comes from the manifest and is shown as plain text.
2. **Review consent.** Open the entry and choose **Install**. The review shows
   what the app asks for, drawn against its own `a/task-board/` namespace:
   - one tree, `tasks`;
   - two roles, `viewer` (`Read`, `RangeRead`) and `editor` (`Read`,
     `RangeRead`, `Write`, `Delete`), both scoped to `tasks`;
   - six bridge operations for its UI: `context.read`, `data.read`,
     `data.write`, `data.delete`, `nav.sync` and `ui.notify`, the data ones
     limited to `tasks`.

   Nothing reaches outside the app's namespace, so there are no exceptions to
   approve. The app never asks for `context.user`, so it never learns who you
   are.
3. **Bind roles to groups.** Bind `editor` to the `task-editors` group and
   `viewer` to the `task-viewers` group. Leave the `visitors` group unbound.
4. **Install, then enable.** Install records the consent and the bindings;
   enabling the install is what lets role holders open it.
5. **Open it.** Signed in as a member of `task-editors` (`alice`), open
   **Apps**, then Task board, then its **Open** tab. The board loads in a
   sandboxed frame. Add a task, select it, move it between **To do**,
   **Doing** and **Done**, and delete it. Selecting a card updates the address
   line, so the link to a task can be copied and reopened. Switch the theme
   between Paper and Board and the board follows it at once.

### The same app, three groups

| Signed in as | Group | Role | What they see |
|--------------|-------|------|---------------|
| `alice` | `task-editors` | `editor` | The full board: add, move and delete controls. |
| `bob` | `task-viewers` | `viewer` | The same board, read-only. The add, move and delete controls are not shown, and a note says the role cannot change the board. |
| `carol` | `visitors` | none | Nothing. Task board is not in her apps at all, and its address resolves as not found. |

## How the UI stays untrusted

- The frame is sandboxed with an opaque origin and a policy that forbids every
  network connection. The module never calls `fetch`, opens a socket, reads
  storage or cookies, or touches the parent window; a test enforces that.
- Every read and write goes through `lattice.request(...)` to the Explorer's
  broker and then to the cluster, which allows it only when both the app's
  consented grants and the signed-in user's own rights allow it. An operator
  who can write every tree still gets a read-only board when bound only as a
  `viewer`.
- The frame is never told the user's roles. The board starts read-only and
  shows its write controls only after the cluster accepts a harmless probe (a
  delete of `probe/write-access`, a key that never holds a task). A viewer's
  probe is refused, so a viewer never sees a control the bridge would refuse.
- Stored values are treated as untrusted input: a card that does not parse is
  skipped, and titles are always rendered as text.
- The layout reflows from three columns to one on a 360px screen, every
  control is a 44px touch target, and nothing depends on hover.
