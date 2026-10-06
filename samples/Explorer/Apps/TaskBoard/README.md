# Task board: the pilot app with an untrusted UI

`task-board` is a small Lattice App that ships its own UI. It is the Explorer
sample's pilot for the whole app path: a manifest with a `presentation` and a
`ui` section, a bundle that runs in a sandboxed frame, and discovery from the
in-image app source. Its cards are JSON values stored under `tasks/{id}` in the
app's one tree, `tasks`.

## What is in the folder

| Path | Role |
|------|------|
| `src/manifest.json` | The manifest: one tree, declared for replication, the `viewer` and `editor` roles, the presentation, and the UI bundle with its digests and bridge operations. |
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

The walkthrough switches between identities in one run, and the sample keeps
everything in memory, so start it signed out:

```
dotnet run --project samples/Explorer/Explorer.csproj -- --sign-in-as none
```

Sign in as `explorer-admin` (any password) from the console's **Sign in**
dialog, then:

1. **Find it in the catalogue.** Open **Apps**. The source selector lists
   `in-image` beside **All sources**; choose `in-image` and the **Available**
   filter. Task board appears with its icon, display name and summary. All of
   that text comes from the manifest and is shown as plain text.
2. **Review consent.** Open the entry and choose **Install...**. The review shows
   what the app asks for, drawn against its own `a/task-board/` namespace:
   - tree `tasks`;
   - roles `viewer` (`Read`, `RangeRead`) and `editor` (`Read`,
     `RangeRead`, `Write`, `Delete`), both scoped to `tasks`;
   - replication of `tasks`, last-writer-wins (see [Tenants](#tenants));
   - bridge operations for its UI: `context.read`, `data.read`,
     `data.write`, `data.delete`, `nav.sync` and `ui.notify`, the data ones
     limited to `tasks`.

   Nothing reaches outside the app's namespace, so there are no exceptions to
   approve. The app never asks for `context.user`, so it never learns your
   name. It learns only which of its own roles you hold, through
   `context.read`.
3. **Bind roles to groups.** Bind `editor` to the `task-editors` group and
   `viewer` to the `task-viewers` group. Leave the `visitors` group unbound.
4. **Install, then enable.** Install records the consent and the bindings;
   enabling the install is what lets role holders open it.
5. **Open it.** **Sign out**, then sign in as a member of `task-editors`
   (`alice`), open **Apps**, then Task board, and choose **Open Task board** on its
   overview. The board opens in a new browser window, alone in a sandboxed frame. Add a
   task, select it, move it between **To do**,
   **Doing** and **Done**, and delete it. Selecting a card updates the window's address,
   so the link to a task can be copied and reopened. The board opens in the console's
   own appearance (Paper or Board, contrast and density); an open board keeps the
   appearance it started with until it is opened again.

### The same app under different groups

| Signed in as | Group | Role | What they see |
|--------------|-------|------|---------------|
| `alice` | `task-editors` | `editor` | The full board: add, move and delete controls. |
| `bob` | `task-viewers` | `viewer` | The same board, read-only. `context.read` reports only `viewer`, so the add, move and delete controls are not shown, and a note says the role cannot change the board. |
| `carol` | `visitors` | none | Nothing. Task board is not in her apps at all, and its address resolves as not found. |

## Tenants

An app is installed per tenant: each install has its own consent, role
bindings and lifecycle, and its trees live in that tenant's namespace. The
manifest's one tree, `tasks`, is `a/task-board/tasks` in the default tenant and
`t/{tenant}/a/task-board/tasks` in any other.

The manifest also declares `tasks` for replication, last-writer-wins. Declaring
it enrols nothing by itself: installing and enabling the app enrols that
install's tree, through the cluster's runtime replication control. On the
Explorer sample's two-region estate that control is on, so every install's
`tasks` tree replicates between `east` and `west`; with `--minimal` there is no
peer and the declaration is inert.

The sample installs the task board in tenant `acme` at startup, the way a
tenant's own install would be recorded: `editor` bound to `acme-editors`
(`acme-admin`), and three cards on the board. Its tree,
`t/acme/a/task-board/tasks`, is listed in **Replication > Enrolled trees** as a
runtime enrolment, and its link to `west` is live. As the operator, `/t/acme/data`
lists it as an app tree.

The walkthrough above installs a second, independent copy. An install from the
console lands in the tenant its address names: the walkthrough starts at
`/t/default/apps`, where the operator's console opens, so its copy is in the
default tenant, which is why `alice`, `bob` and `carol`, who belong to no tenant,
see it. The same steps at `/t/globex/apps` would install a copy in `globex`
instead. The default tenant's copy and acme's copy share nothing: separate
consent, separate bindings, separate boards.

## How the UI stays untrusted

- The frame is sandboxed with an opaque origin and a policy that forbids every
  network connection. The module never calls `fetch`, opens a socket, reads
  storage or cookies, or touches the parent window; a test enforces that.
- Every read and write goes through `lattice.request(...)` to the Explorer's
  broker and then to the cluster, which allows it only when the app's
  consented grants, an app role the signed-in user holds by binding, and the
  user's own rights all allow it. An operator who can write every tree still
  gets a read-only board when bound only as a `viewer`.
- The frame learns which of this app's roles the signed-in user holds from the
  `roles` member of `context.read`: role names only, never their groups. The
  board starts read-only and shows its write controls only when that list holds
  `editor`, the one role in its manifest that may write. A viewer's list holds
  `viewer`, so a viewer never sees a control the bridge would refuse. If a
  write is still refused (a role unbound mid-session, say), the board drops
  back to read-only. A host that sends no `roles` leaves the board read-only.
  The role list only hides or shows controls; the cluster enforces every
  write.
- Stored values are treated as untrusted input: a card that does not parse is
  skipped, and titles are always rendered as text.
- The layout reflows from three columns to one on a 360px screen, every
  control is a 44px touch target, and nothing depends on hover.
