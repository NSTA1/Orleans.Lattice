# Orleans.Lattice.Apps

The engine for installable Lattice apps: the manifest contract, the app registry,
the role-to-rule compiler, the in-image app source, the activation pipeline and the
change-feed subscription runtime. Register it on the silo with `AddLatticeApps()`
after `AddLattice(...)`, and make each app shipped in the image installable with
`AddLatticeApp(slug, assembly, manifestResourceName)`. Installing and enabling an
app is an operator action, normally through `Orleans.Lattice.Api.Apps`; activation
also needs membership and authorization and fails closed without them.

JSON is canonical: a manifest can be inspected through `AppManifestParser.Parse`
before loading app code.
`AppManifestResources.Load` reads a named embedded resource from an already
available assembly without depending on the package's filesystem layout.
Neither entry point invokes app code, installs an app, or registers services.

Manifests declare identity, trees, flat membership-group roles, optional
replication intent and schema-family bindings, subscriptions, and MCP tools.
Parsing and validation return `AppManifestResult` with path-addressed errors;
invalid content never produces a usable manifest. Missing resources also return
an error. Null assembly/stream arguments are programming errors.

`AppManifestResources.GetJsonSchema` exposes the bundled JSON Schema (also
packed under `schema/`). The parser checks JSON shape, rejects unknown and
duplicate properties, then checks semantic constraints such as unique names,
known operation bits, and references to declared trees. Schema-family bindings
are declarations only: they do not run migrations or validate stored values.

Scope templates contain a local tree name and an optional other-app slug.
Omitting the slug means the manifest's own app. A compiler composes these as
`a/{app}/{tree}` before tenant composition unless the declaration adopts a legacy
physical tree through `AdoptedTreeId`; no arbitrary path interpolation or
role inheritance is supported. A declaration is a request, not authorization:
provenance is descriptive, and cross-app access still requires operator consent.
JSON operations are readable name arrays, such as `["Read", "RangeRead"]`,
not numbers. Unknown, repeated, composite and `None` names are rejected.
Scopeless `Telemetry` and `AppInstall` do not belong in a tree-scoped role.
Tree-scoped administration, backup, restore, schema, replication and lifecycle
requests remain explicit and require the corresponding installation ceiling.

`AppRoleBinding.Create` binds a role to a membership group id.
`AppCapabilityCeiling.Structural` records allowed operations with no exception
scopes. The ceiling is pinned per app id and version; the compiler checks every
rule against it. `ApprovedExceptionScopes` records explicit operator approval
outside the app namespace, including cross-app or adopted legacy trees. Adoption
fails activation without that approval; the ceiling and binding records themselves
grant nothing. Adopted ids are unique within a manifest,
cannot use the `a/`, `_lattice_`, `sys-`, or `t/` prefixes or be the cluster-wide
sentinel `*`, and are at most 1024 characters with no surrounding white space or
control characters. No exception can approve a scope on `*` or on a `_lattice_`,
`sys-` or `t/` tree, and manifests are size-bounded (1 MiB, 256 entries per
section) before any per-entry work.

Tree sizing fields are optional pins; omission inherits host defaults. An install
applies them only when it first registers a tree, so a tree that already exists
keeps its structure. `VirtualShardCount` cannot change in an upgrade; pass the
previous manifest to `AppManifestValidator.Validate` to check this. The tree does
not keep the declared slot count through every later operation, though: an
`ILattice.ReshardAsync` to a different shard count while the tree is still empty,
or an `ILattice.ResizeAsync` once it holds data, leaves it routing over the
default 4096 virtual slots.
`Rebuildable` marks a tree whose contents can be re-derived; no backup or restore
path in this version acts on it.
MCP tool names are app-local and become `{slug}_{name}` at dispatch.
