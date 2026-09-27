# Orleans.Lattice.Apps

Pure data contracts for installable Lattice apps. JSON is canonical: a manifest
can be inspected through `AppManifestParser.Parse` before loading app code.
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
scopes. The ceiling is pinned per app id and version; the compiler must intersect
every rule with it. `ApprovedExceptionScopes` records explicit operator approval
outside the app namespace, including cross-app or adopted legacy trees. Adoption
must fail activation without that approval; these contracts do not implement
activation or grant authority themselves. Adopted ids are unique within a manifest,
cannot use the `a/`, `_lattice_`, `sys-`, or `t/` prefixes or be the cluster-wide
sentinel `*`, and are at most 1024 characters with no surrounding white space or
control characters. No exception can approve a scope on `*` or on a `_lattice_`,
`sys-` or `t/` tree, and manifests are size-bounded (1 MiB, 256 entries per
section) before any per-entry work.

Tree sizing fields are optional pins; omission inherits host defaults.
`VirtualShardCount` is fixed at creation and cannot change in an upgrade;
pass the previous manifest to `AppManifestValidator.Validate` to check this.
Rebuildable trees may be rederived rather than restored by app-scoped backup.
MCP tool names are app-local and become `{slug}_{name}` at dispatch.
