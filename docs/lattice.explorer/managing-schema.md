# Managing schema from the Explorer

> **Hidden by default.** The Schema area is withheld from the Explorer's switcher
> for the initial release because its versioning UI cannot yet express what
> differs between schema versions. Surface it by calling
> `AddExplorerSchemaPlugin()` on the head's service collection - registration is
> the whole of the opt-in, and it replaces the retired `EnableSchemaArea` flag
> that `LatticeExplorerWebOptions` once carried (see
> [Running the Explorer](running-the-explorer.md)). The schema control services
> ship and stay registered regardless, so this only decides whether the tab is
> rendered. Tracking
> issue: re-surface the area once version-shape differences are expressible.

The Orleans.Lattice Explorer has a top-level area switcher above the per-tree
detail tabs. **Schema** is the schema-management admin area. It lets an operator
inspect and edit a tree's write-validation policy and its value-versioning
config, run a read-only compliance audit, and inspect the strict-mode dead-letter
queue - over the schema control gRPC binding, with no new server surface.

## Where it sits

Schema is one of the switcher's areas, alongside **Explore** (the tree browser),
**Backups**, and **Access**. Selecting it swaps the working surface to the schema
admin tabs. Like every area, it is registered in one place and carries an
advisory availability rule.

The area drives the schema control API
([`Orleans.Lattice.Api.Schema.Grpc`](../lattice.api.schema.grpc/README.md)) over
the transport-agnostic facade
([`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md)); the underlying
enforcement, versioning, and dead-letter semantics belong to
[`Orleans.Lattice.Schema`](../lattice.schema/README.md) and are not
re-implemented here.

## Per-tree, selection auto-loads

Schema state is per tree. Each tab starts empty; picking a tree from the tree
list immediately probes that tree (there is no separate **Load** button - selecting
a tree loads it). The area is capability-gated as a whole (see below), and the
per-tree load also reports whether the specific tree is governed by a policy at
all - a tree with no policy accepts all values, and the area says so rather than
showing an error.

## The three tabs

- **Policy** - view, set, and clear the tree's write-validation policy. A tree
  with no policy accepts every value; setting a policy turns on validation for
  subsequent writes. When a policy is loaded, this tab also hosts a **Compliance**
  action: a **read-only** audit that scans the tree's entries against its compiled
  policy and reports how many values are compliant versus non-compliant, with a
  breakdown of the reasons. It never mutates anything; a tree with no policy is
  reported as ungoverned.
- **Versions** - view, set, advance, migrate, and clear the tree's
  envelope-version config, and see the status of the last remediation run.
  Advancing the target version can either leave existing values in place or
  migrate them up to the new version. Version operations require the versioning
  add-on to be registered on the silo; when it is not, the area reports that
  clearly instead of failing opaquely.
- **Dead letters** - list the writes that strict-mode validation diverted (the
  schema-rejected entries), and show their count.

## Capability-aware, demote not hide

The area is gated in two layers. The coarse **SchemaAllowed** gate is the
capability probe's own answer: the probe reports each capability as a flag
rather than throwing on an authorization denial, and it is the flags it reports,
not the fact that it completed, that constitute the grant - a probe that comes
back with nothing set withholds rather than admits. The resulting state follows
the fault. A cluster that does not serve schema administration at all answers
`Unimplemented`, and the area resolves `Unavailable` and renders no entry, with
the absence explained in the rail's capabilities affordance rather than left as
a silent gap. Any other transport fault, including an unreachable endpoint or a
console not yet configured with one, withholds the grant instead: the area
resolves `Denied` for a signed-in caller and `AuthenticationRequired` for an
anonymous one, and is re-probed when the connection status next changes. Inside
the area, each mutating action - setting or clearing a policy, changing or
advancing the version config, running remediation, scanning compliance -
disables from the **per-tree capability snapshot** the panel requests when a
tree is loaded (and also whenever no tree is loaded or an action is already in
flight), not from the coarse gate.

## Advisory, not a security boundary

The gating is a usability affordance only. The **server remains the
fail-closed enforcement point**: every real read or mutation authorizes the
tree's scope on the server when it runs - Read authority for the inspect verbs
and the compliance audit, SchemaAdmin authority for the mutations - regardless of
what the cached capability said. An over-optimistic capability still fails closed
on the server, and the Explorer surfaces a clean "not permitted" message rather
than an unhandled error. The capability probe itself has no side effects.

## See also

- [Schema enforcement and versioning](../lattice.schema/README.md) - the engine
  whose control surface this area drives.
- [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md) - the
  transport-agnostic schema control facade.
- [`Orleans.Lattice.Api.Schema.Grpc`](../lattice.api.schema.grpc/README.md) - the
  gRPC binding and typed client the area drives.
