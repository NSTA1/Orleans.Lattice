# Managing access control from the Explorer

The Orleans.Lattice Explorer has a top-level area switcher above the per-tree
detail tabs. **Access** is the membership and authorization admin area. It lets
an operator inspect and edit the identity directory and the authorization rules
that gate a cluster whose State API has authorization enabled, and it explains
why a given subject is allowed or denied an operation - all over the existing
auth gRPC binding, with no new server surface.

## Where it sits

Access is one of the switcher's areas, alongside **Explore** (the tree browser),
**Backups**, and **Schema**. Selecting it swaps the whole working surface to the
access admin tabs. Like every area, it is registered in one place and carries an
advisory rule that decides whether it is available to the connected user.

The area drives the authorization admin surface of the auth API
([`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/README.md)); it holds
no policy logic of its own. It never re-implements a verdict: the Explain view
renders the server's `Allowed` flag verbatim, and any precedence ranking it shows
is a presentation-only aid to reading the rule set, not a second opinion.

## The four tabs

- **Users** - browse the identity directory's users, one page at a time with a
  *Load more* control, and select a user to see the groups it belongs to
  (directly and transitively) and the rules that mention it.
- **Groups** - browse groups, add and remove members (including nested groups),
  and see a group's direct and transitive membership. Group nesting is resolved
  through the server's membership listing, so the transitive views match what the
  server actually evaluates.
- **Policies** - author the authorization rules. A rule targets a scope chosen
  with the scope picker (whole tree, a key prefix, or a single key), one or more
  `LatticeOperation` values chosen from a multi-select, and an **Allow** or
  **Deny** effect. Existing rules are listed and can be removed.
- **Explain** - drive the facade's introspection: ask whether a subject may
  perform an operation on a scope (**Explain**) and see the effective permission
  set for a subject over a scope (**EffectivePermissions**). Both render the
  server's answer directly.

## Capability-aware, grey-out not hide

The whole area is gated by a single coarse capability, **AuthAdminAllowed**. It
is discovered with a fail-closed probe: the Explorer asks the server for the
smallest possible page of the admin surface, and only if that succeeds is the
area treated as available. If the probe is denied or the endpoint is
unreachable, the area entry stays visible but **disabled (greyed out)**, so the
user can see the capability exists without being able to enter it.

Inside the area every mutating action - creating or removing a rule, adding or
removing a member - is likewise shown disabled, not hidden, whenever the
capability is absent or an action is already in flight. Nothing is silently
dropped from the UI.

## Advisory, not a security boundary

The grey-out is a usability affordance only. The **server remains the
fail-closed enforcement point**: every real read or mutation is authorized on
the server when it runs, regardless of what the cached capability said. If the
capability was over-optimistic - for example the grant changed after it was
probed - the action still fails closed on the server, and the Explorer surfaces a
clean "not permitted" message rather than an unhandled error. The probe itself
has no side effects; it never creates, changes, or removes anything.

## See also

- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Adding a custom auth method](adding-a-custom-auth-method.md)
- [`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/README.md) - the auth
  gRPC binding and typed admin client the area drives.
