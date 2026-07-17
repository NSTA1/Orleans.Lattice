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

## Picking a subject: searchable typeahead

Everywhere the area needs a subject - adding a group member, targeting a policy
rule, or naming the subject to explain - it offers the same **searchable subject
picker** instead of a raw id box. As the operator types, the picker searches the
configured [identity-directory provider](../lattice.membership/identity-directory-providers.md)
across users and groups, coalescing keystrokes so a burst of typing issues a
single search, and pages further matches with a *Load more* control. Each result
shows the principal's friendly display name as its primary label, with the
underlying id (for example the object id) available as a hover tooltip. Selecting a
result fills in both the id and its kind, and the "Selected" line likewise leads
with the friendly name and keeps the id on hover.

The whole area follows the same convention: everywhere it renders a principal -
the user and group lists, a group's direct members, the member add and remove
status messages, and the subject and group-closure lines in an Explain verdict or
an Effective-permissions result - it leads with the friendly display name resolved
from the directory and keeps the raw id on a hover tooltip. Names are resolved on
load and cached, and every one falls back to the raw id when no directory is
configured or an id does not resolve, so the display never blocks on the directory
and never regresses to a broken label.

When the configured provider is the no-op default (no directory), the picker
reports the directory as **unavailable** and degrades to a plain free-text box:
the entered id is used as-is and is **not** validated. It never enumerates the
tenant in that state - it simply takes what is typed.

## Fail-closed create

The Users and Groups tabs create principals through the same picker, and creation
is **validated and fail-closed**. When a directory is available, the entered id is
resolved against it before the principal is saved:

- resolved, and the kind matches, the create proceeds;
- resolved as the wrong kind (a group id typed into the user form, or vice
  versa), it is blocked with an inline kind-mismatch message;
- not resolved at all, it is blocked with a "no such principal in the directory"
  message - an unknown id can never be created.

Only when the directory is unavailable does the form fall back to accepting the id
unvalidated, and it says so. Each provider supplies a one-line **Explanation** of
what a valid id looks like (for example an object id, or a UPN), which the form
shows beneath the input so the operator knows what to type. When a directory
result is selected in a create form, its friendly display name also auto-fills the
new principal's display-name field, which the operator can still edit before
saving.

## Access-state banner

The area shows a banner describing the cluster's real authentication and
enforcement posture, read from the server, so an operator is never guessing:

- the **authentication mode** the silo can authoritatively see from its
  registered authenticators (anonymous, claims-based, or unknown); and
- a **"recorded but not enforced"** notice when the server confirms authorization
  rules are being stored but the connected State API is not actually enforcing
  them - so an operator does not mistake an advisory rule set for a live gate. A
  failed or denied read is never rendered as "unenforced".

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

- [Identity-directory providers](../lattice.membership/identity-directory-providers.md) - the provider seam that backs the subject picker and the validated create form.
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Adding a custom auth method](adding-a-custom-auth-method.md)
- [`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/README.md) - the auth
  gRPC binding and typed admin client the area drives.
