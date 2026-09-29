# Managing access from the Explorer

The **Access** area is the Explorer surface for the cluster's authorization rule store, local membership groups, and access explanations. It drives the auth administration facade; the Explorer presents and submits the data, but the cluster remains the enforcement point for every read and mutation.

Access is cluster-wide. Tenant-rooted route forms exist so typed addresses can be normalised, but the navigator strips the tenant from Access addresses. App links from Access re-enter the current tenant where the Apps area is tenant-scoped.

## Availability

Access is visible only after a fail-closed probe can read the first page of the group catalogue. A successful probe makes the area visible in the directory spine. The probe is remembered for the signed-in identity on the current circuit and is asked again when the identity changes.

The area is hidden when the auth administration facade is absent, the cluster cannot be reached, the cluster does not serve access administration, or a signed-in identity is refused by the probe.

The only unavailable state is an anonymous or unsigned circuit that is refused by the probe. The exact sentence shown is:

> Sign in to administer access on this cluster.

Inside the area, denials become **Not permitted** empty states or inline errors. The standard denial text for access administration is:

> You are not permitted to administer access on this cluster. Ask a cluster administrator for the Admin grant on access administration.

Other faults are presented as plain sentences, for example that the cluster does not serve access administration, could not be reached, or did not answer.

## Addresses

The Access area is cluster-wide. These route forms exist in the shipped pages:

| Page | Plain address | Tenant-rooted form | Notes |
| --- | --- | --- | --- |
| Rules | `/access`, `/access/rules` | `/t/{tenant}/access`, `/t/{tenant}/access/rules` | Lists authorization rules and opens the new-rule dialog with `?new=true`. |
| Rule details | `/access/rules/{ruleId}` | `/t/{tenant}/access/rules/{ruleId}` | Shows one rule. Use `?tree={treeId}` when the rule id is reused under more than one governed tree. |
| Groups | `/access/groups` | `/t/{tenant}/access/groups` | Lists groups and opens the new-group dialog with `?new=true`. |
| Group details | `/access/groups/{groupId}` | `/t/{tenant}/access/groups/{groupId}` | Shows one group, its direct members, parent groups, and matching rules. |
| Explain | `/access/explain` | `/t/{tenant}/access/explain` | Explains one operation or lists effective permissions. |

Access uses these query keys:

- `new=true` opens the create dialog on the Rules or Groups page.
- `tree` qualifies a rule id on a rule detail page, and names the tree for an explanation.
- `subject` and `kind=user|group` prefill the Explain page subject.
- `operation` preselects the operation for an explanation.
- `view=permissions` opens the effective-permissions view on the Explain page.
- `key` and `prefix` travel in Explain addresses when the scope is a single key or key prefix.

## Pages and actions

### Rules

The Rules page lists every rule the caller may administer, 200 at a time. It has a rule search box, an ownership filter with **All rules**, **Authored**, and **App-owned**, and a **New rule** button.

A rule draft includes:

- a stable rule id, unique within the governed tree;
- an **Allow** or **Deny** effect;
- a subject, chosen as a user or group;
- a scope: whole tree, key prefix in a tree, single key in a tree, all trees, or access administration when delegation is enabled;
- one or more operations, grouped as data operations, administration, and cluster-wide capabilities;
- an optional condition.

The editor refuses an empty rule id, an app-owned id prefix, an empty subject, a missing tree or key where the scope needs one, reserved system trees, no operations, or cluster-wide capabilities on a narrower scope. When all-trees grants are off, the scope hint says that data operations in a cluster-wide rule are refused while cluster-wide App install and Telemetry capabilities can still be granted.

Saving a rule submits it directly to the cluster. Directory validation failures are shown beside the subject, app-owned-rule failures beside the rule id, and other denials or faults as a form error.

The Rule details page shows effect, subject, scope, operations, condition, and owner. Authored rules can be edited. Deleting a rule opens a destructive confirmation named **Delete this rule**; the confirmation text states that the rule stops applying at once and cannot be undone. App-owned rules are read-only, are attributed to their app, and link to `/apps/{slug}/roles` so role bindings can be changed in the Apps area. The page also links to Explain for the rule's subject.

### Groups

The Groups page lists groups 200 at a time and searches by group id or display name. **New group** opens a create dialog.

The group id field uses the same subject picker used elsewhere in Access. When an identity directory is available, the group id must resolve as a group before the create is sent. Choosing a directory match fills the display name. When no directory is available, the field says so and accepts the typed id as it is.

The Group details page shows the display name, direct members, parent groups, and rules that apply to the group. Operators can rename the group, add a user or nested group as a direct member, remove a direct member, explain access for the group, or delete the group.

Removing a member opens a destructive confirmation named **Remove this member**. Deleting a group opens a destructive confirmation named **Delete this group**; the confirmation says the group record is removed, rules that name it stop matching anyone through it, and the action cannot be undone.

When the cluster's membership merge mode means local membership has no effect, group and member data remain visible, but creating groups and adding or removing members are turned off. Rules that name a group still matter when a token asserts that group.

### Explain

The Explain page asks the cluster why a user or group is allowed or denied an operation on a scope. It can also list the rules that form a subject's effective permissions. The verdict is the cluster's verdict: the page renders the returned `Allowed` flag, default effect, reason, group closure, filtered range-read note, and matched rules in precedence order.

The form supports cluster-wide, tree, key-prefix, and single-key scopes. Groups in a result link back to their group pages. The group closure shown by the access facade is resolved from the membership directory for the named subject; a live caller's token may assert additional groups.

## Subject picking and directory validation

Every user or group field is a type-ahead picker (see [Pickers](navigation-model.md#pickers)) over the cluster's identity directory. It searches as you type: each query is one bounded directory search, and a burst of typing costs one search. It lists matching principals by id, with the display name, rendered as text, beside each; choosing one can fill a form's display name. There is no separate search button and no load-more control: the list is bounded, so keep typing to narrow it. Changing between user and group clears the id.

With a directory, only a listed principal is accepted, so an unknown id is refused inline before anything is sent, and the directory's own explanation of what a valid id looks like is shown as the hint. The same directory seam validates group creation and member additions when validation is required: unknown ids and wrong-kind ids fail before the membership write. With no configured directory, the picker says "No identity directory is configured, so the id is used as typed and is not validated." and accepts the typed id.

Other Access fields are pickers too. The Tree field of a rule or an explanation offers the trees you can reach, and a new rule's id is checked as you type against the rule ids already in use under its tree. That check reads one page of rules, so it is a guide rather than a guarantee: the cluster still refuses a real collision when the rule is saved.

## Access posture banner

Rules and Groups show the cluster's access posture when it can be read:

- authentication mode: anonymous, claims, Basic, or unknown;
- whether rules are **Enforced** or **Recorded, not enforced**;
- whether all-trees grants are on;
- whether access-admin delegation is on;
- whether an identity directory is configured.

If rules are recorded but not enforced, the page warns that the rule set is advisory until enforcement is turned on. If local group membership is inert, the page warns that groups and members defined in the Explorer have no effect on access.

## Palette and address completions

Access contributes these commands:

| Command id | Label | Target |
| --- | --- | --- |
| `access.explain` | `Explain access...` | `/access/explain` |
| `access.create-rule` | `Create an access rule` | `/access/rules?new=true` |
| `access.create-group` | `Create a group` | `/access/groups?new=true` |

The visible controls carry the same command ids: the Explain navigation link, **New rule**, and **New group**.

The address line completes `group:{id}` and `rule:{id}` from the first page of the group and rule catalogues. Typing `/access/groups/` or `/access/rules/` completes the same targets. A successful write invalidates the completion cache.

## Limits and caching

- The availability probe reads one group row.
- Rule and group lists load 200 rows at a time.
- A subject-picker query is one directory search bounded by the picker's limit (8 by default).
- Address completions read at most the facade's maximum auth page size for groups and rules.
- The access model and completion catalogues are scoped to the circuit. Writes clear the group and rule completion cache.
- The area availability answer is cached per signed-in identity on the circuit.

## Server authority

The access facade authorizes every administration call as access administration on the policy tree before reading or mutating membership or policy state. Policy explanations are computed through the same access gate used by the data plane. The Explorer is therefore an administrative client, not a second policy engine.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Area availability](area-availability.md)
- [Areas reference](areas.md#access)
- [Lattice Apps](lattice-apps.md)
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Adding a custom auth method](adding-a-custom-auth-method.md)
- [Auth API](../lattice.api.auth/README.md)
- [Auth engine](../lattice.auth/README.md)
- [Auth gRPC binding](../lattice.api.auth.grpc/README.md)
- [Membership](../lattice.membership/README.md)
- [Identity-directory providers](../lattice.membership/identity-directory-providers.md)