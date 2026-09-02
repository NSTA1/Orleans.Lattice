# Navigation visibility policy

The rail shows areas you cannot currently open. This page explains why, what
each state renders as, and the one thing this policy is explicitly not.

## This is a usability policy, not a security control

The console's access gates are **advisory**. The server is the sole enforcement
point: every call is authorized server-side regardless of what the console
chose to draw. Hiding an entry therefore buys no security whatsoever, and
nothing on this page should be relied upon as a security boundary.

That is precisely what makes showing a denied entry affordable. If hiding
bought security there would be a trade to argue about. It does not, so the only
question left is which choice serves the user, and being told a capability
exists is more useful than being unable to tell it apart from one that does
not.

One real caveat bounds this. **Capability names are safe to reveal; instance
names are not.** "This cluster has a Backups area" discloses a product feature.
"This cluster has a tenant called `acme`" discloses a customer. The policy on
this page therefore governs navigation areas only, never data-derived entries
such as tenant ids or tree names.

## The four states

| State | When it is correct | How it renders |
| --- | --- | --- |
| `Allowed` | The caller can perform the operation | Normal, active entry |
| `AuthenticationRequired` | The caller is anonymous | Prominent and clickable, inviting sign-in |
| `Denied` | The caller is authenticated but holds no grant | Visible but demoted, below a divider, at lower visual weight, stating a remedy |
| `Unavailable` | The cluster does not serve the capability at all | Hidden, and explained once in a capabilities affordance |

The order matters, and is evaluated as written: capability, then grant, then
credential. An absent capability wins over everything, because neither a
credential nor a grant can conjure a facade the cluster does not serve. A
demonstrated grant is checked next, so a caller who provably holds it is
admitted whatever the shell believes about their sign-in state. Only a caller
who did not demonstrate a grant is then sorted by credential: anonymous yields
`AuthenticationRequired`, and signed-in yields `Denied`.

Two consequences are worth stating explicitly, because both were once wrong.

**An anonymous caller is never `Denied`.** Telling someone who has not signed in
that a surface "is not available for your account" is wrong on its face; the
honest answer is "sign in". Anonymous always yields
`AuthenticationRequired`.

**A gate never reports `Allowed` for something the caller cannot do.** Inviting
a user into a surface that will refuse them from the server is strictly worse
than an honest disabled entry, because the refusal arrives later and with less
context. A shared conformance test enumerates every registered gate by
reflection and asserts all four states across an identity matrix, so a plugin
added later is covered without editing the guard.

## Denials state a remedy

A refusal always says what would fix it. The gate supplies the missing
permission and the audience to ask as structured data, and the console renders
them:

```text
Requires the Backup permission - ask a platform administrator
```

not

```text
Backups is not available for your account
```

The difference matters. The second names the area, which the user can already
see on the entry they just clicked, and gives them nothing to act on. Where a
gate declares no remedy the console falls back to a general one; a refusal with
no remedy at all is the failure this path exists to prevent.

Remedies are delivered through the console's help primitive, which is
keyboard-focusable and screen-reader-associated, not through a `title`
attribute that is invisible on touch and unreachable by keyboard.

## Absences are explained, not merely absent

`Unavailable` renders no entry, because an area the cluster does not serve is
not something the caller can ever be granted. But an absence with no
explanation is indistinguishable from a bug, so the rail carries a capabilities
affordance answering "why can I not see everything?", naming the areas this
cluster does not serve.

Telemetry is the common case: a cluster with no telemetry backend configured
serves no Telemetry area, and a user should be able to find that out rather than
wonder.

## Hiding what you cannot use

Some operators would rather not see entries they cannot open. A preference
hides inaccessible entries, and **defaults to showing them**, because
discoverability is the more useful default for someone learning the product.
The choice persists like any other view preference.

## See also

- [The Explorer navigation model](navigation-model.md)
- [Writing an Explorer plugin](writing-a-plugin.md)
- [Managing access](managing-access.md)
