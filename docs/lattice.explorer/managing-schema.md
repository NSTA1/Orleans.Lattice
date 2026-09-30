# Managing schema from the Explorer

The **Schema** area is a native compiled-in Explorer area. No extra registration is needed. It appears when the schema control facade's capability probe grants the caller at least one schema capability.

Schema is tenant-scoped. In a tenant-rooted Explorer, the same address under `/t/{tenant}` scopes the caller-supplied tree name through the active tenant before the cluster authorizes or acts on it.

## Availability

Schema is visible when its fail-closed capability probe returns any schema grant. The probe names a reserved sentinel tree and has no side effects. A definite visible or refused answer is remembered until the sign-in state or connection changes. A transport fault is not remembered, so the next navigation asks again.

The area is hidden when the schema facade is absent, when a signed-in identity receives no grant, when the probe is denied, when the cluster does not serve schema administration, or when the connection is unavailable.

An anonymous caller whose probe returns no grants sees the area as unavailable. The exact sentence shown is:

> Sign in to manage schema on this cluster.

Per-tree grants are probed separately and reused briefly. If the caller has no grant on a selected tree, the tree page says:

> You may not manage this tree's schema

Panel-level denials are shown beside the panel, for example **You may not read this tree's policy**, **You may not scan this tree**, or **You may not read this tree's dead letters**.

## Addresses

The Schema area is tenant-scoped. These route forms exist in the shipped pages:

| Page | Plain address | Tenant-rooted form | Notes |
| --- | --- | --- | --- |
| Schema directory | `/schema` | `/t/{tenant}/schema` | Lists governed trees by default. Use `?show=all` to include ungoverned trees and `?filter={text}` to filter by tree id. |
| Tree workspace | `/schema/{tree-path}` | `/t/{tenant}/schema/{tree-path}` | Supports tree ids carried in up to six path segments after `/schema`. The tab is selected with `?tab=...`. |

The area uses these query keys:

- `show=all` lists every tree, not only trees with schema state.
- `filter={text}` filters the directory by tree id.
- `tab=policy|versions|compliance|remediation|dead-letters` selects a tree-workspace tab. Missing or unknown values fall back to `policy`.
- `scan=start` on the compliance tab starts one read-only compliance scan, then the page removes the key from the address so refresh or history navigation does not start another scan.

## Directory page

The directory page shows the logical trees under a schema policy, version config, or app declaration. The **Under schema** and **All trees** links switch between governed trees and every logical tree. The filter narrows by tree id. **Refresh** reads again. **Scan compliance...** opens a picker listing visible trees that have a policy.

Each row shows:

- the logical tree id, linking to `/schema/{tree}`;
- policy state: a policy summary, **None**, **Not permitted**, **Not available**, or **Could not read**;
- versioning state: the target version summary, **Unversioned**, **Not permitted**, **Not available**, or **Could not read**. A version config whose target version is 0, the reserved unversioned value, counts as **Unversioned**, here and on the Versions tab;
- the last compliance scan result recorded in this Explorer circuit;
- the declaring app, when an installed app manifest declares the tree's schema.

A declaring app link goes to `/apps/{slug}` in the same tenant. A row with a policy has a **Scan compliance** action linking to `/schema/{tree}?tab=compliance&scan=start`.

The directory shows a truncated note when the cluster holds more trees than one bounded listing inspects. A tree can still be opened directly by address.

## Tree workspace and tabs

A tree workspace heading shows the logical tree id, badges for policy and version state, and app declaration metadata when present. The declaring app line links to `/apps/{slug}` and names the app version, schema family, schema version, and strict-ingest flag.

The workspace has five tabs.

### Policy

The Policy tab reads, sets, edits, and clears the tree's write-validation policy. A tree with no policy accepts every value. A value must satisfy every rule. Rules are written with the rule builder below.

A policy needs at least one rule, so **Save policy** is disabled until there is a rule, or one being written, with a note saying so; to accept every value, clear the policy instead. Saving replaces the whole policy and affects new writes immediately. Existing stored values are not changed.

#### Rule builder

The rule builder writes the policy as a list of plain-language rules, each read back as a sentence such as "total must be a number between 0 and 10,000". Rules can be edited, moved up or down, and removed. **Add a rule** opens a composer with three steps:

1. **What it checks.** Pick a member from the tree's shape, or type its dotted path, such as `order.total`; leave it empty to check the whole value. The shape is inferred from a sample of the tree's values and the members the policy already names: each member shows the kinds of value seen in the sample. It is bounded (8 levels deep, 64 members per object, 400 members in all), so a very large value shows only part of its shape.
2. **What it must be.** Pick a card from the gallery. Each card shows a live example drawn from the sample for the chosen member, such as "Seen 9.99 to 1,210." A card that does not fit is disabled with the reason.
3. **Details.** Fill in the card's settings. The composer reads the rule back as a sentence and says how many sampled values it passes.

The gallery's cards, and what each one compiles to:

| Card | Checks | Compiles to |
| --- | --- | --- |
| Required | The member is there and not null. | A structured rule. With "It holds an object or a list" on, a presence test (`TypeOf` `Present`) that accepts any value. Off, a "not null" comparison, which older clusters also understand but which reads an object or a list as missing, so the card then says "must be present as text, a number or true or false". The switch is turned on for a member the sample showed holding an object or a list, and its example counts only the values the chosen form accepts. |
| Type | Text, a number, true or false, an object or a list. | A structured rule. Text uses a string test, number and true-or-false use comparisons, and object and list use a `TypeOf` test. |
| One of a set | Only the listed values. "Compare as numbers" treats `5` and `5.0` as the same value. | A structured rule: an "or" of equality comparisons. |
| Number range | A smallest and largest number, optionally whole numbers only. | A structured rule: comparisons, plus a `TypeOf` `Integer` test for whole numbers only. |
| Text length | The fewest and most characters. | A structured rule: a `TypeOf` `String` test and comparisons on `LengthOf`. |
| Common format | An email address, URL, UUID, ISO date, time or date and time, IPv4 or IPv6 address, slug, country or currency code, hex colour, semantic version or E.164 phone number. | A regular-expression rule on the member. |
| Starts, ends or contains | A prefix, a suffix or some text anywhere. | A structured rule: a string test. |
| List length | The fewest and most items. | A structured rule: a `TypeOf` `Array` test and comparisons on `LengthOf`. |
| Every item | Each item of a list satisfies another card. | A structured rule: an `Every` quantifier over the list. |
| Custom pattern | A regular expression, with a live tester. | A regular-expression rule on the member or the whole value. |
| Well-formed value | The whole value is well-formed UTF-8, or one JSON document. | A UTF-8 or JSON rule. Whole value only. |
| Largest size | The whole value is at most a number of bytes. | A size rule. Whole value only. |

Cards combine in three ways:

- **Every item.** The card nests another card, which checks each item; an empty path there means the item itself. Only cards that compile to a structured predicate can be nested, so a format, pattern or whole-value card cannot sit inside it, and a list must be a list for the card to pass.
- **Any of.** **Or...** on a rule adds an alternative, and the rule then passes when at least one alternative holds. Alternatives are structured-predicate cards, so a format, pattern or whole-value card cannot be one.
- **Also accept a missing value.** This toggle makes a card optional: a missing or null member passes, and a present one must satisfy the card. It is offered on structured-predicate cards other than Required and an "any of" group, so not on a format, pattern or whole-value card.

A card can also carry an optional message, which enforcement reports as the violation reason when a value fails it. A rule the builder cannot express as a card, such as one written through the API, is kept exactly as it is and shown as a read-only **Custom rule** card marked "kept as it is". It can be moved or removed, but not edited.

The Common format and Starts, ends or contains cards show the same check as a regular expression. On a top-level rule, **Edit as regex** turns the card into a Custom pattern card holding that expression, so it can be adjusted by hand.

The structured cards use the [structural predicate kinds](../lattice/predicated-operations.md#structural-predicate-kinds). The builder combines checks only with *and* and *or*, so during a rolling upgrade a silo that does not know a structural kind rejects a value a newer silo would accept, never the reverse. The Required card avoids the structural kinds unless the member holds an object or a list.

#### Checking against a sample

Beside the rules, **Check against a sample** reads one page of the tree's values, the first 100 in key order. A value too large for the page's preview is read in full, and one that cannot be read is skipped and counted. Nothing is written, and the read's cursor is released straight away. For a versioned tree the version envelope is stripped first, because a policy judges the body.

The draft is checked locally, with the cluster's own `LatticeSchemaPolicyValidator`, so the result agrees with what enforcement and a compliance scan would say about the same values. The panel shows how many sampled values pass, how many fail each rule, and the first failing values with the rule and its reason. **Read the sample again** refreshes it. The sample is a preview only; the full scan is on the [Compliance](#compliance) tab.

If sampled values would fail the draft, **Save policy** asks first, in a dialog named **Some sampled values would not comply**. It explains that saving does not change stored values, links to **Plan a remediation** on the Remediation tab, and offers **Save anyway** or **Keep editing**. A rule still open in the composer is added before saving, or the save stops and says what to finish.

#### Advanced view

The **Advanced** switch shows the exact policy as JSON, as the cluster will store it, beside the raw rule editor, which adds the four basic rule shapes directly: well-formed UTF-8, one JSON document, a largest size, and a regular expression, optionally on a member. Turning it off returns to the rules as sentences.

The member-path fields in the builder and the raw editor are [pickers](navigation-model.md#pickers) that suggest the members of the inferred shape and accept any path, because a value's members are not known to the cluster until a rule names them.

**Strict ingest** controls whether replicated and restored values are checked too. When it is on, a value that fails strict ingest is diverted to dead letters instead of being applied.

Clearing a policy uses a destructive confirmation named **Clear this tree's policy**. The confirmation states that every value will be accepted from then on, strict ingest stops diverting values, and the rules are not kept.

### Versions

The Versions tab manages the tree's envelope-version config. A version is a stamp, not a shape: new writes are stamped with the schema family and target version, and older values are upgraded when they are read or migrated. The Explorer does not show the shape difference between versions because those registrations live in code on the silos.

When versioning is off, **Turn on versioning** asks for a schema family, a target version of 1 or more, and strict ingest. When versioning is on, the tab can:

- **Advance target version...** after a confirmation named **Advance to version N**;
- **Advance and migrate...** after a confirmation named **Advance to version N and migrate**;
- **Migrate stored values...** after a review dialog;
- **Change config** by replacing the config as typed;
- **Turn off versioning** after a destructive confirmation named **Turn off versioning**.

Advancing can only move the target version up. Migration and advance-and-migrate are staged background operations. The tab links to the Remediation tab while one is running.

If the cluster has not registered schema versioning, the tab says **Versioning is not available** instead of failing opaquely.

### Compliance

The Compliance tab runs a read-only scan of every value in the tree against the current policy. The scan changes nothing. It can be started from the tab, from the directory picker, or by the address `?tab=compliance&scan=start`.

While a scan runs, the tab shows **Scanning every value of {tree}...** and a **Stop scanning** control. Stopping a scan reports that it was stopped before it finished. A finished scan shows scanned, compliant, and non-compliant counts, finish time, and a breakdown of non-compliance reasons. The last scan result is kept for this Explorer circuit and summarised in the directory.

If the tree has no policy, the tab says there is nothing to scan against and links back to the Policy tab.

### Remediation

The Remediation tab is the status page for a tree's background migration or remediation, and the place to start a remediation when the caller may manage schema.

A remediation rewrites every value through ordered transform steps, checks each rewritten value against the current policy, and cuts over only when every value passes. If a value still fails, nothing is cut over. The editor can add only top-level member steps: set, remove, and rename. A step's member is a [picker](navigation-model.md#pickers) that suggests the member paths the tree's policy already names and accepts any path. A rename's new name is a plain name box: it accepts any path, and flags one the policy already names ("The policy already names a member with this name.") without refusing it. Conditional or computed transforms are registered in code on the silos and are not authored here.

Starting a remediation opens a destructive confirmation named **Remediate this tree**. It says every value is rewritten and checked, that a successful run cuts over to the rewritten values, and that a failed value leaves the tree unchanged. The operation runs in the background and the page can be left while it runs.

The status section reads the cluster's own remediation status and also shows operations started in the current circuit. It displays stages (**Confirmed**, **Running in the cluster**, **Finished**), operation id when present, values checked, aborted-key detail, failure text, **Refresh status**, and **Clear this result** for a finished circuit operation. Running status is read again every 2 seconds.

### Dead letters

The Dead letters tab counts strict-mode dead letters on arrival and lists entries on demand because the queue can be large. It is read-only: there is no replay or delete action here.

**Load dead letters** reads the first 100 entries. **Load more dead letters** increases the limit by another 100. Rows show key, reason, source, diverted time, and value size. The detail view shows the full key and a text preview of the rejected value's first bytes.

## Palette and address completions

Schema contributes these commands:

| Command id | Label | Target |
| --- | --- | --- |
| `schema.scan-compliance` | `Scan compliance...` | `/schema` and opens the picker on the directory page |
| `schema.all-trees` | `Show every tree's schema` | `/schema?show=all` |

The visible controls carry the same command ids: the **Scan compliance...** button and the **All trees** link.

Address completions list governed trees from the remembered directory read. Search mode matches tree ids; app mode matches declaring app slugs; address mode matches `/schema/...` paths. Completions stay in the current tenant and include details such as **Schema policy**, **Schema versioning**, **Schema policy and versioning**, or **Schema declared by an app**.

## Limits and caching

- The tree catalogue is cached for 30 seconds and follows at most 20 catalogue pages per read.
- A directory listing inspects at most 500 logical trees at a time and probes up to 8 trees concurrently.
- The directory read is cached for 30 seconds.
- Per-tree grants are cached for 30 seconds unless a refresh is requested.
- Dead-letter reads load 100 entries at a time.
- Running operation status is re-read every 2 seconds while it is running.
- Compliance scan results are remembered only in the current Explorer circuit.
- The rule builder's sample is the first 100 values in key order, and its inferred shape stops at 8 levels, 64 members per object and 400 members in all.

## Server authority

The schema control facade scopes caller-supplied tree names through the active tenant, then authorizes and acts on the same effective tree. Read authority gates policy reads, version reads, remediation status, dead-letter reads, and compliance scans. Schema-admin authority gates policy changes, version changes, and remediation. Version operations require the versioning add-on on the silo.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Area availability](area-availability.md)
- [Areas reference](areas.md#schema)
- [Lattice Apps](lattice-apps.md)
- [Schema engine](../lattice.schema/README.md)
- [Schema API](../lattice.api.schema/README.md)
- [Schema gRPC binding](../lattice.api.schema.grpc/README.md)