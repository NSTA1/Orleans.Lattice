# TagIndexes

## What it shows

A tag index lets you attach arbitrary string tags to keys and then query the
keys back by tag. This sample tags four catalogue items and runs the two query
shapes: `WithAllTags(...)` (intersection / AND - a key must carry every tag) and
`WithAnyTags(...)` (union / OR - a key matches if it carries any tag).

## Run it

```
dotnet run --project samples/TagIndexes
```

## Expected output

```
Silo starting... ready.

== Seeding catalogue items ==
  wrote item:1
  wrote item:2
  wrote item:3
  wrote item:4

== Associating tags with keys ==
  item:1 += [red, round]
  item:2 += [red, square]
  item:3 += [blue, round]
  item:4 += [green, square]

== WithAllTags("red", "round") - intersection ==
  item:1
  count = 1  (expected item:1 only)

== WithAnyTags("red", "blue") - union ==
  item:1
  item:2
  item:3
  count = 3  (expected item:1, item:2, item:3)

Done. WithAllTags narrows (AND); WithAnyTags widens (OR).
```

## When to use

- Faceted lookups: find keys by category, label, or membership without scanning
  the whole tree. `WithAllTags` for "must match all facets", `WithAnyTags` for
  "match any facet".
- Secondary access paths over a primary key/value tree (e.g. items by colour,
  documents by label).

## When not to use

- Range or ordered queries. Tags are set membership, not sortable ranges - use a
  key-ordered scan for range access.

## Feature docs

[docs/lattice/api.md#tag-indexes](../../docs/lattice/api.md#tag-indexes)
