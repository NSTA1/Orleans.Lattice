# Schema enforcement and versioning (`Orleans.Lattice.Schema`)

Orleans.Lattice stores every value as an opaque `byte[]`: the silo attaches no
schema to a value, and typed access is a client-side convenience. That keeps the
core fast and format-agnostic, but it also means the cluster cannot, on its own,
stop a caller writing a malformed value or tell a v1 value from a v2 one.

The companion **`Orleans.Lattice.Schema`** package closes that gap with
independent, composable, strictly opt-in capabilities:

- **Schema enforcement** - per-tree, server-side validation of the values a
  tree's write operations carry, against a declarative policy (JSON
  well-formedness, UTF-8, a maximum byte length, a regex, or a structured
  predicate over a JSON document). A rejected local write fails fast; in strict
  mode a rejected *ingested* item that reaches the check (a replicated
  typed-CRDT entry, or an entry of a replicated atomic batch) is dead-lettered
  rather than applied to the governed tree. The dead-letter write is awaited;
  storage failure or cancellation can still fail that intercepted write. A plain (non-atomic)
  last-writer-wins replication apply, a backup restore and a tree merge write
  below the check, so their values are not validated (see
  [strict-mode ingest](schema-enforcement.md#strict-mode-ingest)). Existing
  data can be brought into compliance by a background, crash-safe
  shadow-build-and-cutover remediation.
- **Schema versioning** - a self-describing, per-value version tag (schema id +
  version) that lets a tree evolve its value shape over time. Stale values are
  upcast to the tree's target version at read time; the target version advances
  monotonically as an admin action.

The capabilities share one serializable value-transform primitive
([`LatticeValueTransform`](value-transforms.md)) and the same dead-letter queue,
which is surfaced read-only through the State API and the Explorer UI. The remote
schema facade starts remediations, eager migrations, and compliance scans as
accept-then-poll operations so a large tree can continue after the caller that
started it times out or closes.

## Zero overhead when off

Neither feature costs anything until a tree opts in. With the package
unregistered, the core write interceptor and value decoder are null
implementations and the read/write path is byte-for-byte identical to a plain
lattice. Even with the package registered, a tree with no policy and no version
config pays only one cached lookup per registered feature on write and a single
leading-byte check on read, and its stored bytes keep their exact steady-state
shape.

## Getting started

Register the feature(s) you want on the silo, after `AddLattice`:

```csharp verify
using Orleans.Lattice.Schema;

// Enforcement: per-tree policies are installed afterwards through
// ILatticeSchemaAdmin; StrictIngest is the global half of strict-mode ingest.
siloBuilder.AddLatticeSchemaEnforcement(options =>
{
    options.StrictIngest = true;
});

// Versioning: declare the schema family and its upcasters.
siloBuilder.AddLatticeSchemaVersioning(registry =>
{
    registry.AddSchema(schemaId: 1, version: 1, name: "order");
    registry.AddSchema(schemaId: 1, version: 2, name: "order");
    registry.AddUpcaster(
        schemaId: 1,
        fromVersion: 1,
        toVersion: 2,
        transform: LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember(
                "status", LatticeValueTransform.Const(LatticeConstant.Text("open")))));
});
```

> When both features are used, call `AddLatticeSchemaEnforcement` **before**
> `AddLatticeSchemaVersioning` so the enforcement validation stage is composed
> ahead of the versioning envelope stage on the write path. The order is not
> checked at registration: calling `AddLatticeSchemaEnforcement` second replaces
> the composed write interceptor with the enforcement stage alone, so new writes
> are no longer stamped with a version envelope.

To layer further option delegates after registration, use
`ConfigureLatticeSchemaEnforcement(Action<LatticeSchemaEnforcementOptions>)` and
`ConfigureLatticeSchemaVersioning(Action<LatticeSchemaVersioningOptions>)`.

## Previewing a policy

`LatticeSchemaPolicyValidator` checks values against a `LatticeSchemaPolicy`
exactly as enforcement does, without writing anything. It compiles the rules once,
with the same checks setting the policy runs, so a rule that could not be set (a
structurally invalid rule, a negative maximum byte length, or a pattern that does
not compile) throws `ArgumentException` from its constructor. `Validate` then
judges a value against every rule in order and returns `null` when it complies, or
the first failing rule's reason. `ValidateRule` judges it against one rule by its
zero-based position, and throws `ArgumentOutOfRangeException` for a position
outside the policy; `RuleCount` and `Policy` report what the validator was built
from. A console or a tool uses it to preview a draft policy against sample
values before setting it; the Explorer's schema rule builder does exactly that.

```csharp verify
using Orleans.Lattice.Schema;

var policy = new LatticeSchemaPolicy(
[
    LatticeSchemaRule.Json(),
    LatticeSchemaRule.Structured(
        LatticePredicateNode.TypeOf("id", LatticeValueKind.Present),
        description: "id is required"),
]);

var validator = new LatticeSchemaPolicyValidator(policy);

// null when the value complies; otherwise the first failing rule's reason.
string? reason = validator.Validate(Encoding.UTF8.GetBytes("""{"name":"widget"}"""));

// Judge one rule on its own, by its position in policy.Rules.
string? idRule = validator.ValidateRule(1, Encoding.UTF8.GetBytes("""{"id":"a1"}"""));
```

The validator judges the bytes it is given. For a tree that also uses schema
versioning, strip the version envelope from a stored value that carries one first
(`LatticeSchemaEnvelope.IsEnveloped`, then `LatticeSchemaEnvelope.StripToBody`,
which removes the header length without checking for it), because a policy judges
the body.

## Documents

| Document | What it covers |
|---|---|
| [Public API](api.md) | Public services, models, declared members and overloads. |
| [Configuration](configuration.md) | Every option/default and registration-order constraint. |
| [Architecture](architecture.md) | Interception, version decoding, control and remediation pipelines. |
| [Chaos tests](chaos-tests.md) | The concrete concurrency/fault scenarios in the schema suite. |
| [Schema enforcement](schema-enforcement.md) | Per-tree policies, rule kinds, strict-mode ingest, background remediation. |
| [Schema versioning](schema-versioning.md) | The per-value version envelope, read-time upcasting, monotonic target-version advance. |
| [Value transforms](value-transforms.md) | The shared `LatticeValueTransform` IR used by remediation and upcasters. |
| [Dead-letter queue](dead-letter-queue.md) | Strict-mode dead-lettering and how to inspect it via the State API and Explorer. |
| [Wire format](wire-format.md) | The frozen per-value version envelope header layout. |

## Capability gate

The in-process admin services this package registers (`ILatticeSchemaAdmin`,
`ILatticeSchemaVersionAdmin`, and `ILatticeSchemaRemediationAdmin`) are trusted,
host-side surfaces: they perform no authorization of their own, and they read
and write the package's reserved `sys-schema-*` trees (and, for a remediation or
a migration, the governed tree itself) as system origin, so any code holding the
service can change a tree's schema. `LatticeSchemaReservedTrees` names those trees
(`PolicyTreeId`, `DeadLetterTreeId`, `VersionConfigTreeId`) and their `Prefix`,
and lets an application check its own tree ids against the reserved namespace
(`IsReserved`, `ThrowIfReserved`). The `LatticeOperation.SchemaAdmin` capability
is enforced by the remote schema control facade,
[`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md), which authorizes
every call fail-closed before it touches these services: SchemaAdmin for
mutations (setting or clearing a policy, changing or advancing a version config,
migrating, remediating) and ordinary Read for inspection. With the
[security](../lattice/security.md) layer enabled, schema control-plane actions
reached through that facade can therefore be granted independently of ordinary
data-plane read/write rights. The compliance audit
(`ILatticeSchemaComplianceAdmin`) reads the tree through the ordinary data plane,
so its scan is subject to the caller's Read authority.
