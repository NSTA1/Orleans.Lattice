using System.Text.Json.Serialization;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// System.Text.Json source-generated metadata for the closed-shape CRDT
/// state and delta types. Routing the closed-shape (de)serialisers through
/// this context (rather than the reflection-based
/// <see cref="JsonLatticeSerializer{T}"/>) removes the per-call reflection
/// metadata resolution from the CRDT delta-apply hot path
/// (<c>BPlusLeafGrain.ApplyCrdtDeltaAsync</c>), where the same
/// <c>(tree, mode)</c> shape is decoded and re-encoded on every mutation.
/// <para>
/// The generated metadata is wire-compatible with the reflection serialiser:
/// no naming policy, default options, and the same public-property discovery
/// rules, so the emitted JSON is byte-identical to the legacy path. This is
/// asserted by the conformance tests so the persisted byte[] rows and the
/// replication wire shape never drift when a tree's shape is resolved through
/// the source-generated lane.
/// </para>
/// <para>
/// Only the closed-shape modes (<see cref="LatticeMergeMode.OrSet"/>,
/// <see cref="LatticeMergeMode.PnCounter"/>,
/// <see cref="LatticeMergeMode.VersionVector"/>,
/// <see cref="LatticeMergeMode.MvRegister"/>) are source-generated; the
/// generic <see cref="LatticeMergeMode.OrMap"/> shape stays on the
/// reflection serialiser because it is open over host-supplied
/// <c>(TKey, TValue)</c> pairs the generator cannot enumerate at build time.
/// </para>
/// </summary>
[JsonSerializable(typeof(OrSet))]
[JsonSerializable(typeof(OrSetDelta))]
[JsonSerializable(typeof(PnCounter))]
[JsonSerializable(typeof(PnCounterDelta))]
[JsonSerializable(typeof(VersionVector))]
[JsonSerializable(typeof(VersionVectorDelta))]
[JsonSerializable(typeof(MvRegister))]
[JsonSerializable(typeof(MvRegisterDelta))]
[JsonSerializable(typeof(OrFlag))]
[JsonSerializable(typeof(OrFlagDelta))]
internal sealed partial class CrdtJsonSerializerContext : JsonSerializerContext
{
}
