using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable persisted state of the <see cref="IWalMaterialiserPinGrain"/>: the
/// per-consumer leaf-materialiser checkpoint frontiers for a single tree.
/// <para>
/// Each entry maps a leaf-materialiser <c>consumerId</c> (the same stable id
/// the leaf reports to the in-memory <see cref="IWalCursorRegistry"/>, of the
/// form <c>{MaterialiserConsumerIdPrefix}{treeName}_{leafGrainId}</c>, optionally
/// partition-suffixed) to the highest <see cref="HybridLogicalClock"/> that
/// leaf has durably checkpointed. The map survives a full silo/cluster restart
/// so the WAL GC can floor its trim point under the slowest leaf's durable
/// checkpoint even before that leaf has re-activated and re-reported into the
/// process-local registry.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalMaterialiserPinState)]
internal sealed class WalMaterialiserPinState
{
    /// <summary>
    /// The durable leaf-materialiser pins for this tree, keyed by the
    /// leaf's stable consumer id. The stored value is each leaf's highest
    /// durable checkpoint frontier; <see cref="HybridLogicalClock.Zero"/>
    /// marks a leaf that has activated but never checkpointed (a "block"
    /// pin that keeps the WAL head retained for that leaf until it
    /// advances).
    /// </summary>
    [Id(0)]
    public Dictionary<string, HybridLogicalClock> Pins { get; set; } =
        new(StringComparer.Ordinal);
}
