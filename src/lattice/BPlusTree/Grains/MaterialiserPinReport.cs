using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A single leaf-materialiser pin report: the stable
/// <see cref="ConsumerId"/> and the highest durable checkpoint
/// <see cref="Frontier"/> that consumer has reached. Carried in the batch
/// <see cref="IWalMaterialiserPinGrain.ReportManyAsync"/> /
/// <see cref="IWalMaterialiserPinGrain.SeedManyAsync"/> contracts so a leaf
/// can mirror or seed every WAL partition's pin in a single grain round-trip
/// rather than one call per partition.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalMaterialiserPinReport)]
internal readonly record struct MaterialiserPinReport
{
    /// <summary>
    /// The stable leaf-materialiser consumer id, of the form
    /// <c>{MaterialiserConsumerIdPrefix}{treeName}_{leafGrainId}</c>
    /// (optionally partition-suffixed). Must not be <see langword="null"/>
    /// or whitespace.
    /// </summary>
    [Id(0)]
    public string ConsumerId { get; init; }

    /// <summary>
    /// The highest <see cref="HybridLogicalClock"/> the consumer has durably
    /// checkpointed. <see cref="HybridLogicalClock.Zero"/> seeds a "block"
    /// pin for a leaf that has activated but never checkpointed.
    /// </summary>
    [Id(1)]
    public HybridLogicalClock Frontier { get; init; }

    /// <summary>
    /// Creates a pin report.
    /// </summary>
    /// <param name="consumerId">The stable leaf-materialiser consumer id.</param>
    /// <param name="frontier">The highest durable checkpoint frontier.</param>
    public MaterialiserPinReport(string consumerId, HybridLogicalClock frontier)
    {
        ConsumerId = consumerId;
        Frontier = frontier;
    }
}
