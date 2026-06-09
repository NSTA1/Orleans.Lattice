namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Result of <see cref="IWalShardGrain.QuiesceForMoveAsync"/>. Reports whether
/// the shard fenced for the move and, when it did, the stable highest offset the
/// move coordinator should copy from the source provider.
/// </summary>
/// <param name="Quiesced">
/// <see langword="true"/> when the fence was raised and the shard drained;
/// <see langword="false"/> when the activation's resolved placement version did
/// not match the coordinator's expected version (the coordinator must abort and
/// re-read placement).
/// </param>
/// <param name="HighestOffsetInclusive">
/// The highest durable offset on the source provider after the drain, or
/// <c>-1</c> when the shard is empty. Meaningful only when <paramref name="Quiesced"/>
/// is <see langword="true"/>.
/// </param>
/// <param name="ObservedPlacementVersion">The placement version the activation resolved its provider against.</param>
/// <param name="ProviderKey">The catalog key the activation's provider was resolved under.</param>
[GenerateSerializer]
[Alias(TypeAliases.WalMoveQuiesceResult)]
[Immutable]
internal readonly record struct WalMoveQuiesceResult(
    [property: Id(0)] bool Quiesced,
    [property: Id(1)] long HighestOffsetInclusive,
    [property: Id(2)] long ObservedPlacementVersion,
    [property: Id(3)] string ProviderKey);
