namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persisted state of the <see cref="Grains.IWalOffsetConsumerRegistryGrain"/>:
/// the offset-reading WAL consumers of one physical tree (issue #4579).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalOffsetConsumerRegistryState)]
internal sealed class WalOffsetConsumerRegistryState
{
    /// <summary>The registered consumer grains, in registration order, without duplicates.</summary>
    [Id(0)]
    public List<GrainId> Consumers { get; set; } = [];
}
