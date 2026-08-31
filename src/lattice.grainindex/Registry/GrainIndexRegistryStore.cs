namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// The default <see cref="IGrainIndexRegistryStore"/>, backed by the internal
/// registry <see cref="ILattice"/> tree.
/// <para>
/// Each index's record is one value under
/// <see cref="GrainIndexRegistryKeys.Definition(string)"/>, so reading a
/// declaration is a single point read and listing every index is one contiguous
/// range scan.
/// </para>
/// </summary>
/// <remarks>
/// Every access runs inside a <see cref="LatticeSystemOrigin"/> scope. The
/// registry is index infrastructure rather than user data: it must be readable
/// and writable during silo start regardless of which caller identity, if any, a
/// host's access gate would otherwise demand, and it is never addressed on a
/// user's behalf.
/// </remarks>
internal sealed class GrainIndexRegistryStore : IGrainIndexRegistryStore
{
    private readonly ILattice _registry;
    private readonly ILatticeSerializer<GrainIndexRegistryRecord> _serializer;

    /// <summary>Initialises a new store.</summary>
    /// <param name="grainFactory">Opens the registry tree. Must not be <c>null</c>.</param>
    /// <param name="serializer">Encodes and decodes the persisted record. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexRegistryStore(
        IGrainFactory grainFactory,
        OrleansGrainIndexSerializer<GrainIndexRegistryRecord> serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);

        // The tree id is a constant, and a grain reference is an immutable
        // proxy, so it is resolved once here rather than on every operation.
        // Later work puts the activation-path "seen" markers in this same tree,
        // where a per-call resolution would sit on a real hot path.
        _registry = grainFactory.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);
        _serializer = serializer;
    }

    /// <inheritdoc />
    public async Task<GrainIndexRegistryRecord?> ReadAsync(
        string indexName,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        using (LatticeSystemOrigin.Enter())
        {
            return await _registry
                .GetAsync(GrainIndexRegistryKeys.Definition(indexName), _serializer, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task WriteAsync(
        string indexName,
        GrainIndexRegistryRecord record,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(record);

        using (LatticeSystemOrigin.Enter())
        {
            await _registry
                .SetAsync(GrainIndexRegistryKeys.Definition(indexName), record, _serializer, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
