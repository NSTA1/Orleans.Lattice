using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// The default <see cref="IGrainIndexBackfillStore"/>, backed by the same
/// internal registry <see cref="ILattice"/> tree that holds the persisted index
/// definitions and the activation path's seen markers.
/// </summary>
/// <remarks>
/// <para>
/// One value per index under
/// <see cref="GrainIndexRegistryKeys.Checkpoint(string)"/>. The checkpoint
/// segment does not prefix, and is not prefixed by, any other kind's segment, so
/// a checkpoint can never be swept up by a scan of definitions, seen markers, or
/// outbox entries.
/// </para>
/// <para>
/// The tree reference is resolved once, in the constructor, and every access
/// runs inside a <see cref="LatticeSystemOrigin"/> scope: the registry is index
/// infrastructure that has to be readable and writable whatever caller identity
/// a host's access gate would otherwise demand, and it is never addressed on a
/// user's behalf.
/// </para>
/// </remarks>
internal sealed class GrainIndexBackfillStore : IGrainIndexBackfillStore
{
    private readonly ILattice _registry;
    private readonly ILatticeSerializer<GrainIndexBackfillCheckpoint> _serializer;

    /// <summary>Initialises a new store.</summary>
    /// <param name="grainFactory">Opens the registry tree. Must not be <c>null</c>.</param>
    /// <param name="serializer">Encodes and decodes the persisted checkpoint. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexBackfillStore(
        IGrainFactory grainFactory,
        OrleansGrainIndexSerializer<GrainIndexBackfillCheckpoint> serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);

        _registry = grainFactory.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);
        _serializer = serializer;
    }

    /// <inheritdoc />
    public async Task<GrainIndexBackfillCheckpoint?> ReadAsync(
        string indexName,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        using (LatticeSystemOrigin.Enter())
        {
            return await _registry
                .GetAsync(GrainIndexRegistryKeys.Checkpoint(indexName), _serializer, cancellationToken)
                .ConfigureAwait(true);
        }
    }

    /// <inheritdoc />
    public async Task WriteAsync(
        string indexName,
        GrainIndexBackfillCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(checkpoint);

        using (LatticeSystemOrigin.Enter())
        {
            await _registry
                .SetAsync(
                    GrainIndexRegistryKeys.Checkpoint(indexName),
                    checkpoint,
                    _serializer,
                    cancellationToken)
                .ConfigureAwait(true);
        }
    }
}
