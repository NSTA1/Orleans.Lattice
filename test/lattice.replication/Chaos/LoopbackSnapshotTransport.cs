using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// In-process <see cref="IRemoteSnapshotTransport"/> for
/// <see cref="ProductionShipperFixture"/>: a receiver's bootstrap export is
/// served by the source site's own <see cref="LatticeRemoteSnapshotService"/>,
/// across the same edge <see cref="LoopbackReplicationTransport"/> ships over.
/// </summary>
/// <remarks>
/// A partition of that edge cuts the export as it would cut a real stream:
/// a request made while the edge is isolated fails, and a stream fails at its
/// next entry once the edge has been isolated at any point since it opened -
/// even if it has healed again since - so no entry read during a partition is
/// delivered after it. Both raise <see cref="IOException"/>, which the
/// bootstrap retries as a transient fault.
/// </remarks>
internal sealed class LoopbackSnapshotTransport(LoopbackTransportRegistry registry, string localClusterId)
    : IRemoteSnapshotItemTransport
{
    /// <inheritdoc />
    public Task<RemoteSnapshotMetadata> GetMetadataAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default)
    {
        var (service, _) = Open(sourceClusterId);
        return service.GetMetadataAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (service, generation) = Open(sourceClusterId);
        await foreach (var entry in service
            .RequestSnapshotAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken)
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            EnsureUncut(sourceClusterId, generation);
            yield return entry;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (service, generation) = Open(sourceClusterId);
        await foreach (var item in service
            .RequestSnapshotItemsAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken)
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            EnsureUncut(sourceClusterId, generation);
            yield return item;
        }
    }

    private (IRemoteSnapshotItemTransport Service, long Generation) Open(string sourceClusterId)
    {
        var edge = registry.Get(sourceClusterId);
        var generation = edge.PartitionGenerationOf(localClusterId);
        if (edge.IsIsolatedFrom(localClusterId))
        {
            throw new IOException(
                $"Loopback snapshot transport: the edge from {sourceClusterId} to {localClusterId} is partitioned.");
        }

        var service = registry.GetSnapshotService(sourceClusterId)
            ?? throw new IOException($"Loopback snapshot transport: no snapshot service registered for {sourceClusterId}.");
        return (service, generation);
    }

    private void EnsureUncut(string sourceClusterId, long openedAtGeneration)
    {
        var edge = registry.Get(sourceClusterId);
        if (edge.IsIsolatedFrom(localClusterId) || edge.PartitionGenerationOf(localClusterId) != openedAtGeneration)
        {
            throw new IOException(
                $"Loopback snapshot transport: a partition of the edge from {sourceClusterId} to {localClusterId} cut the export stream.");
        }
    }
}
