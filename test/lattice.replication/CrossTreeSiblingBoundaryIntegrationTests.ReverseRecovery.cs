using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5106: site A replicates the sibling back from site B, so the supported
/// bidirectional topology needs site B's real snapshot service as site A's bootstrap
/// source. Without it, a reseed request from site B can only be answered from site A's
/// own local view, which cannot restore what site B shipped.
/// </summary>
public partial class CrossTreeSiblingBoundaryIntegrationTests
{
    private static readonly ConcurrentDictionary<string, IRemoteSnapshotItemTransport> SiteBTransports = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, ReverseSnapshotCounter> ReverseSnapshotCounters = new(StringComparer.Ordinal);

    [Test]
    public Task Trim_before_reverse_registration_recovers_from_site_b_snapshot_with_auto_recovery_on() =>
        RunTrimBeforeReverseRegistrationAsync(recoveryEnabled: true, "xtsb-gap-recover-tree", "xtsb-gap-recover-sibling");

    private static ReverseSnapshotCalls ReadReverseSnapshotCalls(string tree) =>
        ReverseSnapshotCounters.TryGetValue(tree, out var counter)
            ? new ReverseSnapshotCalls(Volatile.Read(ref counter.Metadata), Volatile.Read(ref counter.Items))
            : default;

    private readonly record struct ReverseSnapshotCalls(int Metadata, int Items);

    private sealed class ReverseSnapshotCounter
    {
        public int Metadata;
        public int Items;
    }

    /// <summary>
    /// Site A's bootstrap transport. Resolves site B's real snapshot service, which is only
    /// created after site B deploys, and fails closed for any other source cluster.
    /// </summary>
    private sealed class SiteBSnapshotRelay : IRemoteSnapshotItemTransport
    {
        public Task<RemoteSnapshotMetadata> GetMetadataAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)
        {
            var source = Resolve(sourceClusterId);
            Interlocked.Increment(ref Counter(treeName).Metadata);
            return source.GetMetadataAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken);
        }

        public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var item in RequestSnapshotItemsAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken))
            {
                if (!item.IsTrailer)
                {
                    yield return item.Entry;
                }
            }
        }

        public async IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var source = Resolve(sourceClusterId);
            await foreach (var item in source.RequestSnapshotItemsAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken)
                .WithCancellation(cancellationToken))
            {
                if (!item.IsTrailer)
                {
                    Interlocked.Increment(ref Counter(treeName).Items);
                }

                yield return item;
            }
        }

        private static ReverseSnapshotCounter Counter(string treeName) =>
            ReverseSnapshotCounters.GetOrAdd(treeName, static _ => new ReverseSnapshotCounter());

        private static IRemoteSnapshotItemTransport Resolve(string sourceClusterId) =>
            sourceClusterId == SiteBClusterId && SiteBTransports.TryGetValue(sourceClusterId, out var transport)
                ? transport
                : throw new InvalidOperationException(
                    $"Site A has no snapshot transport for source cluster '{sourceClusterId}'.");
    }
}
