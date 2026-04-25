using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IMutationObserver"/> registered by the replication package.
/// Captures every locally-originating mutation at commit time, builds a
/// fully-formed <see cref="ReplogEntry"/> (op + key + value + HLC + origin
/// + TTL), and forwards it to the registered <see cref="IReplogSink"/>
/// before the originating grain''s write returns. Replaces the host-level
/// outgoing-call filter used by the legacy <c>MultiSiteManufacturing</c>
/// sample - capture is now atomic with the write.
/// </summary>
/// <remarks>
/// <para>
/// The observer fires on the grain's scheduler. The sink call is
/// awaited inline, so any latency in <see cref="IReplogSink.WriteAsync"/>
/// is added to the caller's write latency. The default no-op sink is
/// O(1).
/// </para>
/// <para>
/// <see cref="LatticeMutation.OriginClusterId"/> is preserved verbatim
/// when the mutation already carries an origin (i.e. it is a replay of a
/// remote write). When the mutation is local-origin (<c>OriginClusterId</c>
/// is <c>null</c>), the observer stamps the configured local
/// <see cref="LatticeReplicationOptions.ClusterId"/>; the registered
/// <c>LatticeReplicationOptionsValidator</c> guarantees that value is
/// non-empty before any observer call can reach this code path.
/// </para>
/// </remarks>
internal sealed class ReplicationMutationObserver(
    IReplogSink sink,
    IOptionsMonitor<LatticeReplicationOptions> options) : IMutationObserver
{
    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        var op = mutation.Kind switch
        {
            MutationKind.Set => ReplogOp.Set,
            MutationKind.Delete => ReplogOp.Delete,
            MutationKind.DeleteRange => ReplogOp.DeleteRange,
            _ => throw new InvalidOperationException(
                $"Unknown mutation kind: {mutation.Kind}"),
        };

        // The mutation already carries an origin when it is a replay of a
        // remote write; otherwise stamp the validated local cluster id.
        // LatticeReplicationOptionsValidator guarantees ClusterId is non-empty
        // before any observer call can reach this point.
        var origin = mutation.OriginClusterId ?? options.CurrentValue.ClusterId;

        var entry = new ReplogEntry
        {
            TreeId = mutation.TreeId,
            Op = op,
            Key = mutation.Key,
            EndExclusiveKey = mutation.EndExclusiveKey,
            Value = mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = origin,
        };

        return sink.WriteAsync(entry, cancellationToken);
    }
}

