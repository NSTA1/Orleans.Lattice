using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Decorator on the package's <see cref="IReplicationApplier"/> that
/// gates <i>inbound</i> apply on the operator-driven
/// <see cref="IReplicationDisconnectGrain"/> chaos flag - the inbound
/// counterpart to <see cref="ChaosReplicationTransport"/>, which
/// gates <i>outbound</i> ship.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this decorator is required.</b> The chaos
/// <c>ReplicationDisconnect</c> preset historically only paused
/// <i>outbound</i> ship: <see cref="ChaosReplicationTransport.SendAsync"/>
/// returned <see cref="ReplicationAck.Accepted"/> = <see langword="false"/>,
/// which keeps the local shipper's per-peer cursor stationary so the
/// peer never sees our writes while the flag is set. But the peer's
/// cursor was never stopped - the disconnect flag is local-only, and
/// the peer's own shipper happily pushed its writes to our inbound
/// gRPC service, where the package's gRPC <c>Push</c> method calls
/// <see cref="IReplicationApplier.ApplyBatchAsync"/> and merges them
/// onto the local tree. Net effect: the user clicked "Disconnect" on
/// site A, set data on site B, and watched it arrive on site A
/// anyway. This decorator closes that loop.
/// </para>
/// <para>
/// <b>How the gate works.</b> When the flag is set, every call to
/// <see cref="ApplyAsync"/> / <see cref="ApplyBatchAsync"/> throws
/// before delegating to the inner applier. The package's gRPC server
/// catches every non-cancellation exception out of the applier and
/// rethrows it as <c>StatusCode.Internal</c>, so the peer's
/// transport sees a non-success ack and does not advance its
/// per-peer cursor. The pending entries stay on the peer's WAL and
/// are re-shipped on the next push attempt once the flag is cleared.
/// This is the inbound mirror of <see cref="ChaosReplicationTransport"/>'s
/// outbound <c>Accepted = false</c> path: in both directions the
/// upstream side keeps the data and re-tries.
/// </para>
/// <para>
/// <b>Decorator ordering.</b> Registered <i>after</i>
/// <see cref="BaselineReplicationApplierRegistrationExtensions.AddBaselineReplicationApplierDecorator"/>
/// so this is the outermost layer the gRPC server sees. When the
/// flag is set we short-circuit before
/// <see cref="BaselineReplicationApplier"/> runs, so we don't
/// announce a fact-replicated event to the dashboard for an entry
/// that was actually rejected.
/// </para>
/// <para>
/// <b>Operator visibility.</b> Each rejected push surfaces as a
/// <c>StatusCode.Internal</c> ERROR log on the receiver and as a
/// shipper retry on the sender; this is the intended chaos signal
/// and matches what a real network partition would produce. The
/// chaos banner in the dashboard reflects the local flag state so
/// operators know the noise is expected.
/// </para>
/// </remarks>
internal sealed class ChaosReplicationApplier(
    IReplicationApplier inner,
    IGrainFactory grains,
    ILogger<ChaosReplicationApplier> logger) : IReplicationApplier
{
    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
    {
        await ThrowIfDisconnectedAsync(batchSize: 1).ConfigureAwait(false);
        return await inner.ApplyAsync(entry, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyBatchAsync(
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        await ThrowIfDisconnectedAsync(batchSize: entries.Count).ConfigureAwait(false);
        return await inner.ApplyBatchAsync(entries, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Consults the <see cref="IReplicationDisconnectGrain"/> singleton
    /// and throws if the chaos flag is set, so the package's gRPC
    /// service rethrows as <c>StatusCode.Internal</c> and the peer's
    /// transport does not advance its cursor. Exposed as
    /// <c>internal</c> so unit tests can drive the gate without
    /// standing up the full apply pipeline.
    /// </summary>
    internal async Task ThrowIfDisconnectedAsync(int batchSize)
    {
        bool disconnected;
        try
        {
            disconnected = await grains
                .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey)
                .IsDisconnectedAsync()
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // If we can't reach the chaos grain, fail open: let the
            // apply through. The alternative - failing closed - would
            // turn a transient grain-routing blip into a global
            // replication outage every time the chaos grain is in
            // motion across silos. Log so operators can correlate.
            logger.LogWarning(ex,
                "ChaosReplicationApplier: failed to read disconnect flag; allowing inbound apply to proceed.");
            return;
        }

        if (disconnected)
        {
            logger.LogDebug(
                "Chaos replication-disconnect active; rejecting inbound apply ({BatchSize} entr{Plural}) "
                + "so the peer does not advance its cursor.",
                batchSize, batchSize == 1 ? "y" : "ies");

            throw new InvalidOperationException(
                "Chaos replication-disconnect is active on this cluster; inbound apply rejected so "
                + "the peer's shipper does not advance its per-peer cursor. The pending entries will "
                + "be re-shipped once the disconnect flag is cleared.");
        }
    }
}
