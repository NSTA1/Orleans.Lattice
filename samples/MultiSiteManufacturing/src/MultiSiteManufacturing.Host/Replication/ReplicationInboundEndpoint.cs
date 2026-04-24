using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Maps the inbound <c>POST /replicate/{tree}</c> endpoint. Stamps
/// the <c>lattice.replay</c> <see cref="RequestContext"/> flag
/// before applying each entry so the outgoing call filter
/// (<see cref="LatticeReplicationFilter"/>) suppresses the replog
/// append for apply-from-peer writes — closes the A → B → A cycle
/// at the application layer without any library support.
/// </summary>
/// <remarks>
/// <para>
/// <b>Idempotency</b> is a consequence of the tree-level key
/// discipline, not of this endpoint. <c>mfg-facts</c> and
/// <c>mfg-site-activity-index</c> use write-once keys, so repeated
/// apply of the same entry is a no-op. <c>mfg-part-crdt</c> labels
/// are G-Set (idempotent by construction); the LWW-register half
/// has a known cross-cluster divergence risk (plan §13.7) — opt in
/// with that caveat.
/// </para>
/// <para>
/// <b>FUTURE seam.</b> When the library gains cross-tree continuous
/// merge, the receiver's apply loop becomes a merge invocation that
/// preserves the source HLC — at which point the LWW-register
/// caveat goes away.
/// </para>
/// </remarks>
internal static class ReplicationInboundEndpoint
{
    /// <summary>
    /// Registers the inbound replication handler on <paramref name="app"/>.
    /// The endpoint authenticates via
    /// <see cref="ReplicationConstants.AuthHeader"/> against the
    /// topology's shared secret; missing or wrong token → 401.
    /// </summary>
    public static IEndpointRouteBuilder MapReplicationEndpoint(this IEndpointRouteBuilder app)
    {
        app.MapPost("/replicate/{tree}", async (
            string tree,
            ReplicationBatch batch,
            HttpRequest request,
            IGrainFactory grains,
            ReplicationTopology topology,
            ReplicationActivityTracker activity,
            ILoggerFactory loggerFactory,
            CancellationToken cancellationToken) =>
        {
            var log = loggerFactory.CreateLogger("MultiSiteManufacturing.Host.Replication.Inbound");

            if (!request.Headers.TryGetValue(ReplicationConstants.AuthHeader, out var token)
                || !string.Equals(token.ToString(), topology.SharedSecret, StringComparison.Ordinal))
            {
                return Results.Unauthorized();
            }

            // Replication-disconnect chaos preset: refuse inbound
            // replication so the peer backs off. 503 signals a
            // transient unavailability — the peer will retry.
            if (await grains
                .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey)
                .IsDisconnectedAsync())
            {
                return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
            }

            if (batch is null
                || !string.Equals(batch.Tree, tree, StringComparison.Ordinal)
                || string.IsNullOrEmpty(batch.SourceCluster))
            {
                return Results.BadRequest("Malformed batch.");
            }

            if (tree.StartsWith(ReplicationConstants.ReplogTreePrefix, StringComparison.Ordinal))
            {
                return Results.BadRequest("Replog trees are not replicable.");
            }

            var lattice = grains.GetGrain<ILattice>(tree);
            var applied = 0;
            var highest = HybridLogicalClock.Zero;

            // Cycle break: the outgoing call filter reads this flag and
            // skips appending to the local replog for every SetAsync /
            // DeleteAsync invoked below. Stamp once outside the loop;
            // inbound apply is the only path that sets this key, and
            // RequestContext is scoped to this HTTP request's activity
            // so there's no risk of leaking into unrelated grain calls.
            RequestContext.Set(ReplicationConstants.ReplayRequestContextKey, batch.SourceCluster);
            try
            {
                foreach (var entry in batch.Entries)
                {
                    try
                    {
                        if (entry.Op == ReplicationOp.Delete)
                        {
                            await lattice.DeleteAsync(entry.Key, cancellationToken).ConfigureAwait(false);
                        }
                        else if (entry.Value is { Length: > 0 } bytes)
                        {
                            await lattice.SetAsync(entry.Key, bytes, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            // A forward entry whose value is null / empty
                            // at ship time means the key was deleted
                            // between replog append and ship — treat as
                            // a delete on the receiver.
                            await lattice.DeleteAsync(entry.Key, cancellationToken).ConfigureAwait(false);
                        }

                        applied++;
                        if (entry.SourceHlc.CompareTo(highest) > 0)
                        {
                            highest = entry.SourceHlc;
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogWarning(ex,
                            "Inbound replication: apply failed for tree {Tree} key {Key} op {Op} source {Source}",
                            tree, entry.Key, entry.Op, batch.SourceCluster);
                        // Don't throw — partial-apply is fine, the sender
                        // advances the cursor using ack.HighestAppliedHlc.
                        break;
                    }
                }
            }
            finally
            {
                RequestContext.Remove(ReplicationConstants.ReplayRequestContextKey);
            }

            activity.RecordReceived(batch.SourceCluster, applied);

            return Results.Ok(new ReplicationAck
            {
                Applied = applied,
                HighestAppliedHlc = highest,
            });
        });

        return app;
    }
}
