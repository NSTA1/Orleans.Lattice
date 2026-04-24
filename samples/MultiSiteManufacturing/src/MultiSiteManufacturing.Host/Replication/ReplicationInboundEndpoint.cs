using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
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
/// has a known cross-cluster divergence risk — opt in with that
/// caveat.
/// </para>
/// <para>
/// <b>Baseline replay.</b> For the <see cref="LatticeFactBackend.FactTreeId"/>
/// tree, every successfully-applied <see cref="ReplicationOp.Set"/>
/// entry is also decoded and fed to the local
/// <see cref="BaselineFactBackend"/>. This models naive event-log
/// replication for the baseline backend — it lets a peer cluster's
/// baseline track the same fact set a local emit would produce, so
/// cold-seed state on the peer matches the seeding cluster.
/// The replog stays lattice-only (this is a one-way replay from the
/// lattice stream into the peer's baseline); <see cref="ReplicationOp.Delete"/>
/// entries are ignored because the baseline has no fact-retraction
/// concept. Decode / emit failures are logged and the batch
/// continues — baseline is a demo-visualisation backend and a
/// single-entry loss is preferable to breaking replication.
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
            BaselineFactBackend baselineBackend,
            FederationRouter router,
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

            // Only the fact-tree feeds the baseline backend. Other
            // replicated trees (site activity index, part CRDT) have
            // no baseline analogue.
            var replayToBaseline = string.Equals(tree, LatticeFactBackend.FactTreeId, StringComparison.Ordinal);

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

                            if (replayToBaseline)
                            {
                                var replayed = await TryReplayToBaselineAsync(
                                    baselineBackend, bytes, batch.SourceCluster, entry.Key, log, cancellationToken)
                                    .ConfigureAwait(false);

                                // Fire-and-forget dashboard notification:
                                // the broadcaster fans out per-part summary
                                // updates to active UI subscribers. Only
                                // raised on successful decode + emit — a
                                // subscriber that can't find the fact in
                                // the baseline would be confused by a
                                // spurious event.
                                if (replayed is not null)
                                {
                                    router.RaiseFactReplicated(replayed);
                                }
                            }
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

    /// <summary>
    /// Decodes a replicated <c>mfg-facts</c> payload and feeds the
    /// resulting <see cref="Fact"/> into <paramref name="baseline"/>.
    /// Exceptions are logged and swallowed — a single malformed or
    /// un-decodable entry must not abort the replication batch.
    /// </summary>
    /// <returns>
    /// The decoded <see cref="Fact"/> when both decode and emit
    /// succeeded, so the caller can raise
    /// <see cref="FederationRouter.FactReplicated"/>; otherwise
    /// <see langword="null"/>.
    /// </returns>
    /// <remarks>
    /// Exposed as <c>internal</c> so unit tests can exercise the
    /// decode + emit path without standing up the full minimal-API
    /// host.
    /// </remarks>
    internal static async Task<Fact?> TryReplayToBaselineAsync(
        BaselineFactBackend baseline,
        byte[] payload,
        string sourceCluster,
        string key,
        ILogger log,
        CancellationToken cancellationToken)
    {
        Fact fact;
        try
        {
            fact = FactJsonCodec.Decode(payload);
        }
        catch (Exception ex)
        {
            log.LogWarning(ex,
                "Inbound replication: baseline replay skipped — decode failed for key {Key} source {Source}",
                key, sourceCluster);
            return null;
        }

        try
        {
            await baseline.EmitAsync(fact, cancellationToken).ConfigureAwait(false);
            return fact;
        }
        catch (Exception ex)
        {
            log.LogWarning(ex,
                "Inbound replication: baseline emit failed for fact {FactId} serial {Serial} source {Source}",
                fact.FactId, fact.Serial.Value, sourceCluster);
            return null;
        }
    }
}
