using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Migration-step-2 baseline-replay tap. Subscribes to the package's
/// <see cref="IChangeFeed"/> for the <see cref="LatticeFactBackend.FactTreeId"/>
/// tree, filters to remote-origin entries only, decodes each replicated
/// payload, and emits the resulting <see cref="Fact"/> into the local
/// <see cref="BaselineFactBackend"/>.
/// </summary>
/// <remarks>
/// <para>
/// Migration step 2 cut <c>mfg-facts</c> over to the package's
/// replication pipeline. The package's apply pipeline lives below the
/// lattice - replicated entries are merged through the package's
/// internal apply grain, not through <c>ILattice.SetAsync</c> - so
/// the receiver-side application code does not see them at all. The
/// <see cref="IChangeFeed"/> seam is the supported way to observe
/// applied entries from in-process consumers; subscribing with
/// <c>includeLocalOrigin: false</c> yields exactly the entries that
/// arrived from peers.
/// </para>
/// <para>
/// <b>Pull cadence.</b> <see cref="IChangeFeed.Subscribe"/> takes a
/// snapshot of the WAL at call time and completes once the snapshot is
/// exhausted. The background loop therefore drains the snapshot, then
/// awaits <see cref="PollInterval"/> before re-subscribing from the
/// updated cursor. The interval is short enough that operators see
/// the dashboard fact-replicated event within ~1s of the apply, and
/// long enough to avoid hammering the WAL between idle batches.
/// </para>
/// <para>
/// <b>Idempotency.</b> The baseline backend's <c>EmitAsync</c> appends
/// to a per-part grain - re-emitting the same fact is therefore not
/// idempotent (a duplicate would double-count), but the cursor is
/// strictly monotonic across loop iterations and a successful pass
/// updates the cursor only after the emit returns. A crash mid-batch
/// re-reads the same WAL window on restart, which is acceptable for
/// the sample's demo-visualisation backend; production usage would
/// persist the cursor.
/// </para>
/// <para>
/// <b>Failure mode.</b> Decode and emit errors are logged at Warning
/// and swallowed. The baseline is a demo-visualisation backend and a
/// single-entry loss is preferable to wedging the replay loop. Range
/// deletes and tombstones are skipped - the baseline has no
/// fact-retraction concept.
/// </para>
/// </remarks>
internal sealed class BaselineReplicationReplay(
    IChangeFeed changeFeed,
    BaselineFactBackend baseline,
    FederationRouter router,
    ILogger<BaselineReplicationReplay> logger) : BackgroundService
{
    /// <summary>
    /// Lattice tree id observed by the replay loop. Aliased through
    /// <see cref="LatticeFactBackend.FactTreeId"/> so a future rename
    /// of the canonical fact tree picks up here automatically.
    /// </summary>
    public const string ObservedTreeId = LatticeFactBackend.FactTreeId;

    /// <summary>
    /// Delay between the end of one WAL snapshot pass and the start
    /// of the next. Short enough that dashboard UIs see replicated
    /// facts in near-real-time; long enough that an idle replay loop
    /// is not a tight poll.
    /// </summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "BaselineReplicationReplay started; subscribing to change feed for tree {Tree} (remote-origin only).",
            ObservedTreeId);

        var cursor = HybridLogicalClock.Zero;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await foreach (var entry in changeFeed.Subscribe(
                    ObservedTreeId, cursor, includeLocalOrigin: false, stoppingToken)
                    .ConfigureAwait(false))
                {
                    if (entry.Op != ReplogOp.Set || entry.IsTombstone || entry.Value is not { Length: > 0 } payload)
                    {
                        // Only successful Set entries with a non-empty
                        // value carry a fact payload. Deletes,
                        // tombstones, and range deletes are skipped -
                        // baseline has no retraction concept.
                        cursor = entry.Timestamp;
                        continue;
                    }

                    var replayed = await TryReplayToBaselineAsync(
                        baseline, payload, entry.OriginClusterId, entry.Key, logger, stoppingToken)
                        .ConfigureAwait(false);

                    if (replayed is not null)
                    {
                        // Fire-and-forget dashboard notification.
                        // FederationRouter.RaiseFactReplicated already
                        // handles handler exceptions by logging at
                        // Warning, so we don't wrap the call.
                        router.RaiseFactReplicated(replayed);
                    }

                    cursor = entry.Timestamp;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "BaselineReplicationReplay: change feed subscription threw; continuing after backoff.");
            }

            try
            {
                await Task.Delay(PollInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        logger.LogInformation("BaselineReplicationReplay stopped.");
    }

    /// <summary>
    /// Decodes a replicated <c>mfg-facts</c> payload and feeds the
    /// resulting <see cref="Fact"/> into <paramref name="baseline"/>.
    /// Exceptions are logged and swallowed - a single malformed or
    /// un-decodable entry must not abort the replay loop.
    /// </summary>
    /// <returns>
    /// The decoded <see cref="Fact"/> when both decode and emit
    /// succeeded, so the caller can raise
    /// <see cref="FederationRouter.FactReplicated"/>; otherwise
    /// <see langword="null"/>.
    /// </returns>
    /// <remarks>
    /// Exposed as <c>internal</c> so unit tests can exercise the
    /// decode + emit path without standing up the full change-feed
    /// pipeline.
    /// </remarks>
    internal static async Task<Fact?> TryReplayToBaselineAsync(
        BaselineFactBackend baseline,
        byte[] payload,
        string? sourceCluster,
        string key,
        ILogger log,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(baseline);
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(log);

        Fact fact;
        try
        {
            fact = FactJsonCodec.Decode(payload);
        }
        catch (Exception ex)
        {
            log.LogWarning(ex,
                "BaselineReplicationReplay: decode failed for key {Key} source {Source}",
                key, sourceCluster ?? "(unknown)");
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
                "BaselineReplicationReplay: emit failed for fact {FactId} serial {Serial} source {Source}",
                fact.FactId, fact.Serial.Value, sourceCluster ?? "(unknown)");
            return null;
        }
    }
}
