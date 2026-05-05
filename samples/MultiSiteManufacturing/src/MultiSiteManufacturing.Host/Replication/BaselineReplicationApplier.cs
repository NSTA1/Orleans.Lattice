using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Decorator on the package's <see cref="IReplicationApplier"/> that
/// observes every cross-cluster <c>mfg-facts</c> apply, decodes the
/// replicated payload into a <see cref="Fact"/>, mirrors it into the
/// local <see cref="BaselineFactBackend"/>, and raises
/// <see cref="FederationRouter.FactReplicated"/> so the dashboard's
/// "Inventory By Activity" tab refreshes live.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a decorator and not an <see cref="IChangeFeed"/> consumer?</b>
/// The package's apply pipeline merges replicated entries onto the
/// destination tree via <c>IShardRootGrain.MergeManyAsync</c>, which
/// deliberately bypasses the leaf's per-key
/// <see cref="IMutationObserver"/> publication path (the observer
/// fires on direct <c>SetAsync</c> / <c>DeleteAsync</c> only - see
/// <c>BPlusLeafGrain.MutationObserver.cs</c>). The replog is populated
/// by <c>ReplicationMutationObserver</c>, which is one of those
/// observers. Net effect: foreign-origin applies never enter the local
/// replog, and a consumer of <c>IChangeFeed.Subscribe(includeLocalOrigin: false)</c>
/// silently sees nothing for them. The previous polling implementation
/// (deleted alongside this file's introduction) was therefore dead from
/// the moment migration step 5 deleted the host-rolled inbound endpoint.
/// </para>
/// <para>
/// The <see cref="IReplicationApplier"/> seam runs synchronously on
/// every receiver-side apply (single-entry and batched) and returns
/// the precise <see cref="ApplyResult.Applied"/> outcome, so the
/// decorator only fires for entries that actually merged onto the
/// local tree - HWM-deduped, shadow-forward-deduped, parked-causal,
/// and local-origin-defence applies are correctly skipped.
/// </para>
/// <para>
/// <b>Filtering.</b> The decorator only acts on point-Set entries on
/// the canonical fact tree (<see cref="LatticeFactBackend.FactTreeId"/>).
/// Range deletes and per-key tombstones are ignored - the baseline has
/// no fact-retraction concept, matching the prior replay loop's
/// behaviour. Decode and emit failures are logged at Warning and
/// swallowed so a single malformed entry never propagates back into
/// the package's apply pipeline (which would surface as a 500 on the
/// inbound gRPC <c>Push</c> RPC and stall replication).
/// </para>
/// </remarks>
internal sealed class BaselineReplicationApplier(
    IReplicationApplier inner,
    BaselineFactBackend baseline,
    FederationRouter router,
    ILogger<BaselineReplicationApplier> logger) : IReplicationApplier
{
    /// <summary>
    /// Lattice tree id whose applies surface a <see cref="Fact"/> to
    /// the dashboard. Aliased through
    /// <see cref="LatticeFactBackend.FactTreeId"/> so a future rename
    /// of the canonical fact tree is picked up here automatically.
    /// </summary>
    public const string ObservedTreeId = LatticeFactBackend.FactTreeId;

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(ReplogEntry entry, CancellationToken cancellationToken = default)
    {
        var result = await inner.ApplyAsync(entry, cancellationToken).ConfigureAwait(false);
        if (result.Applied)
        {
            await TryFanOutAsync(entry, cancellationToken).ConfigureAwait(false);
        }
        return result;
    }

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyBatchAsync(
        IReadOnlyList<ReplogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);

        // Defer to the inner batch implementation so we keep its
        // optimised single-HWM-RPC-per-origin path. The batch result is
        // a coarse "any applied" summary, so we fall back to fanning
        // out every entry that targets the observed tree and would
        // have applied (Set + non-empty payload). The applier's HWM
        // and shadow-forward dedupe tables are authoritative - we
        // accept the small chance of a duplicate dashboard event for
        // a re-delivered entry rather than reproducing the dedupe
        // logic here.
        var result = await inner.ApplyBatchAsync(entries, cancellationToken).ConfigureAwait(false);
        if (!result.Applied || entries.Count == 0)
        {
            return result;
        }

        for (var i = 0; i < entries.Count; i++)
        {
            await TryFanOutAsync(entries[i], cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <summary>
    /// Decodes the replog entry's payload as a <see cref="Fact"/> and
    /// fans it out to the baseline backend and the dashboard. Exposed
    /// as <c>internal</c> so unit tests can drive the decode + emit
    /// path without standing up the full apply pipeline.
    /// </summary>
    /// <returns>
    /// The decoded <see cref="Fact"/> when both decode and emit
    /// succeeded, so callers / tests can assert what was raised on
    /// <see cref="FederationRouter.FactReplicated"/>; otherwise
    /// <see langword="null"/> (entry skipped or decode/emit failed).
    /// </returns>
    internal async Task<Fact?> TryFanOutAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        if (entry.TreeId != ObservedTreeId)
        {
            return null;
        }

        if (entry.Op != ReplogOp.Set || entry.IsTombstone || entry.Value is not { Length: > 0 } payload)
        {
            // Only successful Set entries with a non-empty value carry
            // a fact payload. Deletes, tombstones, and range deletes
            // are skipped - the baseline has no retraction concept.
            return null;
        }

        Fact fact;
        try
        {
            fact = FactJsonCodec.Decode(payload);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "BaselineReplicationApplier: decode failed for key {Key} source {Source}",
                entry.Key, entry.OriginClusterId ?? "(unknown)");
            return null;
        }

        try
        {
            await baseline.EmitAsync(fact, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "BaselineReplicationApplier: baseline emit failed for fact {FactId} serial {Serial} source {Source}",
                fact.FactId, fact.Serial.Value, entry.OriginClusterId ?? "(unknown)");
            return null;
        }

        // Fire-and-forget dashboard notification.
        // FederationRouter.RaiseFactReplicated already wraps handler
        // exceptions in a try/catch and logs at Warning, so we don't
        // wrap the call.
        router.RaiseFactReplicated(fact);
        return fact;
    }
}
