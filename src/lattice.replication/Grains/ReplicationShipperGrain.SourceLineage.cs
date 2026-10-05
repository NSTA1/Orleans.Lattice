using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The source lineage a shipper reads under (issue #4673). A source restore,
/// revert, purge and recreate, or alias move re-stamps the source tree's
/// registry lineage, and the records the source read before it describe
/// contents the new lineage no longer holds. Every push therefore carries the
/// lineage the shipper's binding was read under
/// (<see cref="ReplicationBatch.SourceLineage"/>), so a receiver that drained
/// the new lineage refuses a batch read under the old one.
/// <para>
/// A binding whose lineage changed - under a new physical log or, for a purge
/// and recreate, under the same one - forces a gap: the shipper reads each
/// partition's next sequence of the newly bound log as a boundary, then takes
/// the peer off the log. Every record below the boundary was appended before
/// the re-seed marker, so the export that settles the re-seed carries it, and
/// the shipper consumes it without shipping for as long as the binding holds,
/// the re-seed rewind included. The old-lineage records the log still holds
/// are all below it, because no old-lineage write lands after the re-stamp. A
/// move of the physical log under an unchanged lineage (a resize) keeps the
/// replay it always had (#4533).
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    // Set when an ack refused a batch for its source lineage; acted on at the
    // end of the tick.
    private bool _sourceLineageRefused;

    /// <summary>The lineage the next batch is stamped with; test seam.</summary>
    internal Guid? SourceLineageStampForTesting => SourceLineageStamp;

    /// <summary>
    /// The lineage pushed batches carry: <see langword="null"/> when the source
    /// registry tracks none for the tree, <see cref="Guid.Empty"/> while the
    /// binding's lineage is not known.
    /// </summary>
    private Guid? SourceLineageStamp =>
        state.State.BoundSourceLineageKnown ? state.State.BoundSourceLineage : Guid.Empty;

    /// <summary>
    /// Reads the source tree's registry row: its physical id, and its lineage
    /// when the row exists. A missing row resolves the physical id the way it
    /// always has and reports no lineage.
    /// </summary>
    private async Task<(string Physical, SourceLineageObservation Lineage)> ResolveSourceBindingAsync()
    {
        var registry = _grainFactory.GetLatticeRegistry();
        var entry = await registry.GetEntryAsync(_treeName);
        if (entry is not null)
        {
            return (entry.PhysicalTreeId ?? _treeName, new SourceLineageObservation(true, entry.Lineage));
        }

        return (await ResolveSourcePhysicalAsync(), new SourceLineageObservation(true, null));
    }

    /// <summary>
    /// The lineage of <paramref name="physical"/> as the registry reports it
    /// now, for a rebind notified with the physical id alone. Unknown when the
    /// registry has already moved on, so the next authoritative resolve decides.
    /// </summary>
    private async Task<SourceLineageObservation> ObserveLineageForAsync(string physical)
    {
        var entry = await _grainFactory.GetLatticeRegistry().GetEntryAsync(_treeName);
        if (entry is null)
        {
            return new SourceLineageObservation(true, null);
        }

        return string.Equals(entry.PhysicalTreeId ?? _treeName, physical, StringComparison.Ordinal)
            ? new SourceLineageObservation(true, entry.Lineage)
            : default;
    }

    /// <summary>
    /// Records the lineage a binding was resolved under. Returns whether the
    /// binding's lineage changed from one known value to another, which the
    /// caller answers with <see cref="ForceSourceLineageGapAsync"/>.
    /// </summary>
    private bool NoteSourceLineage(SourceLineageObservation observed, bool physicalChanged)
    {
        if (!observed.Known)
        {
            if (physicalChanged)
            {
                // The registry moved on again; stamp "unknown" until it settles.
                state.State.BoundSourceLineageKnown = false;
            }

            return false;
        }

        var changed = state.State.BoundSourceLineageKnown && state.State.BoundSourceLineage != observed.Lineage;
        state.State.BoundSourceLineage = observed.Lineage;
        state.State.BoundSourceLineageKnown = true;
        return changed;
    }

    /// <summary>
    /// The binding's lineage changed: reads each partition's next sequence of
    /// the bound log as the boundary below which nothing ships under this
    /// binding, then takes the peer off the log, or raises the marker to the
    /// current export epoch when it already is. The boundary is read first, so
    /// every record below it predates the marker and the settling export
    /// carries it.
    /// </summary>
    private async Task ForceSourceLineageGapAsync(int partitions)
    {
        var boundary = state.State.SourceLineageBoundary;
        boundary.Clear();
        for (var p = 0; p < partitions; p++)
        {
            var grain = _partitionGrainCache.Length > p && _partitionGrainCache[p] is { } cached
                ? cached
                : _grainFactory.GetGrain<IWalShardGrain>($"{_walTreeId}/{p}");
            boundary[p] = await grain.GetNextSequenceAsync(CancellationToken.None);
        }

        long epoch;
        if (!ReseedRequired)
        {
            epoch = await TakePeerOffLogAsync();
        }
        else
        {
            epoch = await _grainFactory.GetGrain<IReplicationExportEpochGrain>(_treeName).GetAsync();
            if (state.State.ReseedRequiredEpoch is { } marker && epoch > marker)
            {
                state.State.ReseedRequiredEpoch = epoch;
            }

            await state.WriteStateAsync();
        }

        Logger.LogWarning(
            "{Context}: the source tree's lineage changed to {Lineage}; records the bound log held at the change are not "
            + "shipped, and the peer must be re-seeded from a snapshot export after epoch {Epoch}.",
            LogContext, state.State.BoundSourceLineage, epoch);
    }

    /// <summary>Whether <paramref name="sequence"/> of <paramref name="partition"/> is below the binding's lineage boundary.</summary>
    private bool IsBelowSourceLineageBoundary(int partition, long sequence) =>
        state.State.SourceLineageBoundary.Count > 0
        && state.State.SourceLineageBoundary.TryGetValue(partition, out var boundary)
        && sequence < boundary;

    /// <summary>Records an ack that refused a batch for its source lineage.</summary>
    private void NoteSourceLineageRefusal(ReplicationAck ack)
    {
        if (ack.SourceLineageRefused)
        {
            _sourceLineageRefused = true;
        }
    }

    /// <summary>
    /// End-of-tick step: a refusal for the source lineage re-resolves the
    /// binding at once. A stale binding rebinds, and never re-sends the old log.
    /// A current, known binding means the peer drained another lineage, so the
    /// peer is taken off the log and re-seeded from the current one. A binding
    /// whose lineage is not known only waits for the next resolve.
    /// </summary>
    private async Task MaybeHandleSourceLineageRefusalAsync(int partitions)
    {
        if (!_sourceLineageRefused)
        {
            return;
        }

        _sourceLineageRefused = false;
        var boundBefore = state.State.BoundPhysicalTreeId;
        var lineageBefore = state.State.BoundSourceLineage;
        var (physical, observed) = await ResolveSourceBindingAsync();
        await ApplyResolvedIdentityAsync(physical, partitions, observed);
        var rebound = !string.Equals(boundBefore, state.State.BoundPhysicalTreeId, StringComparison.Ordinal)
            || lineageBefore != state.State.BoundSourceLineage;
        if (rebound || !state.State.BoundSourceLineageKnown || state.State.BoundSourceLineage is not { } current || current == Guid.Empty)
        {
            return;
        }

        if (!ReseedRequired)
        {
            var epoch = await TakePeerOffLogAsync();
            Logger.LogWarning(
                "{Context}: the peer refused a batch read under source lineage {Lineage}, which it has not drained; it must be "
                + "re-seeded from a snapshot export after epoch {Epoch}.",
                LogContext, current, epoch);
        }
    }

    /// <summary>The lineage a registry read reports for the bound source tree.</summary>
    /// <param name="Known">Whether the read describes the binding at all.</param>
    /// <param name="Lineage">The registry lineage; <see langword="null"/> when the row tracks none.</param>
    private readonly record struct SourceLineageObservation(bool Known, Guid? Lineage);
}
