using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree WAL floor-holder probe (issue #4195). Reads the tree's durable pin
/// offsets the same way the WAL GC pass does, picks the pin holding the offset
/// floor, and classifies the leaf behind it from that leaf's durable state, so an
/// operator can tell a tree that is wedged by a stranded pin from one that merely
/// has nothing to reclaim.
/// </summary>
/// <remarks>
/// Diagnostic only: nothing here feeds the trim predicate or drives a remedy. Every
/// read is fail-soft into the report (an unreadable pin store or leaf is reported
/// as such) rather than failing the call, so a caller always gets an answer that
/// says what could and could not be measured.
/// </remarks>
internal sealed class LatticeWalFloorHolderProbeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<LatticeWalFloorHolderProbeGrain> logger,
    [FromKeyedServices(LatticeOptions.StorageProviderName)] IGrainStorage? leafStateStorage = null)
    : ILatticeWalFloorHolderProbe
{
    private string TreeId => context.GrainId.Key.ToString()!;

    /// <inheritdoc />
    public async Task<WalFloorHolderProbeReport> ProbeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var treeId = TreeId;
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(optionsMonitor);

        IReadOnlyDictionary<string, long> offsets;
        try
        {
            offsets = await LatticeWalGc.ReadDurablePinOffsetsAsync(grainFactory, treeId, shardCount);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(ex, "WAL floor-holder probe could not read the durable pin offsets of tree {Tree}.", treeId);
            return new WalFloorHolderProbeReport { TreeId = treeId, PinStoreReadable = false, PinOffset = -1 };
        }

        var (holder, holderOffset, withoutOffset) = SelectHolder(offsets);
        var report = new WalFloorHolderProbeReport
        {
            TreeId = treeId,
            PinStoreReadable = true,
            PinCount = offsets.Count,
            PinsWithoutOffset = withoutOffset,
            ConsumerId = holder,
            PinOffset = holderOffset,
        };

        if (holder is null)
        {
            return report;
        }

        var walPartitions = await optionsResolver.GetWalPartitionsAsync(treeId);
        if (!WalFloorHolderReader.TryParseConsumerId(treeId, holder, walPartitions, out var leafGrainId, out var partition))
        {
            return report with { State = WalGcBlockingPinState.Unreadable };
        }

        WalGcBlockingPinState state;
        long? checkpoint;
        try
        {
            (state, checkpoint) = await WalFloorHolderReader.ReadLeafCheckpointAsync(leafStateStorage, leafGrainId, partition);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(ex, "WAL floor-holder probe could not read the durable state of leaf {Leaf} on tree {Tree}.", leafGrainId, treeId);
            (state, checkpoint) = (WalGcBlockingPinState.Unreadable, null);
        }

        // Issue #3168: CheckpointedUncovered asserts a coverage hole, which is
        // licensed only when the pin is known unusable (its frontier at the
        // blocking sentinel). This probe samples by offset, not by usability, so
        // the claim stands only when the frontier read proves the pin unusable;
        // anything else - including a frontier that could not be read - is
        // reported as the weaker CheckpointedCoverageUnknown.
        if (state == WalGcBlockingPinState.CheckpointedUncovered)
        {
            var frontier = await ReadFrontierAsync(treeId, holder, shardCount, cancellationToken);
            if (frontier is not { } known || known > HybridLogicalClock.Zero)
            {
                state = WalGcBlockingPinState.CheckpointedCoverageUnknown;
            }
        }

        return report with
        {
            LeafId = leafGrainId.ToString(),
            Partition = partition,
            PersistedCheckpoint = checkpoint,
            State = state,
        };
    }

    /// <summary>
    /// Picks the pin holding the offset floor: the lowest offset <c>&gt;= 0</c>,
    /// ties broken ordinally by consumer id so the answer is deterministic. When no
    /// pin reports a usable offset, the ordinally first <c>-1</c> pin is returned.
    /// </summary>
    /// <param name="offsets">The durable pin offsets, keyed by consumer id.</param>
    /// <returns>The holder (or <see langword="null"/> when there are no pins), its offset, and the count of pins at <c>-1</c>.</returns>
    internal static (string? Holder, long Offset, int WithoutOffset) SelectHolder(IReadOnlyDictionary<string, long> offsets)
    {
        string? usable = null;
        var usableOffset = long.MaxValue;
        string? absent = null;
        var withoutOffset = 0;

        foreach (var (consumerId, offset) in offsets)
        {
            if (offset < 0)
            {
                withoutOffset++;
                if (absent is null || string.CompareOrdinal(consumerId, absent) < 0)
                {
                    absent = consumerId;
                }

                continue;
            }

            if (usable is null
                || offset < usableOffset
                || (offset == usableOffset && string.CompareOrdinal(consumerId, usable) < 0))
            {
                usable = consumerId;
                usableOffset = offset;
            }
        }

        return usable is not null
            ? (usable, usableOffset, withoutOffset)
            : (absent, -1L, withoutOffset);
    }

    private async Task<HybridLogicalClock?> ReadFrontierAsync(
        string treeId, string consumerId, int shardCount, CancellationToken cancellationToken)
    {
        try
        {
            var key = WalMaterialiserPinRouting.ShardKey(treeId, consumerId, shardCount);
            var pins = await grainFactory.GetGrain<IWalMaterialiserPinGrain>(key).GetPinsAsync();
            return pins is not null && pins.TryGetValue(consumerId, out var frontier) ? frontier : null;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(ex, "WAL floor-holder probe could not read the pin frontier of consumer {Consumer} on tree {Tree}.", consumerId, treeId);
            return null;
        }
    }
}
