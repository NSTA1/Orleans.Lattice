using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Diagnostic aggregation for the shard root grain. Computes depth,
/// live/tombstone counts, and hotness in a single grain call for the
/// diagnostics surface (<see cref="ILattice.DiagnoseAsync"/>).
/// </summary>
internal sealed partial class ShardRootGrain
{
    // Activation-scoped per-leaf byte-footprint cache. Not persisted: a
    // reactivation starts empty and the totals converge as leaves
    // re-publish on their next commit. The operator-driven
    // RefreshLeafByteFootprintsAsync re-anchors them exactly when an
    // authoritative figure is needed. Keeping these off the persistent
    // state row avoids a noisy WriteStateAsync on every leaf commit
    // racing the foreground saga writes through the same etag CAS.
    private readonly Dictionary<Guid, LeafByteFootprint> _leafByteFootprints = new();
    private long _leafStateBytesTotal;
    private long _snapshotBytesTotal;
    private long _liveKeyCountTotal;

    /// <inheritdoc />
    public async Task<ShardDiagnosticReport> GetDiagnosticsAsync(bool deep)
    {
        // Retained for wire compatibility with a caller from an older build
        // that has not adopted the bounded protocol. Drives the bounded walk to
        // completion inside this one call, so an old caller sees exactly the
        // old behaviour - including the old whole-chain hold.
        var page = await GetDiagnosticsBoundedAsync(deep, null);
        var report = page.Report;
        var liveKeys = report.LiveKeys;
        var tombstones = report.Tombstones;

        var cursor = page.ResumeFromInclusive;
        while (cursor is not null)
        {
            page = await GetDiagnosticsBoundedAsync(deep, cursor);
            liveKeys += page.Report.LiveKeys;
            tombstones += page.Report.Tombstones;
            cursor = page.ResumeFromInclusive;
        }

        return WithTotals(report, liveKeys, tombstones);
    }

    /// <summary>
    /// Substitutes accumulated key counts into a first-batch report and
    /// recomputes the derived tombstone ratio from them, so the ratio always
    /// describes the totals it is published beside.
    /// </summary>
    private static ShardDiagnosticReport WithTotals(
        ShardDiagnosticReport report, long liveKeys, long tombstones)
    {
        var total = liveKeys + tombstones;
        return report with
        {
            LiveKeys = liveKeys,
            Tombstones = tombstones,
            TombstoneRatio = total > 0 ? (double)tombstones / total : 0.0,
        };
    }

    /// <inheritdoc />
    public async Task<ShardDiagnosticsPage> GetDiagnosticsBoundedAsync(bool deep, string? resumeFromInclusive)
    {
        // Ensure persistent state is loaded before inspection.
        if (state.RecordExists == false)
        {
            // Nothing persisted yet - treat as empty leaf-only shard.
        }

        // A resumed batch reports only the counts it gathered. Depth, hotness
        // and the lifecycle flags are shard-level facts the first batch already
        // established; recomputing depth in particular would re-descend the
        // internal levels once per batch for an identical answer.
        var resuming = resumeFromInclusive is not null;

        var rootIsLeaf = state.State.RootIsLeaf;
        var rootNodeId = state.State.RootNodeId;
        var splitInProgress = state.State.SplitInProgress is not null;
        var bulkPending = state.State.PendingBulkGraft is not null;

        var depth = 1;
        long liveKeys = 0;
        long tombstones = 0;
        string? resumeFrom = null;

        if (rootNodeId is null)
        {
            // No root yet - empty shard.
            depth = 0;
        }
        else if (RootIsLeafTyped)
        {
            // A leaf root is the shard's only leaf: there is no chain to bound
            // and no resume position, so a resumed batch has nothing to add.
            if (!resuming)
            {
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(rootNodeId.Value.GetGuidKey());
                if (deep)
                {
                    var stats = await leaf.GetStatsAsync();
                    liveKeys = stats.LiveKeys;
                    tombstones = stats.Tombstones;
                }
                else
                {
                    liveKeys = await leaf.CountAsync();
                }
            }
        }
        else
        {
            GrainId? startLeafId;
            if (resuming)
            {
                startLeafId = await ResolveWalkStartLeafAsync(resumeFromInclusive);
            }
            else
            {
                // Walk leftmost path to compute depth. Capped at
                // MaxTreeDescentLevels (issue 1972) so a corrupt or cyclic
                // leftmost-child pointer surfaces as a typed exception instead
                // of spinning this non-reentrant grain forever. #1957 applied
                // the cap to the bare `while (true)` descents in Traversal.cs;
                // this one is spelled `while (!childrenAreLeaves)` and lives in
                // Diagnostics.cs, so it fell outside that sweep.
                var currentId = rootNodeId.Value;
                var childrenAreLeaves = false;
                for (var level = 0; level < MaxTreeDescentLevels && !childrenAreLeaves; level++)
                {
                    depth++;
                    var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId.GetGuidKey());
                    var next = await internalGrain.GetLeftmostChildWithMetadataAsync();
                    currentId = next.ChildId;
                    childrenAreLeaves = next.ChildrenAreLeaves;
                }

                if (!childrenAreLeaves)
                {
                    throw new InvalidOperationException(
                        $"ShardRootGrain {context.GrainId} diagnostics depth descent exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
                }

                // The descent lands on the leftmost leaf, so it doubles as the
                // walk's start position and saves a second descent.
                startLeafId = currentId;
            }

            // Walk the leaf chain through the shared bounded walk, so this
            // aggregation obeys the same budget, key cursor and
            // "only stop where you can resume" rule as every other bounded
            // chain walk (issues 1973, 1972).
            var walk = BoundedLeafWalk.FromResolvedStart(
                grainFactory,
                startLeafId,
                resumeFromInclusive,
                LeafWalkBudget.ForScanPage(await GetOptionsAsync()));

            while (walk.HasLeaf)
            {
                var leaf = walk.CurrentLeaf;
                if (deep)
                {
                    var stats = await leaf.GetStatsAsync();
                    liveKeys += stats.LiveKeys;
                    tombstones += stats.Tombstones;
                }
                else
                {
                    liveKeys += await leaf.CountAsync();
                }

                if (!await walk.MoveNextAsync()) break;
            }

            resumeFrom = walk.ResumeFromInclusive;
        }

        if (resuming)
        {
            // Only the counts are meaningful on a resumed batch; see
            // ShardDiagnosticsPage.
            return new ShardDiagnosticsPage
            {
                Report = new ShardDiagnosticReport
                {
                    LiveKeys = liveKeys,
                    Tombstones = tombstones,
                },
                ResumeFromInclusive = resumeFrom,
            };
        }

        var hotness = await GetHotnessAsync();
        var opsPerSec = hotness.Window.TotalSeconds > 0
            ? (hotness.Reads + hotness.Writes) / hotness.Window.TotalSeconds
            : 0.0;
        var ratio = (liveKeys + tombstones) > 0
            ? (double)tombstones / (liveKeys + tombstones)
            : 0.0;

        return new ShardDiagnosticsPage
        {
            Report = new ShardDiagnosticReport
            {
                // ShardIndex stamped by caller.
                Depth = depth,
                RootIsLeaf = rootIsLeaf,
                LiveKeys = liveKeys,
                Tombstones = tombstones,
                TombstoneRatio = ratio,
                OpsPerSecond = opsPerSec,
                Reads = hotness.Reads,
                Writes = hotness.Writes,
                HotnessWindow = hotness.Window,
                SplitInProgress = splitInProgress,
                BulkOperationPending = bulkPending,
            },
            ResumeFromInclusive = resumeFrom,
        };
    }

    /// <inheritdoc />
    public Task<ShardStorageUsage> GetStorageUsageAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // O(1) read off the activation-scoped running totals. Leaves
        // publish their per-leaf footprint via
        // PublishLeafByteFootprintAsync on every commit boundary, so the
        // sum here matches a fresh deep walk by construction once every
        // leaf has published at least once since activation. A
        // freshly-reactivated shard root reports zero until the first
        // leaf publish lands; the operator-driven re-anchor below
        // returns the exact figure when authoritative numbers are needed.
        return Task.FromResult(new ShardStorageUsage
        {
            LeafStateBytes = _leafStateBytesTotal,
            SnapshotBytes = _snapshotBytesTotal,
            LiveKeys = _liveKeyCountTotal,
        });
    }

    /// <inheritdoc />
    public Task PublishLeafByteFootprintAsync(Guid leafKey, LeafByteFootprint footprint)
    {
        // Activation-scoped: no WriteStateAsync, so we cannot race the
        // foreground saga writes through the shard root's etag CAS.
        if (footprint.StateBytes < 0 && footprint.SnapshotBytes < 0)
        {
            if (_leafByteFootprints.Remove(leafKey, out var prev))
            {
                _leafStateBytesTotal -= prev.StateBytes;
                _snapshotBytesTotal -= prev.SnapshotBytes;
                _liveKeyCountTotal -= prev.LiveKeys;
            }
            return Task.CompletedTask;
        }

        if (_leafByteFootprints.TryGetValue(leafKey, out var existing))
        {
            if (existing.StateBytes == footprint.StateBytes
                && existing.SnapshotBytes == footprint.SnapshotBytes
                && existing.LiveKeys == footprint.LiveKeys)
            {
                return Task.CompletedTask;
            }
            _leafStateBytesTotal += footprint.StateBytes - existing.StateBytes;
            _snapshotBytesTotal += footprint.SnapshotBytes - existing.SnapshotBytes;
            _liveKeyCountTotal += footprint.LiveKeys - existing.LiveKeys;
        }
        else
        {
            _leafStateBytesTotal += footprint.StateBytes;
            _snapshotBytesTotal += footprint.SnapshotBytes;
            _liveKeyCountTotal += footprint.LiveKeys;
        }
        _leafByteFootprints[leafKey] = footprint;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<ShardStorageUsage> RefreshLeafByteFootprintsAsync(CancellationToken cancellationToken)
    {
        // Retained for wire compatibility with a caller from an older build
        // that has not adopted the bounded protocol. Drives the bounded walk to
        // completion inside this one call.
        var total = default(ShardStorageUsage);
        string? cursor = null;
        while (true)
        {
            var page = await RefreshLeafByteFootprintsBoundedAsync(cursor, total, cancellationToken);
            total = Add(total, page.Usage);
            if (page.ResumeFromInclusive is not { } next) return total;
            cursor = next;
        }
    }

    /// <summary>Sums two byte-footprint rollups field by field.</summary>
    private static ShardStorageUsage Add(ShardStorageUsage a, ShardStorageUsage b) => new()
    {
        LeafStateBytes = a.LeafStateBytes + b.LeafStateBytes,
        SnapshotBytes = a.SnapshotBytes + b.SnapshotBytes,
        LiveKeys = a.LiveKeys + b.LiveKeys,
    };

    /// <inheritdoc />
    public async Task<ShardStorageUsagePage> RefreshLeafByteFootprintsBoundedAsync(
        string? resumeFromInclusive,
        ShardStorageUsage accumulatedSoFar,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var page = await DeepWalkLeafFootprintsBoundedAsync(resumeFromInclusive, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        if (page.ResumeFromInclusive is null)
        {
            // Final batch: re-anchor the cached totals to the freshly-walked
            // whole-chain figure so any drift accumulated since the last
            // activation self-heals. Deliberately NOT done per batch - the
            // totals are what every concurrent GetStorageUsageAsync reads, and
            // anchoring them to a partial sum would make the shard under-report
            // its own footprint for the rest of the walk. A mid-walk
            // deactivation therefore leaves the totals untouched rather than
            // wrong, which is the same state a freshly-reactivated shard is
            // already documented to be in (issue 1972).
            var rollup = Add(accumulatedSoFar, page.Usage);
            _leafStateBytesTotal = rollup.LeafStateBytes;
            _snapshotBytesTotal = rollup.SnapshotBytes;
            _liveKeyCountTotal = rollup.LiveKeys;
        }

        return page;
    }

    private async Task<ShardStorageUsagePage> DeepWalkLeafFootprintsBoundedAsync(
        string? resumeFromInclusive, CancellationToken cancellationToken)
    {
        var rootNodeId = state.State.RootNodeId;
        if (rootNodeId is null)
        {
            return default;
        }

        if (RootIsLeafTyped)
        {
            // A leaf root is the shard's only leaf: no chain, no resume
            // position, and nothing for a resumed batch to add.
            if (resumeFromInclusive is not null) return default;
            return new ShardStorageUsagePage
            {
                Usage = await AccumulateLeafUsageAsync(rootNodeId.Value.GetGuidKey(), cancellationToken),
            };
        }

        GrainId? startLeafId;
        if (resumeFromInclusive is not null)
        {
            startLeafId = await ResolveWalkStartLeafAsync(resumeFromInclusive);
        }
        else
        {
            // Capped at MaxTreeDescentLevels for the same reason as the
            // diagnostics depth descent above (issue 1972).
            var currentId = rootNodeId.Value;
            var childrenAreLeaves = false;
            for (var level = 0; level < MaxTreeDescentLevels && !childrenAreLeaves; level++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId.GetGuidKey());
                var next = await internalGrain.GetLeftmostChildWithMetadataAsync();
                currentId = next.ChildId;
                childrenAreLeaves = next.ChildrenAreLeaves;
            }

            if (!childrenAreLeaves)
            {
                throw new InvalidOperationException(
                    $"ShardRootGrain {context.GrainId} leaf-footprint descent exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
            }

            startLeafId = currentId;
        }

        var walk = BoundedLeafWalk.FromResolvedStart(
            grainFactory,
            startLeafId,
            resumeFromInclusive,
            LeafWalkBudget.ForScanPage(await GetOptionsAsync()));

        var usage = default(ShardStorageUsage);
        while (walk.HasLeaf)
        {
            cancellationToken.ThrowIfCancellationRequested();
            usage = Add(usage, await AccumulateLeafUsageAsync(
                walk.CurrentLeafId!.Value.GetGuidKey(), cancellationToken));

            if (!await walk.MoveNextAsync()) break;
        }

        return new ShardStorageUsagePage
        {
            Usage = usage,
            ResumeFromInclusive = walk.ResumeFromInclusive,
        };
    }

    /// <summary>
    /// Reads a single leaf's state-byte footprint and its persisted-snapshot
    /// footprint, returning them as a one-leaf <see cref="ShardStorageUsage"/>.
    /// The leaf-state figure comes from the leaf grain's
    /// <see cref="LeafStats.StateBytes"/>; the snapshot figure comes from the
    /// per-leaf snapshot storage grain keyed by the same <see cref="System.Guid"/>.
    /// </summary>
    private async Task<ShardStorageUsage> AccumulateLeafUsageAsync(Guid leafKey, CancellationToken cancellationToken)
    {
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafKey);
        var stats = await leaf.GetStatsAsync();

        var snapshot = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey);
        var snapshotBytes = await snapshot.GetSnapshotByteSizeAsync(cancellationToken);

        return new ShardStorageUsage
        {
            LeafStateBytes = stats.StateBytes,
            SnapshotBytes = snapshotBytes,
            LiveKeys = stats.LiveKeys,
        };
    }
}
