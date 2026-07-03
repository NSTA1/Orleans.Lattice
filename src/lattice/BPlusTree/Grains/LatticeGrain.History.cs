using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-key change-history read path. The primary read is a prefix scan over the
/// tree's durable history view (the append-only revision rows the history view
/// stores at <c>{key}/{encodedHlc}</c> in the <c>view-{name}</c> tree), reusing the
/// ordinary entry range-scan machinery rather than a bespoke reader. When no history
/// view is enabled it falls back, best-effort, to the retained source
/// write-ahead-log window and honestly reports truncation at the garbage-collection
/// trim point.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public async Task<EntryHistoryPage> ScanEntryHistoryAsync(
        string key,
        HybridLogicalClock? fromHlc,
        HybridLogicalClock? toHlc,
        int limit,
        string? continuation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ThrowIfSystemTree();
        if (limit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), limit, "The page limit must be greater than zero.");
        }

        var effectiveLimit = Math.Min(limit, EntryHistoryReader.MaxPageSize);

        // Fail-closed read enforcement: a caller may only observe a key's change
        // history if the access gate admits a point read of that key. A denied
        // key reports an empty timeline (not-found semantics), never throwing,
        // matching the point-read surfaces. Inherits the null-gate / gate-bypass
        // zero-cost short-circuit, so the default host pays nothing here.
        if (!await IsPointReadAllowedAsync(key, cancellationToken))
        {
            return new EntryHistoryPage
            {
                Revisions = Array.Empty<EntryRevision>(),
                Continuation = null,
                Truncated = false,
                EarliestAvailable = HybridLogicalClock.Zero,
                Source = EntryHistorySource.None,
            };
        }

        var registration = TryFindHistoryView();
        if (registration is not null)
        {
            return await ScanHistoryViewAsync(registration, key, fromHlc, toHlc, effectiveLimit, continuation, cancellationToken);
        }

        return await ScanWalWindowAsync(key, fromHlc, toHlc, effectiveLimit, continuation, cancellationToken);
    }

    /// <summary>
    /// Finds this tree's durable history-view registration (an accumulative view
    /// driven by the built-in history projection whose source is this tree), or
    /// <see langword="null"/> when none is enabled or the views feature is not
    /// registered.
    /// </summary>
    private ViewRegistration? TryFindHistoryView()
    {
        var catalog = services.GetService<IViewCatalog>();
        if (catalog is null)
        {
            return null;
        }

        foreach (var registration in catalog.All())
        {
            if (registration.Accumulative
                && registration.Projection is HistoryLatticeViewProjection
                && string.Equals(registration.SourceTreeId, TreeId, StringComparison.Ordinal))
            {
                return registration;
            }
        }

        return null;
    }

    /// <summary>
    /// Prefix-scans the history view's active-generation tree for a key's revision
    /// rows, decoding and mapping each into an <see cref="EntryRevision"/>. The scan
    /// is read-only and runs under an authorised view-read scope; it never perturbs
    /// the maintainer or its write-ahead-log pins.
    /// </summary>
    private async Task<EntryHistoryPage> ScanHistoryViewAsync(
        ViewRegistration registration,
        string key,
        HybridLogicalClock? fromHlc,
        HybridLogicalClock? toHlc,
        int effectiveLimit,
        string? continuation,
        CancellationToken cancellationToken)
    {
        var codec = services.GetRequiredService<HistoryRowCodec>();
        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(registration.ViewName);

        var (startInclusive, endExclusive) =
            EntryHistoryReader.ResolveViewScanWindow(key, fromHlc, continuation);

        var revisions = new List<EntryRevision>(Math.Min(effectiveLimit, 16));
        string? continuationOut = null;

        using (ViewReadContext.BeginScope())
        {
            var activeTreeId = await maintainer.GetActiveTreeIdAsync(cancellationToken);
            var viewTree = grainFactory.GetGrain<ILattice>(activeTreeId);

            await foreach (var entry in viewTree
                .EntriesAsync(startInclusive, endExclusive, cancellationToken: cancellationToken))
            {
                var row = codec.Decode(entry.Value);

                // The prefix {key}/ can also catch a nested key ({key}/child/...);
                // the decoded SourceKey is the exact filter.
                if (!string.Equals(row.SourceKey, key, StringComparison.Ordinal))
                {
                    continue;
                }

                if (toHlc is { } to && row.Timestamp.CompareTo(to) > 0)
                {
                    // Rows for a single key arrive in ascending HLC order, so the
                    // first one past the upper bound ends the in-range timeline.
                    continuationOut = null;
                    break;
                }

                if (fromHlc is { } from && row.Timestamp.CompareTo(from) < 0)
                {
                    continue;
                }

                revisions.Add(EntryHistoryReader.MapViewRow(row, EntryHistoryReader.DefaultValuePreviewBudget));
                if (revisions.Count >= effectiveLimit)
                {
                    continuationOut = entry.Key;
                    break;
                }
            }
        }

        return new EntryHistoryPage
        {
            Revisions = revisions,
            Continuation = continuationOut,
            Truncated = false,
            EarliestAvailable = HybridLogicalClock.Zero,
            Source = EntryHistorySource.View,
        };
    }

    /// <summary>
    /// Best-effort fallback for a tree with no history view: reads the retained
    /// write-ahead-log window for the key's partition above the current trim point,
    /// in offset order, and reports truncation when garbage collection has trimmed
    /// older entries. Returns an empty <see cref="EntryHistorySource.None"/> page
    /// when the replication read seam is not registered.
    /// </summary>
    private async Task<EntryHistoryPage> ScanWalWindowAsync(
        string key,
        HybridLogicalClock? fromHlc,
        HybridLogicalClock? toHlc,
        int effectiveLimit,
        string? continuation,
        CancellationToken cancellationToken)
    {
        var reader = services.GetService<ICommitLogReader>();
        if (reader is null)
        {
            return new EntryHistoryPage
            {
                Revisions = Array.Empty<EntryRevision>(),
                Continuation = null,
                Truncated = false,
                EarliestAvailable = HybridLogicalClock.Zero,
                Source = EntryHistorySource.None,
            };
        }

        // Resolve the WAL the same way the writer did: against the physical tree
        // id (routing can alias the logical id after a snapshot/reshard) and the
        // registry-pinned partition count (tree-immutable from first register, not
        // the silo's live LatticeOptions.WalPartitions).
        var (physicalTreeId, _) = await GetRoutingAsync(cancellationToken);
        var partitions = await optionsResolver.GetWalPartitionsAsync(physicalTreeId);
        var partition = WalPartitionHash.Compute(key, partitions);
        var tail = await reader.GetTailOffsetAsync(physicalTreeId, partition, cancellationToken);
        var truncated = tail > 0;

        var fromOffsetExclusive = -1L;
        if (continuation is not null && long.TryParse(continuation, out var parsed))
        {
            fromOffsetExclusive = parsed;
        }

        var revisions = new List<EntryRevision>(Math.Min(effectiveLimit, 16));
        string? continuationOut = null;
        var earliest = HybridLogicalClock.Zero;
        var sawAny = false;

        await foreach (var (offset, mutation) in reader
            .ReadAsync(physicalTreeId, partition, fromOffsetExclusive, cancellationToken))
        {
            if (!sawAny)
            {
                // The oldest readable entry on the partition is the trim-point floor.
                earliest = mutation.Timestamp;
                sawAny = true;
            }

            if (!EntryHistoryReader.WalMutationMatchesKey(mutation, key))
            {
                continue;
            }

            if (!EntryHistoryReader.WithinBounds(mutation.Timestamp, fromHlc, toHlc))
            {
                continue;
            }

            revisions.Add(EntryHistoryReader.MapWalMutation(mutation, EntryHistoryReader.DefaultValuePreviewBudget));
            if (revisions.Count >= effectiveLimit)
            {
                continuationOut = offset.ToString(System.Globalization.CultureInfo.InvariantCulture);
                break;
            }
        }

        return new EntryHistoryPage
        {
            Revisions = revisions,
            Continuation = continuationOut,
            Truncated = truncated,
            EarliestAvailable = truncated ? earliest : HybridLogicalClock.Zero,
            Source = EntryHistorySource.WalWindow,
        };
    }
}
