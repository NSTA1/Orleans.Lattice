using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Pure, allocation-conscious engine for the content-hash payload-elision
/// round trip. Splits the round trip into three side-effect-free steps so
/// each can be unit-tested in isolation and composed by the shipper (sender
/// side) and a transport's exchange handler (receiver side):
/// <list type="number">
///   <item><see cref="BuildManifest"/> - sender: hash the eligible entries
///   of a drained batch into a per-entry manifest.</item>
///   <item><see cref="ComputeMissingSet"/> - receiver: compare a manifest
///   against the content the receiver already holds and report the missing
///   subset plus the high-water-mark advance for identical-content entries
///   carrying a newer clock.</item>
///   <item><see cref="ComputeElidedIndices"/> - sender: turn the receiver's
///   missing set back into the set of drain-buffer indices to drop.</item>
/// </list>
/// Only value-carrying point-<see cref="MutationKind.Set"/> entries that are
/// not part of an atomic-batch prepare phase and carry a real (non-zero)
/// clock are ever manifested, so range deletes, saga terminal marks,
/// prepared entries, and zero-clock entries are always shipped verbatim and
/// the per-origin FIFO, causal-dependency, and atomic-batch invariants are
/// preserved across the elision path.
/// </summary>
internal static class ContentManifestPlanner
{
    /// <summary>
    /// Builds the per-entry content-hash manifest for the eligible entries
    /// of <paramref name="batch"/>. Each returned
    /// <see cref="ContentManifestEntry"/> carries its index into
    /// <paramref name="batch"/>, the key, the content hash, and the clock.
    /// Entries that are not elision-eligible are skipped, so the manifest is
    /// a (possibly empty) subset of the batch.
    /// </summary>
    public static IReadOnlyList<ContentManifestEntry> BuildManifest(IReadOnlyList<WalRecord> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        // Pre-count the eligible entries so the manifest is allocated at its
        // exact final size. IsManifestEligible is four field compares and no
        // allocation, so the extra pass is free next to the per-entry content
        // hashing the second pass does - and unlike a batch.Count hint it can
        // never over-allocate on a delete-heavy or saga-heavy batch. Growing
        // from empty previously walked the 4/8/16/.../1024 doubling chain and
        // abandoned every intermediate backing array.
        var eligible = 0;
        for (var i = 0; i < batch.Count; i++)
        {
            var probe = batch[i];
            if (IsManifestEligible(in probe))
            {
                eligible++;
            }
        }

        if (eligible == 0)
        {
            return Array.Empty<ContentManifestEntry>();
        }

        var manifest = new List<ContentManifestEntry>(eligible);
        for (var i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            if (!IsManifestEligible(in record))
            {
                continue;
            }

            manifest.Add(new ContentManifestEntry
            {
                EntryIndex = i,
                Key = record.Key ?? string.Empty,
                ContentHash = ReplicationContentHash.Compute(in record),
                Hlc = record.Timestamp,
            });
        }

        return manifest;
    }

    /// <summary>
    /// Receiver side: compares <paramref name="request"/>'s manifest against
    /// the content the receiver already holds (<paramref name="receiverHeldContent"/>,
    /// keyed by key to the content hash and clock the receiver last applied)
    /// and computes the pull-missing response. A manifest entry whose key is
    /// absent from <paramref name="receiverHeldContent"/>, or present with a
    /// different content hash, is reported as missing. A manifest entry whose
    /// content the receiver already holds is not missing; if its
    /// <see cref="ContentManifestEntry.Hlc"/> is strictly newer than the
    /// clock the receiver holds for that key, the response's
    /// <see cref="ContentManifestResponse.AdvancedHlc"/> is raised to the
    /// maximum such clock (the metadata-only high-water-mark advance for the
    /// idempotent re-set of an identical value).
    /// </summary>
    public static ContentManifestResponse ComputeMissingSet(
        in ContentManifestRequest request,
        IReadOnlyDictionary<string, (ulong ContentHash, HybridLogicalClock Hlc)> receiverHeldContent)
    {
        ArgumentNullException.ThrowIfNull(receiverHeldContent);
        var entries = request.Entries ?? (IReadOnlyList<ContentManifestEntry>)Array.Empty<ContentManifestEntry>();

        List<int>? missing = null;
        var advanced = HybridLogicalClock.Zero;
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (receiverHeldContent.TryGetValue(entry.Key, out var held)
                && held.ContentHash == entry.ContentHash)
            {
                // Receiver already holds byte-identical content. Elide the
                // payload; advance the high-water-mark when the manifest
                // clock is newer than what the receiver recorded.
                if (entry.Hlc.CompareTo(held.Hlc) > 0 && entry.Hlc.CompareTo(advanced) > 0)
                {
                    advanced = entry.Hlc;
                }
                continue;
            }

            (missing ??= new List<int>()).Add(entry.EntryIndex);
        }

        return new ContentManifestResponse
        {
            ExchangeSupported = true,
            MissingEntryIndices = missing ?? (IReadOnlyList<int>)Array.Empty<int>(),
            AdvancedHlc = advanced,
        };
    }

    /// <summary>
    /// Sender side: turns the receiver's <paramref name="missingEntryIndices"/>
    /// back into the set of drain-buffer indices the sender should elide -
    /// every manifested <see cref="ContentManifestEntry.EntryIndex"/> the
    /// receiver did <em>not</em> ask for. Because the manifest's
    /// <see cref="ContentManifestEntry.EntryIndex"/> values are positions in
    /// the drain buffer, the returned set is directly usable to drop entries
    /// from the outbound batch.
    /// </summary>
    public static HashSet<int> ComputeElidedIndices(
        IReadOnlyList<ContentManifestEntry> manifest,
        IReadOnlyList<int> missingEntryIndices)
    {
        ArgumentNullException.ThrowIfNull(manifest);

        var missing = missingEntryIndices is null || missingEntryIndices.Count == 0
            ? null
            : new HashSet<int>(missingEntryIndices);

        // Every manifested index the receiver did not ask for lands in the
        // result, so `manifest.Count - missing.Count` is its exact size in the
        // steady state (nothing missing) and a sound lower bound otherwise.
        // The subtraction is clamped because `missingEntryIndices` arrives over
        // the wire and is not trusted to be a subset of the manifest.
        var capacity = manifest.Count - (missing?.Count ?? 0);
        var elided = new HashSet<int>(capacity < 0 ? 0 : capacity);
        for (var i = 0; i < manifest.Count; i++)
        {
            var index = manifest[i].EntryIndex;
            if (missing is null || !missing.Contains(index))
            {
                elided.Add(index);
            }
        }

        return elided;
    }

    /// <summary>
    /// Whether <paramref name="record"/> is eligible for content-hash
    /// payload elision: a value-carrying point-<see cref="MutationKind.Set"/>
    /// write that is not part of an atomic-batch prepare phase and carries a
    /// real (non-zero) clock. Deletes carry no payload, range deletes / saga
    /// terminals / prepared entries / zero-clock entries are never elided so
    /// atomic-batch boundaries, causal ordering, and per-origin FIFO are
    /// preserved.
    /// </summary>
    private static bool IsManifestEligible(in WalRecord record) =>
        record.Op == MutationKind.Set
        && !record.IsPrepared
        && record.AtomicBatchSize == 0
        && record.Timestamp != HybridLogicalClock.Zero;
}
