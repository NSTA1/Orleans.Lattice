using System.Globalization;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice;

/// <summary>
/// The two durable reads that name and classify the leaf behind a WAL
/// materialiser pin, shared by the WAL GC scheduler's floor-holder census and
/// the on-demand floor-holder probe (issue #4195), so the two can never
/// disagree about which leaf a pin belongs to or what state it is in.
/// </summary>
/// <remarks>
/// Both reads are pure: neither activates the leaf. The leaf a wedge strands is
/// precisely one that cannot be activated into fixing itself, so a read that
/// needed the leaf live would measure the wrong leaves.
/// </remarks>
internal static class WalFloorHolderReader
{
    /// <summary>
    /// Durable state name of <c>BPlusLeafGrain</c>'s persisted
    /// <see cref="LeafNodeState"/>, as declared by its
    /// <c>[PersistentState("leaf", ...)]</c> injection. Reading the same slot
    /// the grain would is what makes a direct read equivalent to asking the leaf.
    /// </summary>
    internal const string LeafStateName = "leaf";

    /// <summary>
    /// Parses a materialiser consumer id back into the grain id of the leaf that
    /// published it and the WAL partition the pin belongs to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Fail-closed: anything that does not match the exact expected shape
    /// resolves nothing. A consumer id carrying no partition suffix is partition
    /// <c>0</c>, matching the legacy single-partition shape. The suffix is
    /// stripped only when the tree is actually partitioned, so a grain id that
    /// legitimately ends in <c>_&lt;digits&gt;</c> on a single-partition tree is
    /// not silently truncated.
    /// </para>
    /// <para>
    /// The suffix grammar is the one <see cref="ClassifyConsumerId"/> applies
    /// before any pin removal (issue #4242): on a partitioned tree a trailing
    /// <c>_&lt;sign?&gt;&lt;digits&gt;</c> group is a partition suffix, and it
    /// resolves only when it is canonical decimal (no sign, no leading zero) and
    /// below <paramref name="walPartitions"/>. Any other such group - <c>_9</c>
    /// on an 8-partition tree, <c>_03</c>, <c>_+3</c> - resolves nothing rather
    /// than being attributed to a partition the tree does not have.
    /// </para>
    /// </remarks>
    /// <param name="treeId">The physical tree id the pin belongs to.</param>
    /// <param name="consumerId">The materialiser consumer id.</param>
    /// <param name="walPartitions">The tree's WAL partition count.</param>
    /// <param name="leafGrainId">The leaf that published the pin, when parsed.</param>
    /// <param name="partition">The WAL partition the pin belongs to, when parsed.</param>
    /// <returns><see langword="true"/> when the id parsed.</returns>
    internal static bool TryParseConsumerId(
        string treeId,
        string consumerId,
        int walPartitions,
        out GrainId leafGrainId,
        out int partition) =>
        Parse(treeId, consumerId, walPartitions, out leafGrainId, out partition, out _, out _)
            == ConsumerIdVerdict.LeafPublished;

    /// <summary>
    /// Whether <paramref name="consumerId"/> is exactly the materialiser consumer
    /// id a <c>BPlusLeafGrain</c> of a tree pinned to
    /// <paramref name="walPartitions"/> WAL partitions would publish (issue
    /// #4238). Equivalent to <see cref="ClassifyConsumerId"/> returning
    /// <see cref="ConsumerIdVerdict.LeafPublished"/>.
    /// </summary>
    /// <param name="treeId">The physical tree id the pin belongs to.</param>
    /// <param name="consumerId">The materialiser consumer id.</param>
    /// <param name="walPartitions">The tree's registry-pinned WAL partition count.</param>
    /// <returns><see langword="true"/> only when the id is provably a leaf's own.</returns>
    internal static bool IsLeafPublishedConsumerId(string treeId, string consumerId, int walPartitions) =>
        ClassifyConsumerId(treeId, consumerId, walPartitions) == ConsumerIdVerdict.LeafPublished;

    /// <summary>
    /// Judges whether <paramref name="consumerId"/> is exactly the materialiser
    /// consumer id a <c>BPlusLeafGrain</c> of a tree pinned to
    /// <paramref name="walPartitions"/> WAL partitions would publish, and when it
    /// is not, why (issues #4238, #4246). This is the gate every pin
    /// <i>removal</i> passes, and it is deliberately stricter than
    /// <see cref="TryParseConsumerId"/>, which the read-only diagnostics share.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A removal is authorised by a storage read of the parsed leaf finding no
    /// record, or a record with no tree id. That evidence is about the parsed
    /// grain, not about the publisher of the pin, so it is only evidence about
    /// the publisher when the parse is provably the inverse of how the leaf
    /// built the id. A parse that names some other grain - a partition suffix
    /// left on, a grain-id suffix taken off - reads an absent record and would
    /// delete the pin of a leaf that is still live, letting the GC trim WAL that
    /// leaf has not replayed.
    /// </para>
    /// <para>
    /// So the id is accepted only when all of these hold: it parses; the leaf
    /// key is a 32-hex guid in the canonical form Orleans renders a guid key in,
    /// which is the only key shape a <c>BPlusLeafGrain</c> has; the partition is
    /// canonical and in range of the pinned count; and rebuilding the id from the
    /// parsed leaf and partition, exactly as the leaf builds it (unsuffixed on a
    /// single-partition tree, suffixed on a partitioned one), gives back the same
    /// string. Anything that fails is left where it is: holding the floor costs
    /// retained WAL, never data.
    /// </para>
    /// <para>
    /// A refusal is split in two so it can be counted rather than folded into
    /// "did not parse" (issue #4246). <see cref="ConsumerIdVerdict.AmbiguousPartition"/>
    /// is an id whose partition cannot be read unambiguously against the pinned
    /// count: an out-of-range or non-canonical suffix, a missing suffix on a
    /// partitioned tree, a non-positive count, or - the #4238 shape - a canonical
    /// leaf followed by a partition suffix on a tree the pass believes is
    /// single-partition. <see cref="ConsumerIdVerdict.MalformedId"/> is every
    /// other refusal: another tree's prefix, an id that does not parse as a
    /// grain id, or a leaf key that is not a canonical guid.
    /// </para>
    /// </remarks>
    /// <param name="treeId">The physical tree id the pin belongs to.</param>
    /// <param name="consumerId">The materialiser consumer id.</param>
    /// <param name="walPartitions">The tree's registry-pinned WAL partition count.</param>
    /// <returns>The verdict; only <see cref="ConsumerIdVerdict.LeafPublished"/> authorises a removal.</returns>
    internal static ConsumerIdVerdict ClassifyConsumerId(string treeId, string consumerId, int walPartitions)
    {
        if (walPartitions < 1)
        {
            return ConsumerIdVerdict.AmbiguousPartition;
        }

        var verdict = Parse(
            treeId, consumerId, walPartitions, out var leafGrainId, out var partition, out var suffixed, out var trailingDigits);
        if (verdict != ConsumerIdVerdict.LeafPublished)
        {
            return verdict;
        }

        if (walPartitions > 1 && !suffixed)
        {
            return ConsumerIdVerdict.AmbiguousPartition;
        }

        if (!IsCanonicalGuidKey(leafGrainId.Key.ToString()))
        {
            // A canonical leaf with a partition suffix left on, on a tree read as
            // single-partition: the leaf is recognisable, its partition is not.
            return walPartitions == 1
                && trailingDigits > 0
                && GrainId.TryParse(leafGrainId.ToString()[..^(trailingDigits + 1)], out var stripped)
                && IsCanonicalGuidKey(stripped.Key.ToString())
                ? ConsumerIdVerdict.AmbiguousPartition
                : ConsumerIdVerdict.MalformedId;
        }

        var expected = walPartitions == 1
            ? $"{BPlusTree.Grains.ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{leafGrainId}"
            : $"{BPlusTree.Grains.ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{leafGrainId}_{partition.ToString(CultureInfo.InvariantCulture)}";

        return string.Equals(expected, consumerId, StringComparison.Ordinal)
            ? ConsumerIdVerdict.LeafPublished
            : ConsumerIdVerdict.MalformedId;
    }

    /// <summary>
    /// The one consumer-id grammar both <see cref="TryParseConsumerId"/> and
    /// <see cref="ClassifyConsumerId"/> read through (issue #4242).
    /// </summary>
    /// <param name="treeId">The physical tree id the pin belongs to.</param>
    /// <param name="consumerId">The materialiser consumer id.</param>
    /// <param name="walPartitions">The tree's WAL partition count.</param>
    /// <param name="leafGrainId">The parsed leaf, when the id parsed.</param>
    /// <param name="partition">The parsed partition, when the id parsed.</param>
    /// <param name="suffixed">Whether a partition suffix was stripped.</param>
    /// <param name="trailingDigits">
    /// The length of a trailing suffix-shaped group (<c>_&lt;sign?&gt;&lt;digits&gt;</c>,
    /// excluding the separator) that was NOT stripped because the tree is
    /// single-partition, else <c>0</c>.
    /// </param>
    /// <returns>
    /// <see cref="ConsumerIdVerdict.LeafPublished"/> when the id parsed (the leaf
    /// key is not yet judged), otherwise the refusal.
    /// </returns>
    private static ConsumerIdVerdict Parse(
        string treeId,
        string consumerId,
        int walPartitions,
        out GrainId leafGrainId,
        out int partition,
        out bool suffixed,
        out int trailingDigits)
    {
        leafGrainId = default;
        partition = 0;
        suffixed = false;
        trailingDigits = 0;

        var expectedStart = $"{BPlusTree.Grains.ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_";
        if (!consumerId.StartsWith(expectedStart, StringComparison.Ordinal)
            || consumerId.Length == expectedStart.Length)
        {
            return ConsumerIdVerdict.MalformedId;
        }

        var remainder = consumerId.AsSpan(expectedStart.Length);
        var lastSeparator = remainder.LastIndexOf('_');
        var tail = lastSeparator > 0 ? remainder[(lastSeparator + 1)..] : ReadOnlySpan<char>.Empty;
        if (IsSuffixShaped(tail))
        {
            if (walPartitions > 1)
            {
                if (!TryParseCanonicalPartition(tail, walPartitions, out partition))
                {
                    partition = 0;
                    return ConsumerIdVerdict.AmbiguousPartition;
                }

                remainder = remainder[..lastSeparator];
                suffixed = true;
            }
            else
            {
                trailingDigits = tail.Length;
            }
        }

        if (!GrainId.TryParse(remainder.ToString(), out leafGrainId))
        {
            partition = 0;
            suffixed = false;
            trailingDigits = 0;
            return ConsumerIdVerdict.MalformedId;
        }

        return ConsumerIdVerdict.LeafPublished;
    }

    /// <summary>
    /// Whether <paramref name="tail"/> reads as an attempted partition suffix:
    /// an optional sign followed by one or more ASCII digits.
    /// </summary>
    private static bool IsSuffixShaped(ReadOnlySpan<char> tail)
    {
        if (tail.Length > 0 && (tail[0] == '+' || tail[0] == '-'))
        {
            tail = tail[1..];
        }

        return tail.Length > 0 && IsAsciiDigits(tail);
    }

    /// <summary>Whether <paramref name="digits"/> is canonical decimal: digits only, no leading zero.</summary>
    private static bool IsCanonicalDigits(ReadOnlySpan<char> digits) =>
        digits.Length > 0
        && IsAsciiDigits(digits)
        && (digits.Length == 1 || digits[0] != '0');

    /// <summary>
    /// Reads a canonical decimal partition below <paramref name="walPartitions"/>.
    /// </summary>
    private static bool TryParseCanonicalPartition(ReadOnlySpan<char> digits, int walPartitions, out int partition)
    {
        partition = 0;
        return IsCanonicalDigits(digits)
            && int.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out partition)
            && partition < walPartitions;
    }

    private static bool IsAsciiDigits(ReadOnlySpan<char> span)
    {
        foreach (var c in span)
        {
            if (!char.IsAsciiDigit(c))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsCanonicalGuidKey(string key) =>
        key is { Length: 32 }
        && Guid.TryParseExact(key, "N", out var guid)
        && string.Equals(guid.ToString("N"), key, StringComparison.Ordinal);

    /// <summary>
    /// Reads one leaf's persisted projection checkpoint for a partition directly
    /// from the storage provider and maps it onto a
    /// <see cref="WalGcBlockingPinState"/>, returning the numeric checkpoint the
    /// state was derived from (<see langword="null"/> when none was read).
    /// Never activates the leaf.
    /// </summary>
    /// <remarks>
    /// A storage fault propagates: the caller decides how to log it and maps it
    /// to <see cref="WalGcBlockingPinState.Unreadable"/>. A missing storage
    /// provider is a property of the measurement rather than of the leaf, so it
    /// reads as <see cref="WalGcBlockingPinState.Unreadable"/>, never as an
    /// absence of durable state.
    /// </remarks>
    /// <param name="storage">The leaf state storage provider, or <see langword="null"/> when this silo has none.</param>
    /// <param name="leafGrainId">The leaf to read.</param>
    /// <param name="partition">The WAL partition whose checkpoint to read.</param>
    /// <returns>The classification and the persisted checkpoint behind it.</returns>
    internal static async Task<(WalGcBlockingPinState State, long? Checkpoint)> ReadLeafCheckpointAsync(
        IGrainStorage? storage,
        GrainId leafGrainId,
        int partition)
    {
        if (storage is null)
        {
            return (WalGcBlockingPinState.Unreadable, null);
        }

        var grainState = new GrainState<LeafNodeState>(new LeafNodeState());
        await storage.ReadStateAsync(LeafStateName, leafGrainId, grainState);

        if (!grainState.RecordExists || grainState.State is null)
        {
            return (WalGcBlockingPinState.NoDurableState, null);
        }

        // The tree id is read before the checkpoint (issue #3105): a pin can only
        // exist if the leaf carried a tree id when it was written, so finding none
        // proves the state was cleared afterwards and the pin outlived its
        // publisher.
        if (string.IsNullOrEmpty(grainState.State.TreeId))
        {
            return (WalGcBlockingPinState.Orphaned, null);
        }

        return (
            LatticeWalGcScheduler.ClassifyCheckpoint(grainState.State, partition),
            LatticeWalGcScheduler.ReadPersistedCheckpoint(grainState.State, partition));
    }
}
