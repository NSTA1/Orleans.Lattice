using System.Runtime.CompilerServices;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The rule a filtered WAL read follows (issue #3565), shared by every layer of
/// the replay read path so they cannot disagree about what a filtered window
/// contains.
/// <para>
/// A filtered read examines the entries of a window in ascending offset order,
/// at most a stated number of them, and delivers every examined entry the
/// <see cref="WalKeyFilter"/> does not exclude, in full. Excluded entries are
/// dropped, with one exception: when the <b>last entry examined</b> is excluded
/// it is delivered <i>routing-only</i> - its offset, kind and key exact, every
/// other field default. That trailing entry is what keeps a reader that advances
/// by the last offset it receives moving past everything the filter dropped:
/// without it a window whose tail is foreign would read as shorter than it is,
/// and a leaf owning nothing near the head of a quiescent partition could never
/// reach it, which the scanned-through checkpoint of issue #2270 depends on. It
/// also means a filtered read is empty exactly when its window holds no entry,
/// so an empty page still means end-of-window to every reader.
/// </para>
/// <para>
/// Dropping is safe because a reader judges each record with the same ownership
/// the filter encodes: an excluded record is one it would reject. The trailing
/// routing-only entry carries its real kind and key, so the same judgement
/// rejects it too.
/// </para>
/// <para>
/// The examined-entry bound is what keeps a filtered slice's work equal to an
/// unfiltered slice's. Counting only delivered entries instead would let one
/// read scan an entire partition gap for a leaf that owns little of it, turning
/// a bounded slice into an unbounded grain call.
/// </para>
/// <para>
/// <b>Scheduler affinity.</b> Like <see cref="BoundedFanOut"/>, this never calls
/// <c>ConfigureAwait(false)</c>: it runs inside the WAL shard and replay
/// coordinator grains' turns, where resuming off the activation's task scheduler
/// would drop <c>RequestContext</c> - and with it the system-origin scope the WAL
/// grain's internal-origin guard checks - and break the single-threaded activation
/// contract.
/// </para>
/// </summary>
internal static class WalFilteredRead
{
    /// <summary>
    /// The routing-only projection of <paramref name="mutation"/>: tree id, kind
    /// and key, every other field default.
    /// </summary>
    public static LatticeMutation RoutingOnly(in LatticeMutation mutation) => new()
    {
        TreeId = mutation.TreeId,
        Kind = mutation.Kind,
        Key = mutation.Key,
    };

    /// <summary>
    /// The routing-only projection of <paramref name="record"/>: tree id,
    /// operation and key, every other field default.
    /// </summary>
    public static WalRecord RoutingOnly(in WalRecord record) => new()
    {
        TreeId = record.TreeId,
        Op = record.Op,
        Key = record.Key,
    };

    /// <summary>
    /// Applies the rule to an ascending entry stream: examines entries up to
    /// <paramref name="toOffsetInclusive"/>, at most
    /// <paramref name="maxExamined"/> of them.
    /// </summary>
    /// <param name="source">Entries in ascending offset order.</param>
    /// <param name="toOffsetInclusive">Inclusive upper bound of the window.</param>
    /// <param name="maxExamined">Maximum entries to examine; must be at least 1.</param>
    /// <param name="filter">The reader's ownership.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    public static async IAsyncEnumerable<WalEntry> ApplyAsync(
        IAsyncEnumerable<WalEntry> source,
        long toOffsetInclusive,
        int maxExamined,
        WalKeyFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxExamined, 1);

        var examined = 0;
        var trailingExcluded = false;
        var trailing = default(WalEntry);
        await foreach (var entry in source.WithCancellation(cancellationToken))
        {
            if (entry.Offset > toOffsetInclusive)
                break;

            examined++;
            if (filter.Excludes(entry.Mutation.Kind, entry.Mutation.Key))
            {
                trailingExcluded = true;
                trailing = entry;
            }
            else
            {
                trailingExcluded = false;
                yield return entry;
            }

            if (examined >= maxExamined)
                break;
        }

        if (trailingExcluded)
            yield return trailing with { Mutation = RoutingOnly(trailing.Mutation) };
    }

    /// <summary>
    /// Applies the rule to an ascending <c>(offset, mutation)</c> stream - the
    /// commit-log reader shape of
    /// <see cref="ApplyAsync(IAsyncEnumerable{WalEntry}, long, int, WalKeyFilter, CancellationToken)"/>.
    /// </summary>
    /// <param name="source">Entries in ascending offset order.</param>
    /// <param name="toOffsetInclusive">Inclusive upper bound of the window.</param>
    /// <param name="maxExamined">Maximum entries to examine; must be at least 1.</param>
    /// <param name="filter">The reader's ownership.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    public static async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ApplyAsync(
        IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> source,
        long toOffsetInclusive,
        int maxExamined,
        WalKeyFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxExamined, 1);

        var examined = 0;
        var trailingExcluded = false;
        var trailingOffset = 0L;
        var trailingMutation = default(LatticeMutation);
        await foreach (var (offset, mutation) in source.WithCancellation(cancellationToken))
        {
            if (offset > toOffsetInclusive)
                break;

            examined++;
            if (filter.Excludes(mutation.Kind, mutation.Key))
            {
                trailingExcluded = true;
                trailingOffset = offset;
                trailingMutation = mutation;
            }
            else
            {
                trailingExcluded = false;
                yield return (offset, mutation);
            }

            if (examined >= maxExamined)
                break;
        }

        if (trailingExcluded)
            yield return (trailingOffset, RoutingOnly(trailingMutation));
    }
}
