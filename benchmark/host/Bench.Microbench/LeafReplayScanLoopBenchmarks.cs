using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the leaf WAL replay scan loop - the <c>while</c> loop in
/// <c>BPlusLeafGrain.Activation.cs</c> that drives a <see cref="ReplaySliceReader"/>
/// across a partition - so the cost of its upper bound is measurable with no
/// Orleans cluster in the loop.
/// <para>
/// Issue #3489: the WAL head is the exclusive next sequence, but the reader's
/// upper bound is inclusive. The pre-fix loop bounded both its condition and
/// its read by <c>head</c>, so after consuming the newest real entry it made one
/// further read for an offset that does not exist, which came back empty. The
/// <c>Baseline_HeadBound</c> arm runs that loop; the <c>Fixed_NewestOffsetBound</c>
/// arm bounds by <c>head - 1</c>, which consumes the same entries with one read
/// fewer per pass.
/// </para>
/// <para>
/// The coordinator is an in-memory fake whose slices and completed tasks are
/// built once in <see cref="Setup"/>, so the measured allocation is the
/// reader's own per-read cost and nothing the fake contributes. Applying the
/// entries is deliberately excluded: both arms apply the identical set, and
/// only the read count differs. Each arm returns the number of reads it made,
/// so a regression in either bound shows up in the result as well as the time.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=replayscanloop</c> (or
/// <c>--suite replayscanloop</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LeafReplayScanLoopBenchmarks
{
    private const int SliceBudget = 64;

    private FakeCoordinator _coordinator = null!;
    private long _head;

    /// <summary>Number of WAL entries the replay pass consumes.</summary>
    [Params(64, 1024)]
    public int EntryCount { get; set; }

    /// <summary>Builds the fake coordinator's slices for <see cref="EntryCount"/> entries.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _coordinator = new FakeCoordinator(EntryCount, SliceBudget);
        _head = EntryCount;
    }

    /// <summary>The pre-#3489 loop: condition and read both bounded by the exclusive head.</summary>
    /// <returns>The number of reads the pass made.</returns>
    [Benchmark(Baseline = true)]
    public Task<int> Baseline_HeadBound() => ScanAsync(_head);

    /// <summary>The #3489 loop: condition and read both bounded by the newest offset, <c>head - 1</c>.</summary>
    /// <returns>The number of reads the pass made.</returns>
    [Benchmark]
    public Task<int> Fixed_NewestOffsetBound() => ScanAsync(_head - 1);

    private async Task<int> ScanAsync(long bound)
    {
        var reader = new ReplaySliceReader(_coordinator, "t", 0, SliceBudget);
        var fromExclusive = -1L;
        var reads = 0;
        while (fromExclusive < bound)
        {
            var slice = await reader.ReadSliceAsync(fromExclusive, bound, null, CancellationToken.None);
            reads++;
            if (slice.Count == 0)
            {
                break;
            }

            fromExclusive = slice[slice.Count - 1].Offset;
        }

        return reads;
    }

    /// <summary>
    /// In-memory coordinator serving entries at offsets <c>0 .. entryCount - 1</c>
    /// with <c>(from, to]</c> semantics capped at the budget. Every slice and
    /// its completed task is pre-built per starting offset, so a read allocates
    /// nothing.
    /// </summary>
    private sealed class FakeCoordinator : ILeafReplayCoordinatorGrain
    {
        private static readonly Task<IReadOnlyList<CommitLogSliceEntry>> Empty =
            Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>());

        private readonly int _entryCount;
        private readonly int _budget;
        private readonly Task<IReadOnlyList<CommitLogSliceEntry>>[] _slicesByStart;

        public FakeCoordinator(int entryCount, int budget)
        {
            _entryCount = entryCount;
            _budget = budget;
            var mutation = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "k", ShardIndex = 0 };
            var entries = new CommitLogSliceEntry[entryCount];
            for (var i = 0; i < entryCount; i++)
            {
                entries[i] = new CommitLogSliceEntry(i, mutation);
            }

            _slicesByStart = new Task<IReadOnlyList<CommitLogSliceEntry>>[entryCount];
            for (var start = 0; start < entryCount; start++)
            {
                var count = Math.Min(budget, entryCount - start);
                _slicesByStart[start] = Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
                    new ArraySegment<CommitLogSliceEntry>(entries, start, count));
            }
        }

        public Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
            long fromOffsetExclusive,
            long toOffsetInclusive,
            int budget,
            CancellationToken cancellationToken = default)
        {
            var start = fromOffsetExclusive + 1;
            var newest = Math.Min(toOffsetInclusive, _entryCount - 1);
            if (start > newest)
            {
                return Empty;
            }

            if (newest != _entryCount - 1 || budget != _budget)
            {
                throw new InvalidOperationException("The fake serves only full-log reads at its configured budget.");
            }

            return _slicesByStart[start];
        }

        public Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
            long fromOffsetExclusive,
            long toOffsetInclusive,
            int budget,
            WalKeyFilter filter,
            CancellationToken cancellationToken = default) =>
            ReadSliceAsync(fromOffsetExclusive, toOffsetInclusive, budget, cancellationToken);

        public Task<long> GetHeadOffsetAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult((long)_entryCount);

        public Task<long> GetTailOffsetAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(0L);
    }
}
