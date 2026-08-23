using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Whether a <see cref="WalOffsetContiguityModel"/> run assigns each offset by
/// reading and advancing the shared counter in one atomic step (the production
/// shape, under the grain's state gate) or splits the read from the advance, so
/// the safety test can prove the atomicity is load-bearing by removing it and
/// asserting Coyote re-finds two concurrent appends that are handed the same
/// offset.
/// </summary>
public enum WalOffsetContiguityMode
{
    /// <summary>
    /// The fix: a writer reads the next offset and advances the counter in one
    /// indivisible step by calling <see cref="WalOffsetAllocationCore.Assign"/> -
    /// exactly as <c>WalShardGrain</c> does under <c>_stateGate</c>. No other
    /// writer can observe the same counter value, so every entry gets a unique
    /// offset and the run stays dense.
    /// </summary>
    AtomicAssign,

    /// <summary>
    /// The guard removed: the writer reads the counter, yields, and only then
    /// writes back the advanced value. Two writers that read in the gap observe
    /// the same offset and both stamp it, producing a duplicate offset and a gap
    /// in the run - a torn WAL sequence in which one entry silently overwrites
    /// another's slot.
    /// </summary>
    SplitReadAdvance,
}

/// <summary>
/// A Coyote concurrency model of concurrent WAL appends assigning per-shard
/// sequence numbers (offsets), driving the <b>production</b> allocation step
/// (<see cref="WalOffsetAllocationCore.Assign"/>) that <c>WalShardGrain</c>
/// performs under its state gate for every entry. Because the model executes the
/// same read-and-advance Orleans runs, a violation Coyote finds is a violation of
/// the real append path.
/// <para>
/// The scenario interleaves <c>writerCount</c> single-entry writers - each a
/// {read offset, advance counter} state machine - contending over one shared
/// next-offset counter, exactly the contention the grain's
/// <c>[AlwaysInterleave]</c> append turns create. The Coyote scheduler explores
/// every interleaving of the writers' phases.
/// </para>
/// <para>
/// The safety property is <b>every assigned offset is unique</b> (asserted the
/// moment it is stamped) and the completed run is <b>dense</b> - the assigned
/// offsets are exactly <c>0..writerCount-1</c> with no duplicate and no gap. This
/// is the dense, strictly-ascending offset guarantee the <c>IWalShardGrain</c>
/// append contract promises; it holds only because the read and the advance are
/// one atomic step under the state gate. <see cref="WalMoveQuiesceModel"/> guards
/// the same state-gate region against a shard-move quiesce; this model guards it
/// against a concurrent second appender.
/// </para>
/// </summary>
public sealed class WalOffsetContiguityModel : ICoyoteModel
{
    private readonly int _writerCount;
    private readonly WalOffsetContiguityMode _mode;

    // The shared next-offset counter every writer contends over, and the set of
    // offsets stamped so far (to catch a duplicate the instant it is assigned).
    private long _nextOffset;
    private readonly HashSet<long> _assigned = new();

    /// <summary>
    /// Creates the model for <paramref name="writerCount"/> concurrent
    /// single-entry appenders under the chosen atomicity <paramref name="mode"/>.
    /// </summary>
    public WalOffsetContiguityModel(int writerCount, WalOffsetContiguityMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(writerCount, 1);
        _writerCount = writerCount;
        _mode = mode;
    }

    private enum WriterPhase
    {
        NotStarted,
        Read,
        Done,
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        _nextOffset = 0;
        _assigned.Clear();

        var writerPhase = new WriterPhase[_writerCount];
        var writerScratch = new long[_writerCount];

        // Advance one scheduler-chosen writer by one phase per iteration until
        // every writer has stamped its offset. Each step strictly advances a
        // writer, so exploration terminates.
        while (AnyWriterPending(writerPhase))
        {
            var w = SelectPendingWriter(writerPhase, runtime);
            StepWriter(writerPhase, writerScratch, w);
        }

        // Every writer stamped exactly one offset; a correct run assigns the
        // dense span 0..writerCount-1 with no duplicate and no gap.
        Specification.Assert(
            _assigned.Count == _writerCount && _nextOffset == _writerCount,
            $"WAL offset run is not dense: {_assigned.Count} distinct offsets and next-offset {_nextOffset} "
            + $"for {_writerCount} appends - a duplicate or gap in the sequence");
    }

    /// <summary>Advances one writer's {read, advance} state machine by a single phase.</summary>
    private void StepWriter(WriterPhase[] writerPhase, long[] writerScratch, int writer)
    {
        switch (writerPhase[writer])
        {
            case WriterPhase.NotStarted:
                if (_mode == WalOffsetContiguityMode.AtomicAssign)
                {
                    // Atomic: read and advance in one indivisible step through the
                    // real production core, exactly as WalShardGrain does under
                    // _stateGate.
                    Commit(WalOffsetAllocationCore.Assign(ref _nextOffset));
                    writerPhase[writer] = WriterPhase.Done;
                }
                else
                {
                    // Non-atomic: read the counter and yield. A concurrent writer
                    // may now read the same value before this one advances it.
                    writerScratch[writer] = _nextOffset;
                    writerPhase[writer] = WriterPhase.Read;
                }

                break;

            case WriterPhase.Read:
                // Write back the advanced counter and stamp the offset read in the
                // previous phase - the split that lets two writers collide.
                _nextOffset = writerScratch[writer] + 1;
                Commit(writerScratch[writer]);
                writerPhase[writer] = WriterPhase.Done;
                break;
        }
    }

    /// <summary>
    /// Records a stamped offset and asserts it was not already handed to another
    /// writer - the uniqueness half of the dense-offset invariant.
    /// </summary>
    private void Commit(long offset)
    {
        Specification.Assert(
            _assigned.Add(offset),
            $"two WAL appends were assigned the same offset {offset}: the read-and-advance was not atomic, "
            + "so one entry silently overwrites the other's sequence slot (torn WAL sequence)");
    }

    private static bool AnyWriterPending(WriterPhase[] writerPhase)
    {
        for (var i = 0; i < writerPhase.Length; i++)
        {
            if (writerPhase[i] != WriterPhase.Done)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Picks which pending writer advances next, driving the choice through the
    /// runtime so the harness explores every writer interleaving.
    /// </summary>
    private static int SelectPendingWriter(WriterPhase[] writerPhase, ICoyoteRuntime runtime)
    {
        var fallback = -1;
        for (var i = 0; i < writerPhase.Length; i++)
        {
            if (writerPhase[i] == WriterPhase.Done)
            {
                continue;
            }

            if (fallback < 0)
            {
                fallback = i;
            }

            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return fallback;
    }
}
