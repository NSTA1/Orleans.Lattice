using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Whether a <see cref="WalMoveQuiesceModel"/> run performs the move-fence check
/// and the offset assignment as one atomic step (the production shape, both under
/// the grain's state gate) or splits them, so the safety test can prove the
/// atomicity is load-bearing by removing it and asserting Coyote re-finds an
/// append that assigns an offset after the move fence was raised.
/// </summary>
public enum WalMoveQuiesceMode
{
    /// <summary>
    /// The fix: a writer reads the fence and, if admitted, assigns its offset in
    /// one indivisible step - exactly as <c>WalShardGrain</c> does under
    /// <c>_stateGate</c>. No quiesce can raise the fence between the check and the
    /// assignment, so no offset is ever committed after the source tail is fenced.
    /// </summary>
    AtomicFenceCheck,

    /// <summary>
    /// The guard removed: the writer reads the fence, yields, and only then
    /// assigns its offset. A quiesce that raises the fence in the gap is bypassed
    /// by the already-admitted writer, which assigns an offset after the
    /// coordinator captured the stable tail - the entry the move never copies, a
    /// silent placement-move data loss.
    /// </summary>
    NonAtomicFenceCheck,
}

/// <summary>
/// A Coyote concurrency model of the WAL placement-move quiesce fence racing
/// concurrent writers, driving the <b>production</b> admission rule
/// (<see cref="WalMoveFenceCore.IsAppendAdmitted"/>) that <c>WalShardGrain</c>
/// applies under its state gate. Because the model executes the same fence
/// decision Orleans runs, a violation Coyote finds is a violation of the real
/// move path.
/// <para>
/// The scenario interleaves <c>writerCount</c> writers - each a two-phase
/// {check fence, assign offset} state machine - against a coordinator that raises
/// the fence and then captures the highest assigned offset as the stable tail to
/// copy. The Coyote scheduler explores every interleaving of writer phases and
/// the quiesce.
/// </para>
/// <para>
/// The safety property is <b>no offset is assigned once the fence is raised</b>:
/// every admitted writer must assign its offset while the activation is still
/// unfenced. An offset committed after the fence sits above the tail the
/// coordinator copied and is stranded on the abandoned source provider.
/// </para>
/// </summary>
public sealed class WalMoveQuiesceModel : ICoyoteModel
{
    private readonly int _writerCount;
    private readonly WalMoveQuiesceMode _mode;

    // Shared activation state the writers and the quiesce contend over.
    private bool _moveFenced;
    private long _nextOffset;
    private bool _quiesceCaptured;
    private long _capturedHighest;

    /// <summary>
    /// Creates the model for <paramref name="writerCount"/> concurrent writers
    /// racing the quiesce under the chosen atomicity <paramref name="mode"/>.
    /// </summary>
    public WalMoveQuiesceModel(int writerCount, WalMoveQuiesceMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(writerCount, 1);
        _writerCount = writerCount;
        _mode = mode;
    }

    private enum WriterPhase
    {
        NotStarted,
        Checked,
        Done,
    }

    private enum QuiescePhase
    {
        NotStarted,
        Fenced,
        Captured,
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        _moveFenced = false;
        _nextOffset = 0;
        _quiesceCaptured = false;
        _capturedHighest = -1;

        var writerPhase = new WriterPhase[_writerCount];
        var writerAdmitted = new bool[_writerCount];
        var quiesce = QuiescePhase.NotStarted;

        // Advance one scheduler-chosen actor by one phase per iteration until the
        // quiesce has captured the tail and every writer has resolved. Each step
        // is finite and every actor strictly advances, so exploration terminates.
        while (quiesce != QuiescePhase.Captured || AnyWriterPending(writerPhase))
        {
            var driveWriter = AnyWriterPending(writerPhase)
                && (quiesce == QuiescePhase.Captured || runtime.RandomBoolean());

            if (driveWriter)
            {
                var w = SelectPendingWriter(writerPhase, runtime);
                StepWriter(writerPhase, writerAdmitted, w);
            }
            else
            {
                quiesce = StepQuiesce(quiesce);
            }
        }
    }

    /// <summary>Advances one writer's {check, assign} state machine by a single phase.</summary>
    private void StepWriter(WriterPhase[] writerPhase, bool[] writerAdmitted, int writer)
    {
        switch (writerPhase[writer])
        {
            case WriterPhase.NotStarted:
                // Drive the real production admission decision.
                writerAdmitted[writer] = WalMoveFenceCore.IsAppendAdmitted(_moveFenced);

                if (_mode == WalMoveQuiesceMode.AtomicFenceCheck)
                {
                    // Atomic: the assignment happens in the same indivisible step
                    // as the check, so no quiesce can interleave between them.
                    CommitIfAdmitted(writerAdmitted[writer]);
                    writerPhase[writer] = WriterPhase.Done;
                }
                else
                {
                    // Non-atomic: yield after the check. The quiesce may now fence
                    // before this writer assigns its offset in the next step.
                    writerPhase[writer] = WriterPhase.Checked;
                }

                break;

            case WriterPhase.Checked:
                CommitIfAdmitted(writerAdmitted[writer]);
                writerPhase[writer] = WriterPhase.Done;
                break;
        }
    }

    /// <summary>
    /// Assigns the next offset for an admitted writer and asserts the fence is
    /// still down - the invariant the atomic check guarantees and the split check
    /// violates.
    /// </summary>
    private void CommitIfAdmitted(bool admitted)
    {
        if (!admitted)
        {
            return;
        }

        Specification.Assert(
            !_moveFenced,
            "a WAL writer assigned an offset after the move fence was raised: the coordinator's "
            + "captured tail misses it, stranding the entry on the abandoned source provider "
            + "(placement-move data loss)");

        var offset = _nextOffset++;

        // An offset committed after the coordinator captured the tail is above it
        // by construction; assert directly so the stranded-entry class is caught
        // even if the fence were (incorrectly) lowered before capture.
        if (_quiesceCaptured)
        {
            Specification.Assert(
                offset <= _capturedHighest,
                $"WAL writer assigned offset {offset} above the captured stable tail {_capturedHighest}: "
                + "the placement move copied a tail that misses this entry");
        }
    }

    /// <summary>Advances the coordinator's fence-then-capture quiesce by one phase.</summary>
    private QuiescePhase StepQuiesce(QuiescePhase quiesce)
    {
        switch (quiesce)
        {
            case QuiescePhase.NotStarted:
                _moveFenced = true;
                return QuiescePhase.Fenced;

            case QuiescePhase.Fenced:
                // Fence is up and in-flight writers have drained; capture the
                // highest assigned offset as the stable tail to copy.
                _capturedHighest = _nextOffset - 1;
                _quiesceCaptured = true;
                return QuiescePhase.Captured;

            default:
                return quiesce;
        }
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
    /// runtime so the harness explores every writer/quiesce interleaving.
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
