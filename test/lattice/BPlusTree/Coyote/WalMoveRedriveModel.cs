using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which resume rule a <see cref="WalMoveRedriveModel"/> run drives, so the
/// safety test can prove the resume-past-target cursor is load-bearing by
/// removing it (always resuming from the source floor, ignoring what the target
/// already holds) and asserting Coyote re-finds an offset copied to the target
/// twice.
/// </summary>
public enum WalMoveRedriveMode
{
    /// <summary>
    /// The fix: each re-drive resumes the copy just past the higher of the
    /// reserved source floor and the target's current highest offset, through the
    /// production core (<see cref="Orleans.Lattice.WalMoveResumeCore.ResumeCursor"/>)
    /// exactly as <c>LatticeAdminGrain.RunMoveCopyPhasesAsync</c> does. However an
    /// interrupted copy left the target, the next attempt appends only the missing
    /// suffix, so every source offset lands on the target exactly once.
    /// </summary>
    ResumePastTarget,

    /// <summary>
    /// The guard removed: every re-drive resumes from the source floor
    /// (<c>srcLowest - 1</c>) regardless of what the target already holds, as if
    /// the copy always restarted from the beginning of the retained tail. A
    /// re-drive after a partial copy then re-appends offsets the target already
    /// has, and Coyote finds the duplicate - a torn target log in which an offset
    /// appears twice.
    /// </summary>
    ResumeFromFloorAlways,
}

/// <summary>
/// A Coyote concurrency model of the WAL placement move's <b>resumable tail
/// copy</b> under a coordinator that crashes and re-drives mid-copy, driving the
/// <b>production</b> resume arithmetic
/// (<see cref="Orleans.Lattice.WalMoveResumeCore.ResumeCursor"/>) that
/// <c>LatticeAdminGrain.RunMoveCopyPhasesAsync</c> applies before flipping the
/// placement pin. Because the model executes the same cursor computation Orleans
/// runs, a violation Coyote finds is a violation of the real move path.
/// <para>
/// The source retains a dense, offset-preserving tail
/// <c>[srcLowest..srcHighest]</c> that must be copied to the target before the
/// pin flips. An attempt copies a scheduler-chosen prefix of the still-missing
/// suffix and then may "crash" (the coordinator aborts and re-drives), leaving a
/// partial copy on the target; the next attempt recomputes its resume cursor from
/// the target's current highest offset. The Coyote scheduler explores every crash
/// point, so the copy is interrupted and resumed at every offset boundary.
/// </para>
/// <para>
/// The safety property is <b>copy each offset exactly once</b>: no source offset
/// is ever appended to the target twice (a duplicate, asserted at each append)
/// and, once the copy completes, the target holds exactly <c>[srcLowest..
/// srcHighest]</c> with no gap. This is the idempotent-re-drive guarantee that
/// makes an interrupted move always safe to retry - it holds only because the
/// resume cursor is taken past what the target already holds, not from a fixed
/// point.
/// </para>
/// </summary>
public sealed class WalMoveRedriveModel : ICoyoteModel
{
    private readonly long _srcLowest;
    private readonly long _srcHighest;
    private readonly WalMoveRedriveMode _mode;

    /// <summary>
    /// Creates the model for a source retained tail of
    /// <paramref name="tailLength"/> dense offsets under the chosen resume
    /// <paramref name="mode"/>. The tail starts at a non-zero floor so the
    /// reserved-floor arithmetic (<c>srcLowest - 1</c>) is exercised.
    /// </summary>
    public WalMoveRedriveModel(int tailLength, WalMoveRedriveMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(tailLength, 1);

        // A non-zero floor: the source trimmed a prefix, so the retained tail is
        // [srcLowest..srcHighest] and the destination floor must be reserved.
        _srcLowest = 2;
        _srcHighest = _srcLowest + tailLength - 1;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // The offsets already landed on the target, and the target's current
        // highest offset (-1 when empty, matching GetHighestOffsetAsync).
        var copied = new HashSet<long>();
        var dstHighest = -1L;

        // Re-drive until the target holds the whole retained tail. Each attempt
        // copies at least one offset (from just past the resume cursor), so the
        // target's highest strictly advances and the loop terminates.
        while (dstHighest < _srcHighest)
        {
            var resumeFrom = _mode == WalMoveRedriveMode.ResumePastTarget
                ? Orleans.Lattice.WalMoveResumeCore.ResumeCursor(_srcLowest, dstHighest)
                // The removed guard: always resume from the source floor, ignoring
                // the prefix a prior attempt already landed on the target.
                : _srcLowest - 1;

            // This attempt copies (resumeFrom, crashAt]; the scheduler picks how
            // far it gets before the coordinator crashes and re-drives.
            var firstOffset = resumeFrom + 1;
            var crashAt = SelectCrashPoint(firstOffset, runtime);

            for (var offset = firstOffset; offset <= crashAt; offset++)
            {
                // Exactly-once: appending an offset the target already holds is a
                // torn log - the duplicate the resume cursor exists to prevent.
                Specification.Assert(
                    copied.Add(offset),
                    $"source offset {offset} was copied to the target twice: the resume cursor did not skip "
                    + "the prefix a prior attempt already landed (duplicate WAL entry after re-drive)");
                dstHighest = offset;
            }
        }

        // Density: the completed copy holds exactly the retained tail, no gap.
        for (var offset = _srcLowest; offset <= _srcHighest; offset++)
        {
            Specification.Assert(
                copied.Contains(offset),
                $"source offset {offset} is missing from the target after the copy completed: the resume cursor "
                + "skipped it (gap in the moved WAL tail)");
        }

        Specification.Assert(
            copied.Count == _srcHighest - _srcLowest + 1,
            $"target holds {copied.Count} offsets but the retained tail is {_srcHighest - _srcLowest + 1}: "
            + "the re-driven copy is not an exact reproduction of the source tail");
    }

    /// <summary>
    /// Picks how far this attempt copies before the coordinator crashes and
    /// re-drives, driving the choice through the runtime so the harness explores
    /// every crash point from "copied one offset" up to "copied the whole
    /// remaining suffix" (no crash).
    /// </summary>
    private long SelectCrashPoint(long firstOffset, ICoyoteRuntime runtime)
    {
        // Always make progress (copy at least firstOffset), then extend the run
        // one offset at a time while the scheduler keeps choosing to continue.
        var crashAt = firstOffset;
        while (crashAt < _srcHighest && runtime.RandomBoolean())
        {
            crashAt++;
        }

        return crashAt;
    }
}
