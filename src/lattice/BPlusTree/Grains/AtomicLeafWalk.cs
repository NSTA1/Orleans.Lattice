using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Diagnostic for a leaf-chain walk that is deliberately <b>not</b> work-bounded
/// because its whole-walk atomicity is load-bearing.
/// <para>
/// Most shard range-scan walks are bounded by <see cref="LeafWalkBudget"/>
/// (issue 1955), and the background coordinators' drains by
/// <see cref="BoundedLeafWalk"/> (issue 1973). A handful cannot be: for them,
/// the fact that no other message runs on the grain between the first and last
/// leaf is precisely the invariant the surrounding protocol depends on.
/// Releasing the non-reentrant grain mid-walk would open exactly the window
/// those protocols exist to close - see the per-site notes at each call site,
/// and issue 1956 for the analysis.
/// </para>
/// <para>
/// Since they cannot be bounded, they are instead made <em>attributable</em>.
/// When one of these walks holds the grain longer than
/// <see cref="WarnAfter"/>, it logs a warning naming the operation and the
/// number of leaves it visited. Without that, a stalled grain surfaces only as
/// a flood of Orleans <c>Dispatcher_ExtendedMessageProcessing</c> warnings that
/// name the <em>blocked</em> messages and say nothing about what is blocking
/// them, which is what made issue 1953 hard to diagnose.
/// </para>
/// </summary>
internal struct AtomicLeafWalk
{
    /// <summary>
    /// How long an atomic walk may hold the shard before it is reported.
    /// <para>
    /// Deliberately a constant rather than a <see cref="LatticeOptions"/> knob:
    /// it changes no behaviour, only whether a warning is emitted, so there is
    /// nothing for an operator to tune and no way to mis-tune it into a
    /// correctness problem. The value sits well below Orleans' own
    /// <c>MaxWarningRequestProcessingTime</c> (<c>ResponseTimeout x 5</c>, 150s
    /// by default) so this warning lands <em>before</em> the message-pump
    /// warning flood it explains, and well above any healthy walk.
    /// </para>
    /// </summary>
    internal static readonly TimeSpan WarnAfter = TimeSpan.FromSeconds(10);

    private readonly string _operation;
    private readonly TimeSpan _warnAfter;
    private readonly long _startTimestamp;
    private int _leavesVisited;

    internal AtomicLeafWalk(string operation) : this(operation, WarnAfter)
    {
    }

    /// <summary>
    /// Overload taking an explicit threshold, so a test can exercise the
    /// reporting path without sleeping for <see cref="WarnAfter"/>.
    /// </summary>
    internal AtomicLeafWalk(string operation, TimeSpan warnAfter)
    {
        _operation = operation;
        _warnAfter = warnAfter;
        _startTimestamp = Stopwatch.GetTimestamp();
        _leavesVisited = 0;
    }

    /// <summary>Leaves visited so far.</summary>
    internal readonly int LeavesVisited => _leavesVisited;

    /// <summary>Records that one leaf has been visited.</summary>
    internal void RecordLeafVisited() => _leavesVisited++;

    /// <summary>
    /// Records that <paramref name="count"/> leaves have been visited, for a
    /// caller whose walk is executed by a shared helper that already counted
    /// them.
    /// </summary>
    internal void RecordLeavesVisited(int count) => _leavesVisited += count;

    /// <summary>
    /// Reports the walk if it held the shard longer than <see cref="WarnAfter"/>.
    /// Call once, after the walk completes. Never throws and never alters the
    /// walk's outcome: this is a diagnostic, not a control path.
    /// </summary>
    internal readonly void ReportIfSlow(ILogger logger, GrainId grainId)
    {
        if (logger is null)
        {
            return;
        }

        var elapsed = Stopwatch.GetElapsedTime(_startTimestamp);
        if (elapsed < _warnAfter)
        {
            return;
        }

        logger.LogWarning(
            "Grain {GrainId} held its activation for {ElapsedMs} ms in {Operation}, visiting {LeavesVisited} leaves. " +
            "This walk is intentionally atomic and is not work-bounded, so the grain is unavailable to every other " +
            "caller for its duration (issues 1956, 1973). On a shard root that blocks every request to the shard; on a " +
            "background coordinator it blocks progress queries and cancellation for that operation. A large leaf count " +
            "here explains a concurrent burst of Orleans long-request warnings.",
            grainId,
            (long)elapsed.TotalMilliseconds,
            _operation,
            _leavesVisited);
    }
}
