using System.Diagnostics;
using System.Globalization;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounds the <em>whole caller-visible envelope</em> of a batched write across
/// its sequential stages, so stages that are each individually inside the
/// caller's deadline cannot sum past it unnoticed.
/// <para>
/// <b>This exists because a per-stage bound cannot see a sum.</b>
/// <c>LatticeGrain.SetManyAsync</c> runs <c>gate</c> (arming the tree's
/// background loops), then <c>route</c>, <c>bucket</c> and <c>fanout</c>. Each
/// carries its own independent stage timer and, before issue #2685, no shared
/// deadline. The incident that issue records measured a <c>gate</c> of
/// 4,108.96 ms and a <c>fanout</c> of 26,709.17 ms against a 30,000 ms Orleans
/// response timeout: 30,818 ms in total, with <b>neither stage breaching on its
/// own</b>. A budget that bounds only the dominant stage - which is what
/// <see cref="LatticeOptions.SetManyFanOutBudget"/> did alone - never fires in
/// that regime, because 26.7 s is comfortably inside any budget sized for the
/// fan-out. The caller is handed an anonymous <see cref="TimeoutException"/>
/// from Orleans with nothing naming which stage moved.
/// </para>
/// <para>
/// <b>The corollary is what makes this type load-bearing rather than
/// decorative.</b> Any guard that asserts on one stage against the deadline
/// passes both before and after a regression of this class, because no single
/// stage is ever the thing that breaches. Only a bound on the running total can
/// observe it, so the total is what this type tracks and what the regression
/// tests assert against.
/// </para>
/// <para>
/// <b>It answers "how much of the envelope is left?" and nothing else.</b>
/// Whether to refuse, and with which contract, is the call site's business.
/// <see cref="Describe"/> supplies the per-stage attribution that turns a
/// refusal into a diagnosis: absolute magnitude says where time is spent, and
/// the ratio against a healthy baseline says what moved, and only the second is
/// diagnostic for a system that was healthy hours earlier.
/// </para>
/// <para>
/// <b>Reference type, deliberately</b>, where the sibling
/// <see cref="LeafWalkBudget"/> is a struct. The stages are recorded from
/// inside <c>SetManyAsyncCore</c>, which is reached through a static lambda and
/// so cannot take a mutable struct by reference. The allocation is confined to
/// the armed case: <see cref="Start"/> returns <see langword="null"/> for an
/// unbounded budget, which is the default, so a deployment that has not opted
/// in allocates nothing.
/// </para>
/// </summary>
internal sealed class WriteEnvelopeBudget
{
    private readonly long _startTimestamp;
    private readonly TimeSpan _budget;
    private double _gateMilliseconds;
    private double _routeMilliseconds;
    private double _bucketMilliseconds;
    private double _fanOutMilliseconds;

    private WriteEnvelopeBudget(TimeSpan budget, long startTimestamp)
    {
        _budget = budget;
        _startTimestamp = startTimestamp;
    }

    /// <summary>
    /// Starts the envelope clock, or returns <see langword="null"/> when the
    /// budget is unbounded so the caller allocates nothing and keeps its
    /// historical behaviour.
    /// <para>
    /// Call this as the first statement of the write, <em>before</em> the gate.
    /// Starting it later is the whole defect in miniature: a clock that begins
    /// after the gate cannot see the gate's contribution, so the stage that
    /// degraded 12,085x in issue #2685 would be excluded from the very total it
    /// pushed past the deadline. The same reasoning drives
    /// <see cref="LeafWalkBudget.StartClock"/> one layer down.
    /// </para>
    /// </summary>
    /// <param name="budget">
    /// The envelope budget. <see cref="Timeout.InfiniteTimeSpan"/> and any
    /// non-positive value disable the bound, so a misconfigured option degrades
    /// to the historical unbounded behaviour rather than refusing every write.
    /// </param>
    /// <param name="startTimestamp">
    /// The <see cref="Stopwatch"/> stamp the envelope is measured from. Pass the
    /// one the caller-visible <see cref="LatticeMetrics.SetManyDuration"/>
    /// envelope already starts at, so the budget and the metric an operator
    /// sizes it from measure the same span. Pass <c>0</c> to measure from now.
    /// </param>
    internal static WriteEnvelopeBudget? Start(TimeSpan budget, long startTimestamp = 0L)
        => budget == Timeout.InfiniteTimeSpan || budget <= TimeSpan.Zero
            ? null
            : new WriteEnvelopeBudget(
                budget,
                startTimestamp != 0L ? startTimestamp : Stopwatch.GetTimestamp());

    /// <summary>The configured envelope budget this call runs under.</summary>
    internal TimeSpan Budget => _budget;

    /// <summary>
    /// Wall-clock consumed since <see cref="Start"/>, which is the quantity the
    /// caller's own RPC deadline is measuring.
    /// </summary>
    internal TimeSpan Elapsed => Stopwatch.GetElapsedTime(_startTimestamp);

    /// <summary>
    /// Budget still available, floored at <see cref="TimeSpan.Zero"/> so an
    /// overrun never presents as a negative timeout (which
    /// <see cref="Task.WaitAsync(TimeSpan)"/> rejects) and never wraps into an
    /// effectively unbounded wait.
    /// </summary>
    internal TimeSpan Remaining
    {
        get
        {
            var remaining = _budget - Elapsed;
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }
    }

    /// <summary>
    /// Whether the envelope is exhausted before the stage about to run has
    /// issued any work.
    /// </summary>
    internal bool IsSpent => Remaining == TimeSpan.Zero;

    /// <summary>Accumulates the <c>gate</c> stage's contribution, in milliseconds.</summary>
    internal void RecordGate(double milliseconds) => _gateMilliseconds += milliseconds;

    /// <summary>Accumulates the <c>route</c> stage's contribution, in milliseconds.</summary>
    internal void RecordRoute(double milliseconds) => _routeMilliseconds += milliseconds;

    /// <summary>Accumulates the <c>bucket</c> stage's contribution, in milliseconds.</summary>
    internal void RecordBucket(double milliseconds) => _bucketMilliseconds += milliseconds;

    /// <summary>Accumulates the <c>fanout</c> stage's contribution, in milliseconds.</summary>
    internal void RecordFanOut(double milliseconds) => _fanOutMilliseconds += milliseconds;

    /// <summary>
    /// Resolves the deadline a fan-out may actually wait for: the narrower of
    /// the envelope's remaining budget and the fan-out's own budget.
    /// <para>
    /// Taking the narrower of the two is what stops the additive breach. The
    /// fan-out's own budget is sized against a healthy fan-out and knows
    /// nothing of what the preceding stages already spent, so on its own it
    /// grants a full fresh window to a call that has already consumed most of
    /// the caller's patience.
    /// </para>
    /// </summary>
    /// <param name="envelope">
    /// The envelope budget, or <see langword="null"/> when unbounded.
    /// </param>
    /// <param name="fanOutBudget">
    /// <see cref="LatticeOptions.SetManyFanOutBudget"/>, where
    /// <see cref="Timeout.InfiniteTimeSpan"/> means unbounded.
    /// </param>
    /// <returns>
    /// The effective wait and which bound produced it.
    /// </returns>
    internal static FanOutWait ResolveFanOutWait(WriteEnvelopeBudget? envelope, TimeSpan fanOutBudget)
    {
        if (envelope is null) return new FanOutWait(fanOutBudget, BoundByEnvelope: false);

        var remaining = envelope.Remaining;
        if (fanOutBudget == Timeout.InfiniteTimeSpan || remaining <= fanOutBudget)
        {
            return new FanOutWait(remaining, BoundByEnvelope: true);
        }

        return new FanOutWait(fanOutBudget, BoundByEnvelope: false);
    }

    /// <summary>
    /// Renders the per-stage attribution that makes a refusal self-diagnosing,
    /// naming every stage's contribution alongside the total and the budget.
    /// <para>
    /// Stage figures accumulate across stale-routing retries of the core, so on
    /// a retried call they report the work actually done rather than only the
    /// last attempt's; <see cref="Elapsed"/> is wall-clock from the start
    /// either way and so already covers the retries.
    /// </para>
    /// </summary>
    internal string Describe()
        => string.Create(
            CultureInfo.InvariantCulture,
            $"gate={_gateMilliseconds:F1}ms, route={_routeMilliseconds:F1}ms, bucket={_bucketMilliseconds:F1}ms, fan-out={_fanOutMilliseconds:F1}ms; elapsed={Elapsed.TotalMilliseconds:F1}ms of a {_budget.TotalMilliseconds:F0}ms envelope budget");
}

/// <summary>
/// How long a batch write's fan-out may wait, and which bound produced that
/// figure.
/// <para>
/// The two are resolved together because the refusal has to name the right one.
/// A fan-out that is refused after 26.7 s because the <c>gate</c> had already
/// spent 4.1 s of a 30 s envelope is <b>not</b> a slow fan-out, and reporting it
/// as one sends the investigation to the wrong stage - the precise failure mode
/// issue #2685 records, where ranking stages by absolute magnitude finds the
/// fan-out and ranking them by ratio against a healthy baseline finds the gate.
/// </para>
/// </summary>
/// <param name="Duration">
/// The effective wait, or <see cref="Timeout.InfiniteTimeSpan"/> when neither
/// bound is armed.
/// </param>
/// <param name="BoundByEnvelope">
/// <see langword="true"/> when the whole-envelope budget is the narrower bound,
/// so a refusal is attributed to
/// <see cref="LatticeSaturationSource.SetManyEnvelope"/> and carries the
/// per-stage breakdown; <see langword="false"/> when the fan-out's own budget
/// binds, which keeps the pre-existing
/// <see cref="LatticeSaturationSource.SetManyFanOut"/> contract unchanged.
/// </param>
internal readonly record struct FanOutWait(TimeSpan Duration, bool BoundByEnvelope)
{
    /// <summary>
    /// <see langword="true"/> when some bound is armed, so the fan-out is
    /// waited on with a deadline rather than indefinitely.
    /// </summary>
    internal bool IsArmed => Duration != Timeout.InfiniteTimeSpan;
}
