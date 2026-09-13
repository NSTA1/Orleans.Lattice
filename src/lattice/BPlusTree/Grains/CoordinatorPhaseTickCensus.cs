using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo census of the length of the <i>current run</i> of consecutive failed
/// phase ticks for every live coordinator activation, and the source of the
/// <see cref="LatticeMetrics.CoordinatorPhaseTickConsecutiveFailuresGaugeName"/>
/// observable gauge.
/// <para>
/// <b>Why a second instrument on a condition that is already counted.</b>
/// <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/> is a cumulative
/// counter, and a cumulative counter cannot express consecutiveness. A
/// coordinator that fails one tick in a thousand and a coordinator that has
/// failed every tick since the process started both present as a rising total,
/// and those two need opposite responses: the first is a transient the pump
/// absorbs by design, the second is a phase machine that has stopped advancing
/// and will not resume without intervention elsewhere. Only the run length
/// separates them, and until this gauge existed that run length was computed
/// (<c>CoordinatorGrain._consecutiveTickFailures</c>) and then spent entirely on
/// choosing a log severity - so the one diagnostic that distinguishes a wedge
/// from noise existed only as prose inside a log stream.
/// </para>
/// <para>
/// That is not a hypothetical shortfall. Issue #2814 established that the
/// repository-context approximate-index build had never advanced a single step,
/// and the evidence that made it a <i>wedge</i> rather than a flaky read was the
/// phrase "156 times in a row" in a 53 MB log stream. The counter it sat beside
/// was scraped, aggregated, and charted, and could not have answered the
/// question. A reader of this gauge answers it from one scrape.
/// </para>
/// <para>
/// <b>How to read it.</b> A live coordinator always reports, so <c>0</c> means
/// "this coordinator's last tick succeeded", not "no data". A value that is
/// non-zero but keeps returning to zero is a coordinator absorbing transients. A
/// value that only ever climbs is a wedged phase machine, and its magnitude is
/// how many ticks of work have been discarded back-to-back.
/// </para>
/// <para>
/// <b>Reported as a maximum over the activations sharing a tag set.</b> The tag
/// set is <c>kind</c> plus <see cref="CoordinatorGrain{TSelf}.MetricsTreeId"/>
/// plus the tenant, and that is deliberately coarser than the activation: a
/// coordinator with a composite key overrides <c>MetricsTreeId</c> to the subject
/// alone precisely so the series does not fan out per shard or per embedding
/// space. Two activations can therefore share one tag set, and a gauge may report
/// a tag set once. The reduction is <c>max</c> because the question the gauge
/// exists to answer is "is anything here wedged, and for how long" - taking a sum
/// would invent a run no activation experienced, and taking the last writer would
/// let a healthy sibling hide a wedged one.
/// </para>
/// <para>
/// <b>Enrolment is activation-scoped and cannot ratchet.</b> Every entry is keyed
/// by a token minted for one activation, so a coordinator that is reactivated
/// enrols afresh rather than inheriting a predecessor's run, and a stale
/// withdrawal cannot delete a successor's entry. Withdrawal happens when the
/// coordinator completes and when the activation is deactivated; an activation
/// lost with its silo takes this whole census with it, because the census is
/// process state rather than durable state.
/// </para>
/// </summary>
internal static class CoordinatorPhaseTickCensus
{
    /// <summary>
    /// Live enrolments, keyed by the per-activation token
    /// <see cref="Enrol"/> minted. Lock-free so the gauge callback never blocks a
    /// phase tick and a phase tick never blocks a scrape.
    /// </summary>
    private static readonly ConcurrentDictionary<long, Enrolment> LiveEnrolments = new();

    private static long _nextToken;

    /// <summary>
    /// Enrols one coordinator activation at a run length of zero and returns the
    /// token that identifies it for <see cref="Record"/> and
    /// <see cref="Withdraw"/>.
    /// <para>
    /// <b>Enrolling at zero is the priming half of the instrument and is not
    /// optional.</b> A gauge that reported only coordinators currently failing
    /// would make a healthy coordinator byte-identical to an absent one, which is
    /// the exact ambiguity this whole family of instruments exists to remove: a
    /// reader could not tell "every coordinator is fine" from "the instrument is
    /// not wired" or "the build predates this change". Enrolling when the phase
    /// timer is armed - the same moment the sibling counter primes its own series
    /// - means a zero on this gauge is always a reading.
    /// </para>
    /// </summary>
    /// <param name="kind">The coordinator kind (its keepalive reminder name).</param>
    /// <param name="tree">The subject the coordinator serves.</param>
    /// <returns>The token identifying this enrolment.</returns>
    internal static long Enrol(string kind, string tree)
    {
        var token = Interlocked.Increment(ref _nextToken);
        LiveEnrolments[token] = new Enrolment(kind, tree, 0);
        return token;
    }

    /// <summary>
    /// Records the run length after one phase tick: zero when the tick returned
    /// normally, otherwise the number of consecutive ticks that have now thrown.
    /// <para>
    /// The tags are re-supplied on every call rather than captured at enrolment,
    /// for the same reason
    /// <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/> builds its tag set
    /// per emission: <c>MetricsTreeId</c> is a derived-class hook that may only
    /// become resolvable after activation, so a value captured at arming time can
    /// be superseded by a better one and must not be pinned.
    /// </para>
    /// <para>
    /// A token with no live enrolment is ignored rather than re-created. The only
    /// way to reach that state is a tick that lands after withdrawal, and
    /// resurrecting the entry would leave a series for an activation that no
    /// longer exists - the ratchet this design exists to make unrepresentable.
    /// </para>
    /// </summary>
    /// <param name="token">The token from <see cref="Enrol"/>.</param>
    /// <param name="kind">The coordinator kind (its keepalive reminder name).</param>
    /// <param name="tree">The subject the coordinator serves.</param>
    /// <param name="consecutiveFailures">The current run length, zero when the last tick succeeded.</param>
    internal static void Record(long token, string kind, string tree, int consecutiveFailures)
    {
        if (!LiveEnrolments.TryGetValue(token, out var current)) return;
        LiveEnrolments.TryUpdate(token, new Enrolment(kind, tree, consecutiveFailures), current);
    }

    /// <summary>
    /// Removes one activation's enrolment. Idempotent, so a coordinator that
    /// completes and is then deactivated withdraws once and the second call is a
    /// no-op.
    /// </summary>
    /// <param name="token">The token from <see cref="Enrol"/>.</param>
    internal static void Withdraw(long token) => LiveEnrolments.TryRemove(token, out _);

    /// <summary>
    /// Emits one measurement per distinct <c>(kind, tree)</c> pair among the live
    /// enrolments, carrying the same tags
    /// <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/> carries so the two
    /// series join, and reporting the longest run any activation behind that tag
    /// set is currently in.
    /// </summary>
    /// <returns>One measurement per distinct tag set.</returns>
    internal static IEnumerable<Measurement<long>> Observe()
    {
        // Materialised rather than streamed: the reduction has to see every
        // enrolment before it can emit the first measurement, and a scrape must
        // not observe a tag set twice.
        var worst = new Dictionary<(string Kind, string Tree), int>();
        foreach (var enrolment in LiveEnrolments.Values)
        {
            var key = (enrolment.Kind, enrolment.Tree);
            if (!worst.TryGetValue(key, out var run) || enrolment.ConsecutiveFailures > run)
            {
                worst[key] = enrolment.ConsecutiveFailures;
            }
        }

        var measurements = new List<Measurement<long>>(worst.Count);
        foreach (var ((kind, tree), run) in worst)
        {
            measurements.Add(new Measurement<long>(
                run,
                new KeyValuePair<string, object?>(LatticeMetrics.TagKind, kind),
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, tree),
                LatticeTenantLabel.ForTree(tree)));
        }

        return measurements;
    }

    /// <summary>One activation's tags and the run length it is currently in.</summary>
    /// <param name="Kind">The coordinator kind (its keepalive reminder name).</param>
    /// <param name="Tree">The subject the coordinator serves.</param>
    /// <param name="ConsecutiveFailures">The current run length, zero when the last tick succeeded.</param>
    private readonly record struct Enrolment(string Kind, string Tree, int ConsecutiveFailures);
}
