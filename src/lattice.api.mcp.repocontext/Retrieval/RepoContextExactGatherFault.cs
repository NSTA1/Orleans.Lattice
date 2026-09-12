namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Classifies the fault an exact k-nearest-neighbour gather died of, so the
/// exact-scan ladder can tell a gather that ran out of <b>capacity</b> from one
/// that found a <b>broken index</b>.
/// <para>
/// <b>Why this type exists.</b> Before issue #2749 the ladder absorbed exactly one
/// fault, <see cref="ScanPageStalledException"/>, and treated every other fault as
/// evidence of a broken index. That reasoning is sound and is kept. What defeated
/// it is that <see cref="ScanPageStalledException"/> derives from
/// <see cref="TimeoutException"/>, so the absorbed set is a <i>subclass</i> of the
/// fault the deployment actually produces: the tree aborting its own page fill
/// raises the derived type, but a grain call whose target never replies at all
/// raises the base type, and a <c>catch</c> on the derived type does not see it. A
/// census of one deployment's faults over its whole uptime found
/// <see cref="TimeoutException"/> ten times, <see cref="OperationCanceledException"/>
/// three, <see cref="OutOfMemoryException"/> twice, and
/// <see cref="ScanPageStalledException"/> not once - so the absorbed set matched
/// nothing that happened.
/// </para>
/// <para>
/// <b>The line this draws, and why it is drawable at all.</b> A gather can fail for
/// two structurally different reasons, and only one of them is a statement about
/// the index:
/// </para>
/// <list type="bullet">
/// <item>
/// It ran out of <b>time or memory</b>. That is a statement about how loaded the
/// host is, not about what the index contains: the same gather over the same bytes
/// succeeds once the load goes away. It is transient, it wants backoff, and it is
/// exactly what the breaker was written to damp.
/// </item>
/// <item>
/// It could not <b>make sense of what it read</b> - a deserialisation fault, a
/// missing record, a contract violation. Repeating that costs the same and yields
/// the same, no amount of waiting fixes it, and masking it as "still building"
/// would hide a real defect behind a transient-looking state. It must stay loud.
/// </item>
/// </list>
/// <para>
/// Only the first is absorbed. The load-bearing constraint the original design
/// stated - absorbing a fault must never let a broken index be reported as a
/// still-building plane - is therefore preserved by construction rather than by
/// care: the predicate below recognises timing and memory faults <b>only</b>, so no
/// integrity fault can reach the absorbing branch.
/// </para>
/// <para>
/// <b>The walk is necessary, not defensive.</b> The failure happens several frames
/// below the grain call the gather issues, and reaches the caller wrapped: Orleans
/// surfaces a storage or activation failure carrying the original as an inner
/// exception, and a fan-out gather surfaces several at once inside an
/// <see cref="AggregateException"/>. Testing the outermost type alone would
/// classify the real occurrences as ordinary faults - that is, it would return the
/// wrong answer for precisely the cases this classifier exists to catch. The walk
/// is depth-bounded because an exception graph can be cyclic and this runs on the
/// query path, where a hang is worse than a missed classification.
/// </para>
/// </summary>
internal static class RepoContextExactGatherFault
{
    /// <summary>The tag key every arm below is published under.</summary>
    internal const string FaultTagKey = "fault";

    /// <summary>
    /// The tree aborted its own page fill inside the configured scan-page ceiling
    /// (<see cref="ScanPageStalledException"/>). Absorbed, and absorbed before this
    /// classifier is consulted, by the ladder's dedicated handler.
    /// </summary>
    internal const string StalledTag = "stalled";

    /// <summary>
    /// A call the gather issued did not answer inside its deadline. Absorbed.
    /// Distinct from <see cref="StalledTag"/> in origin, not in remedy: the tree
    /// gave up on itself there, whereas here the caller gave up on the tree.
    /// </summary>
    internal const string TimedOutTag = "timed_out";

    /// <summary>
    /// The gather could not allocate. Absorbed. This is the arm issue #2749
    /// predicted would dominate under memory pressure, and it is the one where
    /// repeating without backoff feeds the very condition that caused it.
    /// </summary>
    internal const string ExhaustedTag = "exhausted";

    /// <summary>
    /// The gather was cancelled by a deadline this process owns rather than by the
    /// caller. Absorbed. A cancellation the <i>caller</i> requested is never
    /// classified here at all - see the remarks on
    /// <see cref="IsTransient(Exception, CancellationToken)"/>.
    /// </summary>
    internal const string AbandonedTag = "abandoned";

    /// <summary>
    /// The fault says something about the index rather than about capacity, so it
    /// was not absorbed and reached the caller as a degraded index. Counting it is
    /// what keeps "no integrity faults occurred" a measured statement rather than
    /// an absent series.
    /// </summary>
    internal const string PropagatedTag = "propagated";

    /// <summary>
    /// How deep the cause chain is walked. Bounded because a hand-constructed
    /// exception graph can be cyclic.
    /// </summary>
    private const int MaxDepth = 16;

    /// <summary>
    /// Whether <paramref name="error"/> is a capacity fault the ladder should
    /// absorb and back off from, rather than an index fault it should report.
    /// <para>
    /// <paramref name="callerCancellation"/> is consulted so a query the
    /// <i>caller</i> abandoned is never mistaken for evidence about the repository.
    /// That distinction is load-bearing rather than tidy: the breaker is keyed per
    /// repository and shared by every caller, so absorbing a caller's own
    /// cancellation would let one client that walked away arm a backoff that
    /// withholds the exact fallback from all the others. A caller-cancelled query
    /// is therefore classified as neither transient nor a fault; it simply
    /// propagates, exactly as it did before.
    /// </para>
    /// </summary>
    /// <param name="error">The fault the gather raised. Must not be <see langword="null"/>.</param>
    /// <param name="callerCancellation">The token the caller supplied.</param>
    /// <returns><see langword="true"/> when the fault is transient and should be absorbed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is null.</exception>
    internal static bool IsTransient(Exception error, CancellationToken callerCancellation)
    {
        ArgumentNullException.ThrowIfNull(error);
        if (callerCancellation.IsCancellationRequested)
        {
            return false;
        }

        return Classify(error) is not PropagatedTag;
    }

    /// <summary>
    /// The arm <paramref name="error"/> is counted under. Returns
    /// <see cref="PropagatedTag"/> for any fault that is not a capacity fault.
    /// <para>
    /// Precedence is deliberate and is not the order the chain happens to be in:
    /// exhaustion outranks timing, because an allocation failure that surfaces
    /// wrapped in a timeout is an exhaustion event reported late, and reading it as
    /// a timeout would lose the one arm that says the host is out of memory. A
    /// stall outranks a plain timeout for the same reason in reverse - the derived
    /// type carries the more specific claim, so it must not be flattened into its
    /// own base.
    /// </para>
    /// </summary>
    /// <param name="error">The fault the gather raised. Must not be <see langword="null"/>.</param>
    /// <returns>One of the tag constants on this type.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is null.</exception>
    internal static string Classify(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        if (Matches(error, 0, static candidate => candidate is OutOfMemoryException))
        {
            return ExhaustedTag;
        }

        if (Matches(error, 0, static candidate => candidate is ScanPageStalledException))
        {
            return StalledTag;
        }

        if (Matches(error, 0, static candidate => candidate is TimeoutException))
        {
            return TimedOutTag;
        }

        if (Matches(error, 0, static candidate => candidate is OperationCanceledException))
        {
            return AbandonedTag;
        }

        return PropagatedTag;
    }

    /// <summary>
    /// Whether any exception in <paramref name="candidate"/>'s cause graph satisfies
    /// <paramref name="predicate"/>, walking <see cref="Exception.InnerException"/>
    /// and every branch of an <see cref="AggregateException"/>.
    /// </summary>
    private static bool Matches(Exception? candidate, int depth, Func<Exception, bool> predicate)
    {
        if (candidate is null || depth >= MaxDepth)
        {
            return false;
        }

        if (predicate(candidate))
        {
            return true;
        }

        if (candidate is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                if (Matches(inner, depth + 1, predicate))
                {
                    return true;
                }
            }

            return false;
        }

        return Matches(candidate.InnerException, depth + 1, predicate);
    }
}
