namespace Orleans.Lattice.Testing.Coyote;

/// <summary>
/// A bounded ledger of the message-transport and participant-restart faults a
/// liveness <see cref="ICoyoteModel"/> is permitted to inject during one
/// exploration iteration: a fixed number of delivery <b>drops</b>, delivery
/// <b>duplicates</b>, and participant <b>restarts</b>. Each fault is consumed
/// through a runtime nondeterministic choice, so the Coyote scheduler explores
/// both injecting and not injecting it, and the count strictly decreases so the
/// budget is eventually exhausted.
/// <para>
/// The bound is what makes a liveness property decidable under the cooperative
/// harness. Because <see cref="CoyoteModelHarness"/> does not apply
/// <c>coyote rewrite</c>, real <c>Task</c>/<c>await</c> interleavings are not
/// controlled and there is no fair infinite schedule for a temperature-based
/// Coyote liveness monitor to flag. A finite fault budget instead encodes the
/// <b>fairness</b> assumption every liveness argument needs - "faults do not
/// happen forever" - as a concrete ceiling: once the budget is exhausted the
/// transport is reliable, so a correct protocol must then converge. A model can
/// therefore drive the interleave to its bounded step limit and assert the good
/// terminal state was reached (a <b>bounded-progress</b> encoding of liveness),
/// with the budget guaranteeing exploration still terminates.
/// </para>
/// </summary>
/// <remarks>
/// The type is deliberately dependency-free (it takes the nondeterministic
/// decision as a <see cref="System.Func{Boolean}"/> rather than referencing a
/// Coyote runtime type), so it is reusable by every model and unit-testable
/// without a Coyote engine. A model passes <c>runtime.RandomBoolean</c> as the
/// decision source; a unit test passes a scripted delegate.
/// <para>
/// <b>Lifecycle:</b> a budget is mutable and drains as faults are consumed, so it
/// is <b>per-iteration</b> state. A model must construct a fresh budget inside its
/// <c>Run</c> method on every exploration iteration and must never store one in a
/// field: the Coyote engine reuses the same model instance for every schedule, so
/// a shared budget is drained by the first few iterations and every later
/// iteration then injects no faults - silently collapsing exploration coverage
/// (issue #1664).
/// </para>
/// </remarks>
public sealed class FaultBudget
{
    private int _drops;
    private int _duplicates;
    private int _restarts;

    /// <summary>
    /// Creates a budget permitting <paramref name="drops"/> delivery drops,
    /// <paramref name="duplicates"/> delivery duplicates, and
    /// <paramref name="restarts"/> participant restarts over one iteration.
    /// </summary>
    /// <param name="drops">The maximum number of deliveries that may be dropped.</param>
    /// <param name="duplicates">The maximum number of deliveries that may be duplicated.</param>
    /// <param name="restarts">The maximum number of participant restarts.</param>
    /// <exception cref="System.ArgumentOutOfRangeException">
    /// Any argument is negative.
    /// </exception>
    public FaultBudget(int drops, int duplicates, int restarts)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(drops);
        ArgumentOutOfRangeException.ThrowIfNegative(duplicates);
        ArgumentOutOfRangeException.ThrowIfNegative(restarts);
        _drops = drops;
        _duplicates = duplicates;
        _restarts = restarts;
    }

    /// <summary>The number of delivery drops still permitted.</summary>
    public int DropsRemaining => _drops;

    /// <summary>The number of delivery duplicates still permitted.</summary>
    public int DuplicatesRemaining => _duplicates;

    /// <summary>The number of participant restarts still permitted.</summary>
    public int RestartsRemaining => _restarts;

    /// <summary>
    /// <see langword="true"/> once every kind of fault has been exhausted, so no
    /// further fault can be injected and the transport is thereafter reliable.
    /// </summary>
    public bool IsExhausted => _drops == 0 && _duplicates == 0 && _restarts == 0;

    /// <summary>
    /// Offers a delivery drop: if the drop budget is non-empty and
    /// <paramref name="decide"/> chooses to inject, consumes one drop and returns
    /// <see langword="true"/>; otherwise leaves the budget untouched and returns
    /// <see langword="false"/>. <paramref name="decide"/> is consulted only when
    /// budget remains, so an exhausted budget adds no scheduling choice point.
    /// </summary>
    /// <param name="decide">
    /// The nondeterministic decision source (for example
    /// <c>runtime.RandomBoolean</c>); invoked at most once and only while budget
    /// remains.
    /// </param>
    public bool TryDrop(Func<bool> decide) => TryConsume(ref _drops, decide);

    /// <summary>
    /// Offers a delivery duplicate: if the duplicate budget is non-empty and
    /// <paramref name="decide"/> chooses to inject, consumes one duplicate and
    /// returns <see langword="true"/>; otherwise returns <see langword="false"/>.
    /// </summary>
    /// <param name="decide">The nondeterministic decision source.</param>
    public bool TryDuplicate(Func<bool> decide) => TryConsume(ref _duplicates, decide);

    /// <summary>
    /// Offers a participant restart: if the restart budget is non-empty and
    /// <paramref name="decide"/> chooses to inject, consumes one restart and
    /// returns <see langword="true"/>; otherwise returns <see langword="false"/>.
    /// </summary>
    /// <param name="decide">The nondeterministic decision source.</param>
    public bool TryRestart(Func<bool> decide) => TryConsume(ref _restarts, decide);

    private static bool TryConsume(ref int remaining, Func<bool> decide)
    {
        ArgumentNullException.ThrowIfNull(decide);
        if (remaining <= 0)
        {
            return false;
        }

        if (!decide())
        {
            return false;
        }

        remaining--;
        return true;
    }
}
