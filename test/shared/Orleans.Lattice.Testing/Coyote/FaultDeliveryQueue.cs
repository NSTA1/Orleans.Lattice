namespace Orleans.Lattice.Testing.Coyote;

/// <summary>
/// A bounded, fault-injecting delivery channel for a liveness
/// <see cref="ICoyoteModel"/>: it holds messages a coordinator has broadcast but
/// that have not yet been processed by their target, and hands them out under
/// scheduler-explored <b>reordering</b>, bounded <b>drops</b>, and bounded
/// <b>duplication</b> drawn from a shared <see cref="FaultBudget"/>. It is the
/// transport half of the fault model; a model layers participant
/// <b>restart</b> on top via <see cref="RemoveAll(System.Predicate{T})"/> and the
/// budget's restart allowance.
/// <para>
/// Delivery order is chosen nondeterministically per step, so the Coyote engine
/// explores every interleaving of in-flight deliveries. A delivered message may
/// be dropped (removed without delivery, modelling permanent transport loss) or
/// duplicated (re-enqueued so it is delivered again, modelling an at-least-once
/// transport) whenever the shared budget permits. Because drops and duplicates
/// each consume budget and the queue never grows except by a budgeted duplicate,
/// the total number of deliveries is bounded by the initial enqueue count plus
/// the duplicate budget, so a model's delivery loop always terminates.
/// </para>
/// </summary>
/// <typeparam name="T">
/// The message payload; typically a small value identifying the delivery target
/// (for example a participant index).
/// </typeparam>
/// <remarks>
/// Like <see cref="FaultBudget"/>, the queue takes its nondeterministic decisions
/// as a <see cref="System.Func{Boolean}"/> rather than referencing a Coyote
/// runtime type, so it is dependency-free and unit-testable without an engine. A
/// model passes <c>runtime.RandomBoolean</c>; a unit test passes a scripted
/// delegate.
/// </remarks>
public sealed class FaultDeliveryQueue<T>
{
    private readonly List<T> _pending = [];
    private readonly FaultBudget _budget;

    /// <summary>
    /// Creates a delivery queue that draws its drop and duplicate allowances from
    /// <paramref name="budget"/>.
    /// </summary>
    /// <param name="budget">The shared fault budget bounding drops and duplicates.</param>
    /// <exception cref="System.ArgumentNullException"><paramref name="budget"/> is null.</exception>
    public FaultDeliveryQueue(FaultBudget budget)
    {
        ArgumentNullException.ThrowIfNull(budget);
        _budget = budget;
    }

    /// <summary><see langword="true"/> while at least one message is in flight.</summary>
    public bool HasPending => _pending.Count > 0;

    /// <summary>The number of in-flight (enqueued, not yet delivered) messages.</summary>
    public int PendingCount => _pending.Count;

    /// <summary>
    /// Enqueues <paramref name="message"/> as a new in-flight delivery.
    /// </summary>
    /// <param name="message">The message to make available for delivery.</param>
    public void Enqueue(T message) => _pending.Add(message);

    /// <summary>
    /// Removes every in-flight message matching <paramref name="match"/> without
    /// delivering it, returning how many were removed. A model uses this to model
    /// a participant <b>restart</b> that loses the volatile in-flight deliveries
    /// targeted at it (its durable state is unaffected, so a correct protocol
    /// recovers through a backstop rather than through the lost delivery).
    /// </summary>
    /// <param name="match">The predicate selecting messages to drop.</param>
    /// <returns>The number of in-flight messages removed.</returns>
    /// <exception cref="System.ArgumentNullException"><paramref name="match"/> is null.</exception>
    public int RemoveAll(Predicate<T> match)
    {
        ArgumentNullException.ThrowIfNull(match);
        return _pending.RemoveAll(match);
    }

    /// <summary>
    /// Selects the next in-flight message under scheduler-explored reordering and
    /// attempts to deliver it. The chosen message is removed from the queue; then,
    /// if the budget permits and <paramref name="decide"/> so chooses, the
    /// delivery is <b>dropped</b> (this call yields nothing) or <b>duplicated</b>
    /// (a copy is re-enqueued for a later delivery). Returns <see langword="true"/>
    /// with the delivered <paramref name="message"/> when a delivery actually
    /// occurs, and <see langword="false"/> when the queue was empty or the chosen
    /// message was dropped.
    /// </summary>
    /// <param name="decide">
    /// The nondeterministic decision source (for example
    /// <c>runtime.RandomBoolean</c>), used both to reorder the selection and to
    /// drive the budgeted drop/duplicate choices.
    /// </param>
    /// <param name="message">
    /// The delivered message when this method returns <see langword="true"/>;
    /// otherwise the default value.
    /// </param>
    /// <exception cref="System.ArgumentNullException"><paramref name="decide"/> is null.</exception>
    public bool TryDeliverNext(Func<bool> decide, out T message)
    {
        ArgumentNullException.ThrowIfNull(decide);
        message = default!;
        if (_pending.Count == 0)
        {
            return false;
        }

        var index = SelectIndex(decide);
        var chosen = _pending[index];
        _pending.RemoveAt(index);

        if (_budget.TryDrop(decide))
        {
            // Permanent transport loss: the message is gone and is not delivered.
            return false;
        }

        if (_budget.TryDuplicate(decide))
        {
            // At-least-once transport: schedule a second delivery of the same
            // message. A correct protocol's apply step must be idempotent.
            _pending.Add(chosen);
        }

        message = chosen;
        return true;
    }

    /// <summary>
    /// Picks the index of the next message to deliver, driving the choice through
    /// <paramref name="decide"/> so the harness explores distinct delivery orders.
    /// Scans the in-flight messages and takes the first the decision source
    /// accepts, defaulting to the head when every candidate is deferred. Always
    /// returns a valid index (the queue is non-empty at the call site).
    /// </summary>
    private int SelectIndex(Func<bool> decide)
    {
        for (var i = 0; i < _pending.Count; i++)
        {
            if (decide())
            {
                return i;
            }
        }

        return 0;
    }
}
