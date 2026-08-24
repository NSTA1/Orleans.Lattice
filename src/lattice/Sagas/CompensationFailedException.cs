namespace Orleans.Lattice;

/// <summary>
/// Thrown by <see cref="IAtomicActionGrain.ExecuteAsync"/> when an atomic-action
/// saga faulted on a forward step and, while compensating the already-committed
/// steps, a compensating effect itself faulted after its retry budget. The saga
/// cannot guarantee it undid every committed step, so it parked in the terminal
/// <see cref="AtomicActionStatus.CompensationFailed"/> state and surfaced this
/// exception rather than silently swallowing the fault. Compensation correctness is
/// the caller's contract; recovering a parked saga requires operator intervention.
/// <para>
/// The terminal outcome is also readable, without catching this, by re-issuing the
/// same operation id (which returns the memoized
/// <see cref="AtomicActionOutcome"/> reporting
/// <see cref="AtomicActionStatus.CompensationFailed"/>).
/// </para>
/// <para>
/// Derives directly from <see cref="System.Exception"/> so the
/// <c>[GenerateSerializer]</c> exception needs no companion deep-copier: Orleans'
/// same-silo deep-copy path finds a base-type copier for
/// <see cref="System.Exception"/> directly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CompensationFailed)]
public sealed class CompensationFailedException : Exception
{
    /// <summary>
    /// The zero-based index of the step whose compensating effect faulted, or
    /// <c>-1</c> when unknown. Populated on the production overload so caller-side
    /// diagnostics can attribute the parked saga to a specific step without parsing
    /// the message.
    /// </summary>
    [Id(0)]
    public int StepIndex { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an unknown
    /// (<c>-1</c>) <see cref="StepIndex"/>. Provided to satisfy the framework's
    /// exception construction contract; production throw sites use the overloads
    /// that carry diagnostic context.
    /// </summary>
    public CompensationFailedException()
    {
        StepIndex = -1;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and an
    /// unknown (<c>-1</c>) <see cref="StepIndex"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which compensation faulted and why.</param>
    public CompensationFailedException(string message) : base(message)
    {
        StepIndex = -1;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and wrapped
    /// inner exception, and an unknown (<c>-1</c>) <see cref="StepIndex"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which compensation faulted and why.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public CompensationFailedException(string message, Exception innerException)
        : base(message, innerException)
    {
        StepIndex = -1;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and the
    /// index of the step whose compensation faulted. The primary production throw
    /// shape.
    /// </summary>
    /// <param name="message">Diagnostic context describing which compensation faulted and why.</param>
    /// <param name="stepIndex">The zero-based index of the step whose compensation faulted.</param>
    public CompensationFailedException(string message, int stepIndex) : base(message)
    {
        StepIndex = stepIndex;
    }
}
