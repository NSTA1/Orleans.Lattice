namespace Orleans.Lattice;

/// <summary>
/// Thrown by <see cref="IAtomicActionGrain.ExecuteAsync"/> when a plan names a
/// custom-step handler id that is not present in the registered handler catalog for
/// the silo. Handler resolution fails closed: an id that was never registered
/// through <c>AddLatticeAtomicAction</c> can never be invoked, whether it arrived
/// from a fresh caller-built plan or a persisted saga step. This is the security
/// seam that guarantees a wire- or storage-supplied step can only ever execute an
/// allow-listed, pre-registered effect.
/// <para>
/// Derives directly from <see cref="System.Exception"/> so the
/// <c>[GenerateSerializer]</c> exception needs no companion deep-copier: Orleans'
/// same-silo deep-copy path finds a base-type copier for
/// <see cref="System.Exception"/> directly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionHandlerNotRegistered)]
public sealed class AtomicActionHandlerNotRegisteredException : Exception
{
    /// <summary>
    /// The unregistered handler id that was rejected. Empty on the parameterless
    /// constructor; populated on the production overload so caller-side diagnostics
    /// can attribute the rejection without parsing the message.
    /// </summary>
    [Id(0)]
    public string HandlerId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="HandlerId"/>. Provided to satisfy the framework's exception
    /// construction contract; production throw sites use the overloads that carry
    /// diagnostic context.
    /// </summary>
    public AtomicActionHandlerNotRegisteredException()
    {
        HandlerId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and an
    /// empty <see cref="HandlerId"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which handler id was rejected.</param>
    public AtomicActionHandlerNotRegisteredException(string message) : base(message)
    {
        HandlerId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and wrapped
    /// inner exception, and an empty <see cref="HandlerId"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which handler id was rejected.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public AtomicActionHandlerNotRegisteredException(string message, Exception innerException)
        : base(message, innerException)
    {
        HandlerId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and the
    /// unregistered handler id that was rejected. The primary production throw
    /// shape.
    /// </summary>
    /// <param name="message">Diagnostic context describing which handler id was rejected.</param>
    /// <param name="handlerId">The unregistered handler id that was rejected.</param>
    public AtomicActionHandlerNotRegisteredException(string message, string handlerId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(handlerId);
        HandlerId = handlerId;
    }
}
