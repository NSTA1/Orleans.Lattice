namespace Orleans.Lattice;

/// <summary>
/// The decision an <see cref="ILatticeWriteInterceptor"/> returns for a
/// <see cref="LatticeWriteRequest"/>: accept the value as-is, accept a
/// transformed value, reject the write, or dead-letter it.
/// </summary>
/// <remarks>
/// <para>
/// This is an in-process decision value, modelled on
/// <see cref="LatticeAccessDecision"/>. It is deliberately a plain
/// <c>readonly struct</c> (not a <c>record struct</c>) because it carries a
/// <see cref="byte"/> array payload that is neither value-comparable nor
/// intended to cross a grain boundary; the type must never be persisted or sent
/// on the wire and carries no Orleans serialization attributes.
/// </para>
/// <para>
/// The <see cref="Accept()"/> factory returns a cached singleton value so the
/// default no-op interceptor (<see cref="NullLatticeWriteInterceptor"/>) produces
/// a decision without allocating.
/// </para>
/// </remarks>
public readonly struct LatticeWriteDecision
{
    private static readonly LatticeWriteDecision AcceptDecision =
        new(LatticeWriteDecisionKind.Accept, transformedValue: null, reason: null);

    private LatticeWriteDecision(LatticeWriteDecisionKind kind, byte[]? transformedValue, string? reason)
    {
        Kind = kind;
        TransformedValue = transformedValue;
        Reason = reason;
    }

    /// <summary>The kind of decision this value represents.</summary>
    public LatticeWriteDecisionKind Kind { get; }

    /// <summary>
    /// The replacement value bytes when <see cref="Kind"/> is
    /// <see cref="LatticeWriteDecisionKind.AcceptTransformed"/>; <c>null</c> for
    /// every other kind.
    /// </summary>
    public byte[]? TransformedValue { get; }

    /// <summary>
    /// A human-readable reason. Set for a <see cref="LatticeWriteDecisionKind.Reject"/>
    /// or <see cref="LatticeWriteDecisionKind.DeadLetter"/> decision (the cause);
    /// <c>null</c> for an accept.
    /// </summary>
    public string? Reason { get; }

    /// <summary>
    /// The cached "accept unchanged" decision. Allocation-free.
    /// </summary>
    /// <returns>A decision whose <see cref="Kind"/> is <see cref="LatticeWriteDecisionKind.Accept"/>.</returns>
    public static LatticeWriteDecision Accept() => AcceptDecision;

    /// <summary>
    /// Creates an accept decision that replaces the incoming value with
    /// <paramref name="newValue"/> before it is made durable.
    /// </summary>
    /// <param name="newValue">The replacement value bytes. Must not be <c>null</c>.</param>
    /// <returns>A decision whose <see cref="Kind"/> is <see cref="LatticeWriteDecisionKind.AcceptTransformed"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="newValue"/> is <c>null</c>.</exception>
    public static LatticeWriteDecision AcceptTransformed(byte[] newValue)
    {
        ArgumentNullException.ThrowIfNull(newValue);
        return new LatticeWriteDecision(LatticeWriteDecisionKind.AcceptTransformed, newValue, reason: null);
    }

    /// <summary>
    /// Creates a reject decision carrying the supplied reason. The choke point
    /// surfaces it to the caller as a <see cref="LatticeWriteRejectedException"/>.
    /// </summary>
    /// <param name="reason">The reason the write is rejected. Must not be <c>null</c> or empty.</param>
    /// <returns>A decision whose <see cref="Kind"/> is <see cref="LatticeWriteDecisionKind.Reject"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is <c>null</c> or empty.</exception>
    public static LatticeWriteDecision Reject(string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        return new LatticeWriteDecision(LatticeWriteDecisionKind.Reject, transformedValue: null, reason);
    }

    /// <summary>
    /// Creates a dead-letter decision carrying the supplied reason. The write is
    /// diverted by the interceptor and never made durable at the target key; the
    /// caller observes no exception.
    /// </summary>
    /// <param name="reason">The reason the write is dead-lettered. Must not be <c>null</c> or empty.</param>
    /// <returns>A decision whose <see cref="Kind"/> is <see cref="LatticeWriteDecisionKind.DeadLetter"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is <c>null</c> or empty.</exception>
    public static LatticeWriteDecision DeadLetter(string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        return new LatticeWriteDecision(LatticeWriteDecisionKind.DeadLetter, transformedValue: null, reason);
    }
}
