namespace Orleans.Lattice;

/// <summary>
/// The kind of decision an <see cref="ILatticeWriteInterceptor"/> returns for a
/// <see cref="LatticeWriteRequest"/>.
/// </summary>
/// <remarks>
/// This is in-process decision vocabulary consumed at the <c>LatticeGrain</c>
/// choke point. It is never persisted or sent on the wire by the core library,
/// so it carries no Orleans serialization attributes.
/// </remarks>
public enum LatticeWriteDecisionKind : byte
{
    /// <summary>Accept the incoming value unchanged.</summary>
    Accept = 0,

    /// <summary>Accept, but replace the incoming value with a transformed one.</summary>
    AcceptTransformed = 1,

    /// <summary>
    /// Reject the write. The choke point surfaces the rejection to the caller as
    /// a <see cref="LatticeWriteRejectedException"/>, and nothing is made durable.
    /// </summary>
    Reject = 2,

    /// <summary>
    /// Divert the write to the interceptor's own dead-letter channel. The value
    /// does not become durable at the target key, and the caller observes no
    /// exception.
    /// </summary>
    DeadLetter = 3,
}
