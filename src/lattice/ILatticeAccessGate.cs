namespace Orleans.Lattice;

/// <summary>
/// The central access-gate enforcement seam consulted at the
/// <c>LatticeGrain</c> data-plane choke point before a read, write, delete,
/// range, CRDT, or lifecycle operation is performed. A registered gate can
/// allow, deny, or allow-with-a-key-filter each <see cref="LatticeAccessRequest"/>.
/// </summary>
/// <remarks>
/// <para>
/// The core library registers <see cref="NullLatticeAccessGate"/> - an
/// allow-all no-op - so behaviour is byte-for-byte unchanged until an add-on
/// (<c>Orleans.Lattice.Auth</c>) registers a real gate. Implementations must be
/// cheap on the hot path: the request is passed by <c>in</c> reference and a
/// synchronous decision should complete the returned <see cref="ValueTask{TResult}"/>
/// without allocating.
/// </para>
/// <para>
/// Enforcement is <b>not</b> wired into any grain operation by this seam; the
/// wiring at the <c>LatticeGrain</c> choke point (including the system-origin
/// bypass and the per-leg authorization of atomic writes) is a later step.
/// </para>
/// </remarks>
public interface ILatticeAccessGate
{
    /// <summary>
    /// Authorizes a single data-plane <paramref name="request"/>.
    /// </summary>
    /// <param name="request">The operation, tree, key(s), and resolved subject to authorize.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>
    /// The <see cref="LatticeAccessDecision"/>: allow, deny, or allow with a
    /// per-key filter.
    /// </returns>
    ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default);
}
