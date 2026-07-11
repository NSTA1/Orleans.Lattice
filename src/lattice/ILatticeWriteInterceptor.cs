namespace Orleans.Lattice;

/// <summary>
/// The pre-commit write-path value interceptor seam consulted at the
/// <c>LatticeGrain</c> data-plane choke point <b>after</b> the
/// <see cref="ILatticeAccessGate"/> has authorized the caller and <b>before</b>
/// the incoming value is appended to the WAL. A registered interceptor can
/// inspect, transform, reject, or dead-letter each incoming
/// <see cref="LatticeWriteRequest"/> without any internal grain being modified.
/// </summary>
/// <remarks>
/// <para>
/// The core library registers <see cref="NullLatticeWriteInterceptor"/> - an
/// always-accept no-op - so behaviour is byte-for-byte unchanged until a
/// companion package (for example a schema-enforcement add-on) registers a real
/// interceptor. Implementations must be cheap on the hot path: the request is
/// passed by <c>in</c> reference and a synchronous decision should complete the
/// returned <see cref="ValueTask{TResult}"/> without allocating.
/// </para>
/// <para>
/// Ordering relative to the access gate is fixed: a request is <b>authorized
/// first</b> (by the access gate) and only an authorized value is <b>then
/// intercepted</b> here, so an interceptor never sees a value the caller was not
/// permitted to write.
/// </para>
/// <para>
/// <b>System-origin bypass.</b> Library-internal traffic (replication apply,
/// saga legs, view maintenance, and other system-origin turns) bypasses the
/// interceptor by default, exactly as it bypasses the access gate, so internal
/// machinery is never self-filtered. An interceptor that must also see that
/// ingest opts in by returning <c>true</c> from
/// <see cref="InterceptsSystemOrigin"/>.
/// </para>
/// </remarks>
public interface ILatticeWriteInterceptor
{
    /// <summary>
    /// Gets a value indicating whether this interceptor must also be consulted
    /// on system-origin (library-internal) writes such as replication apply,
    /// saga legs, and view maintenance. The default no-op returns <c>false</c>,
    /// preserving the "internal machinery is never intercepted" contract; a real
    /// interceptor returns <c>true</c> only when it must govern that ingest too.
    /// </summary>
    bool InterceptsSystemOrigin => false;

    /// <summary>
    /// Inspects a single incoming write and returns the
    /// <see cref="LatticeWriteDecision"/> the choke point applies before the
    /// value becomes durable: accept as-is, accept a transformed value, reject
    /// (surfaced to the caller as an exception), or dead-letter (diverted by the
    /// interceptor and never made durable at the target key).
    /// </summary>
    /// <param name="request">The tree, key, incoming value bytes, operation, and optional TTL of the write.</param>
    /// <param name="cancellationToken">Cancels the interception.</param>
    /// <returns>The decision the choke point applies to the write.</returns>
    ValueTask<LatticeWriteDecision> OnWriteAsync(
        in LatticeWriteRequest request,
        CancellationToken cancellationToken = default);
}
