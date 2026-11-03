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
/// <b>System-origin bypass.</b> The seam covers writes that reach the
/// intercepted data-plane write methods. A system-origin turn that reaches that
/// choke point, such as a saga leg or view-maintenance write, bypasses the
/// interceptor by default, exactly as it bypasses the access gate, so internal
/// machinery is not self-filtered. An interceptor that must also see those
/// intercepted system-origin writes opts in by returning <c>true</c> from
/// <see cref="InterceptsSystemOrigin"/>.
/// </para>
/// </remarks>
public interface ILatticeWriteInterceptor
{
    /// <summary>
    /// Gets a value indicating whether this interceptor must also be consulted
    /// on system-origin (library-internal) writes that reach the intercepted
    /// data-plane write methods, such as saga legs and view maintenance. Restore
    /// or merge paths that write below that choke point are not covered by this
    /// opt-in; a plain replicated apply is, through
    /// <see cref="JudgesReplicatedWritesAsync"/>. The default no-op returns <c>false</c>, preserving
    /// the "internal machinery is never intercepted" contract; a real interceptor
    /// returns <c>true</c> only when it must govern those intercepted writes too.
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

    /// <summary>
    /// Whether a plain last-writer-wins replicated write to the logical tree
    /// <paramref name="treeId"/> must be judged. The choke point asks once per
    /// replicated batch, and only when <see cref="InterceptsSystemOrigin"/> is
    /// <c>true</c>; when this returns <c>true</c> it calls
    /// <see cref="OnWriteAsync"/> for each row with
    /// <see cref="LatticeOperation.Write"/> | <see cref="LatticeOperation.Replication"/>.
    /// For such a row a <see cref="LatticeWriteDecisionKind.DeadLetter"/>
    /// diverts it, a <see cref="LatticeWriteDecisionKind.Reject"/> fails the apply
    /// (replication retries it and then dead-letters it), and either accept
    /// stores the replicated bytes verbatim: a transformed value is not applied,
    /// so regions converge byte for byte.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The default returns <c>true</c>, which fails closed: an interceptor that
    /// opts into system-origin writes sees replicated plain writes too.
    /// <b>Upgrade note:</b> an existing interceptor that returns <c>true</c> from
    /// <see cref="InterceptsSystemOrigin"/> now also receives these writes, and a
    /// rejection stalls the replicated batch until it is dead-lettered. To keep
    /// the previous behaviour, override this member and return <c>false</c>.
    /// </para>
    /// <para>
    /// Implementations should answer from a cache without allocating: the
    /// answer gates every replicated batch to the tree.
    /// </para>
    /// </remarks>
    /// <param name="treeId">The logical tree id, never a physical copy's id.</param>
    /// <param name="cancellationToken">Cancels the check.</param>
    /// <returns><c>true</c> when each plain replicated row must be passed to <see cref="OnWriteAsync"/>.</returns>
    ValueTask<bool> JudgesReplicatedWritesAsync(string treeId, CancellationToken cancellationToken = default) =>
        new(true);
}
