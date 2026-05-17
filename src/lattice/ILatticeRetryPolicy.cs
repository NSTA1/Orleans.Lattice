namespace Orleans.Lattice;

/// <summary>
/// Opt-in retry policy applied at the boundary of every public
/// <see cref="ILattice"/> mutating call. The library never registers a
/// policy itself - hosts wire one through
/// <see cref="LatticeOptions.RetryPolicy"/> (per-tree) or via the
/// <c>AddLatticeRetryPolicy</c> DI extension. Retry is only safe when
/// the caller has also entered a <see cref="LatticeIdempotencyContext"/>
/// scope, so the implementation can assume the ambient key is present
/// for the duration of the operation.
/// </summary>
/// <remarks>
/// <para>
/// The policy contract is deliberately minimal: re-run
/// <paramref name="operation"/> under the same ambient
/// <see cref="LatticeIdempotencyContext"/> scope until it either
/// succeeds, the policy's budget is exhausted, or the supplied
/// <see cref="CancellationToken"/> fires. On budget exhaustion the
/// implementation surfaces the <em>original</em> failure verbatim so
/// the caller's existing exception-handling logic continues to work
/// unchanged.
/// </para>
/// <para>
/// Policies must be safe to share across concurrent calls (the
/// shipped <see cref="BoundedExponentialRetryPolicy"/> stores only
/// immutable configuration). A per-call wrapper that needs mutable
/// state should allocate per-invocation.
/// </para>
/// </remarks>
public interface ILatticeRetryPolicy
{
    /// <summary>
    /// Executes <paramref name="operation"/> under the policy's
    /// retry budget. The operation receives the same
    /// <see cref="CancellationToken"/> the caller supplied so a
    /// caller-level cancellation aborts the in-flight retry promptly.
    /// </summary>
    /// <param name="operation">
    /// The mutating lambda to retry. Receives the same cancellation
    /// token the caller passed to the public <see cref="ILattice"/>
    /// method. Must not be <c>null</c>.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancellation token propagated from the public API call.
    /// Honoured between attempts and inside the policy's back-off
    /// delays.
    /// </param>
    Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken);

    /// <summary>
    /// Typed overload for operations that return a value. Default
    /// implementation forwards to the untyped overload via a closure;
    /// custom policies may override for allocation-free typed paths.
    /// </summary>
    /// <typeparam name="T">The operation's result type.</typeparam>
    Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken);
}
