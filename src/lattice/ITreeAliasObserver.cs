namespace Orleans.Lattice;

/// <summary>
/// Extensibility hook invoked once per logical-tree physical-identity alias
/// change, fired from inside the tree registry's single alias-mutation choke
/// point (<c>SetAliasAsync</c> / <c>RemoveAliasAsync</c>) after the new alias
/// has been durably persisted and only when the effective physical id
/// actually changed. Intended for consumers that bind to a logical tree's
/// physical WAL and must rebind when a shadow-cutover restore, resize, or
/// reshard swaps that binding underneath them - most notably the
/// cross-cluster replication shipper, which uses it to rebind reactively
/// instead of re-reading the registry on every pump tick.
/// <para>
/// Register observers in the silo DI container (for example via
/// <c>services.AddSingleton&lt;ITreeAliasObserver, MyObserver&gt;()</c>).
/// Observers are resolved as <see cref="System.Collections.Generic.IEnumerable{T}"/>
/// so multiple can coexist; exceptions thrown by one observer are logged and
/// do not short-circuit the others. The hook is zero-cost when no observer is
/// registered.
/// </para>
/// </summary>
/// <remarks>
/// <para><b>Threading and latency.</b> <see cref="OnTreeAliasChangedAsync"/>
/// runs on the registry grain's single-threaded scheduler and is awaited
/// inline before the alias-mutation grain method returns. An alias swap is a
/// rare control-plane event (not a data-path write), so the fan-out is not a
/// hot path; even so, implementations should not issue slow synchronous I/O
/// directly from the hook. The canonical safe pattern is to dispatch the
/// rebind notification and return, letting the target absorb it on its own
/// scheduler.
/// </para>
/// <para><b>Failure semantics.</b> Exceptions thrown by the observer are
/// caught, logged as a warning, and suppressed - the alias has already been
/// persisted and cannot be rolled back by the hook. A consumer that misses a
/// notification (throw, or observer not yet activated) is expected to heal
/// out-of-band: the replication shipper's backstop re-resolve covers exactly
/// this case, so a lost notification degrades to poll-driven detection rather
/// than a permanent mis-binding.
/// </para>
/// <para><b>Fire-on-change only.</b> The registry raises this only when the
/// effective physical id genuinely changed. A no-op re-set of the current
/// alias, or a <c>RemoveAliasAsync</c> on an already-unaliased tree, does not
/// fire it, so observers never see spurious rebinds.
/// </para>
/// </remarks>
public interface ITreeAliasObserver
{
    /// <summary>
    /// Invoked once per effective physical-identity change for a logical
    /// tree. Implementations must treat <paramref name="change"/> as
    /// immutable and should complete quickly; long-running work belongs on a
    /// background queue or a fire-and-return dispatch.
    /// </summary>
    /// <param name="change">Metadata describing the alias change, including
    /// the old and new effective physical tree ids.</param>
    /// <param name="cancellationToken">Cancellation signal propagated from
    /// the registry grain's ambient cancellation when one is plumbed through.
    /// Observers should respect it for any asynchronous work they start.</param>
    Task OnTreeAliasChangedAsync(TreeAliasChange change, CancellationToken cancellationToken);
}
