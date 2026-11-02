namespace Orleans.Lattice.Apps;

/// <summary>
/// The per-silo warm projection of the app registry: a compiled, immutable
/// <see cref="CompiledAppRegistrySnapshot"/> refreshed off the core change-feed whenever
/// the reserved <c>sys-app-registry</c> tree mutates, so activation and app surfaces can
/// answer "is this app installed, enabled, and with which ceiling?" synchronously and
/// without storage I/O. <see cref="Current"/> is eventually consistent: the change feed fires
/// only on the silo that committed a write. A surface that decides access on the snapshot calls
/// <see cref="EnsureCurrentAsync"/> first, which on a silo host makes it current with every
/// registry write that returned on any silo before the call began.
/// </summary>
public interface IAppRegistryProjection
{
    /// <summary>
    /// The monotonically increasing epoch of <see cref="Current"/>. It advances every time
    /// the snapshot is rebuilt, so a caller can detect that anything it cached from an
    /// earlier snapshot may be stale.
    /// </summary>
    long CurrentEpoch { get; }

    /// <summary>
    /// The current snapshot, read without locking and swapped atomically on rebuild.
    /// <see cref="CompiledAppRegistrySnapshot.Empty"/> until the first build.
    /// </summary>
    CompiledAppRegistrySnapshot Current { get; }

    /// <summary>
    /// Ensures the snapshot has been built at least once, building it (awaited) when it is
    /// still cold. Idempotent: once any rebuild has advanced the epoch this returns at once.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>A task that completes when the snapshot is warm.</returns>
    Task EnsureWarmAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures the snapshot is current with the cluster-wide registry before it is trusted to decide
    /// access, rebuilding it (awaited) when it is cold or when a registry write committed on any silo
    /// since it was built. Surfaces that grant on the snapshot call this rather than
    /// <see cref="EnsureWarmAsync"/>, because the change feed that refreshes it fires only on the silo
    /// that committed the write (#5595).
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>A task that completes when the snapshot is current.</returns>
    /// <remarks>The default implementation only warms the snapshot, for projections with no cluster-wide currency.</remarks>
    Task EnsureCurrentAsync(CancellationToken cancellationToken = default) => EnsureWarmAsync(cancellationToken);
}
