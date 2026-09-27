namespace Orleans.Lattice.Apps;

/// <summary>
/// The per-silo warm projection of the app registry: a compiled, immutable
/// <see cref="CompiledAppRegistrySnapshot"/> refreshed off the core change-feed whenever
/// the reserved <c>sys-app-registry</c> tree mutates, so activation and app surfaces can
/// answer "is this app installed, enabled, and with which ceiling?" synchronously and
/// without storage I/O. Consistency is eventual: a committed registry change is
/// reflected shortly after it commits, not necessarily before the writing call returns.
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
}
