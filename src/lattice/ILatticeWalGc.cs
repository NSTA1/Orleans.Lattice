namespace Orleans.Lattice;

/// <summary>
/// WAL garbage collector seam. Trims per-shard write-ahead logs by
/// computing a safe trim point from the registered consumer cursors
/// (<see cref="IWalCursorRegistry"/>) and the optional
/// <see cref="LatticeOptions.WalRetention"/> wall-clock
/// hard ceiling, then invoking <see cref="IWalStorageProvider.TrimAsync"/>
/// on every partition whose head is eligible.
/// <para>
/// Hosts decide when to run a GC pass: a hosted background service,
/// an Orleans reminder, an admin-triggered call. The package does not
/// impose a scheduler. <see cref="RunOnceAsync"/> is safe to invoke
/// concurrently for distinct trees; concurrent calls for the same tree
/// will at worst produce duplicate <see cref="IWalStorageProvider.TrimAsync"/>
/// calls (which are idempotent by contract).
/// </para>
/// </summary>
public interface ILatticeWalGc
{
    /// <summary>
    /// Runs a single GC pass against <paramref name="treeName"/>'s
    /// per-shard WALs. The returned <see cref="LatticeWalGcReport"/>
    /// describes the safe trim threshold the run computed and the
    /// number of entries actually trimmed per shard.
    /// </summary>
    /// <param name="treeName">Logical tree id to garbage-collect. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token observed between every shard scan and every storage call.</param>
    Task<LatticeWalGcReport> RunOnceAsync(
        string treeName,
        CancellationToken cancellationToken = default);
}
