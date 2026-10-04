namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Ends a snapshot drain attempt at an entry the replication applier deferred
/// (<see cref="ApplyResult.Deferred"/>), instead of completing the drain past it
/// (issue #4604). The bootstrap drain is the entry's only delivery, so it is
/// re-drained rather than dropped: the coordinator retries the attempt within its
/// transient-retry budget and, once that is spent, fails with the import started,
/// which keeps the tree read-fenced and re-drives the bootstrap. Raised and handled
/// inside <see cref="LatticeBootstrapCoordinatorGrain"/>; it never crosses a grain
/// boundary.
/// </summary>
internal sealed class LatticeBootstrapEntryDeferredException : Exception
{
    /// <summary>Creates the exception for the deferred entry.</summary>
    /// <param name="treeName">The tree being bootstrapped.</param>
    /// <param name="key">The deferred entry's key.</param>
    public LatticeBootstrapEntryDeferredException(string treeName, string key)
        : base($"The replication applier deferred the snapshot entry for key '{key}' while bootstrapping tree '{treeName}'; the drain stops and the snapshot is re-drained.")
    {
        TreeName = treeName;
        Key = key;
    }

    /// <summary>The tree being bootstrapped.</summary>
    public string TreeName { get; }

    /// <summary>The deferred entry's key.</summary>
    public string Key { get; }
}
