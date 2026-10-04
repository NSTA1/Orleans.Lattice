namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree snapshot export epoch (issue #4534). Every full snapshot export
/// advances it at its registry snap0, and a peer that completes a bootstrap
/// from that export echoes the epoch back on its acknowledgements. A shipper
/// that took its peer off the log at epoch <c>e</c> therefore knows the peer
/// has been re-seeded from an export taken after that point once it sees an
/// echoed epoch greater than <c>e</c>. Keyed by the logical tree name.
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationExportEpochGrain)]
internal interface IReplicationExportEpochGrain : IGrainWithStringKey
{
    /// <summary>Returns the current epoch, or <c>0</c> when no full export has been taken.</summary>
    Task<long> GetAsync();

    /// <summary>Durably advances the epoch and returns the new value.</summary>
    Task<long> AdvanceAsync();
}
