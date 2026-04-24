namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>
/// One janitor per replicated tree. Grain key: the tree name (e.g.
/// <c>"mfg-facts"</c>). Runs a reminder every 10 minutes: reads
/// every peer replicator's cursor, takes the min, and prunes replog
/// entries older than <c>min - retention</c> (retention = 24 h).
/// Never prunes ahead of the slowest peer.
/// </summary>
internal interface IReplogJanitorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Runs one sweep. Returns the number of replog entries deleted.
    /// Safe to invoke manually (e.g. from a diagnostic endpoint).
    /// </summary>
    Task<int> SweepAsync();
}
