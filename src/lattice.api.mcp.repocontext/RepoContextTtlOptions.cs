namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Per-repository time-to-live policy for the repository-context surface. Bound
/// per repository through the named-options convention -
/// <c>IOptionsMonitor&lt;RepoContextTtlOptions&gt;.Get(repoId)</c> - so each
/// repository can carry its own memory-entry lifetime while the default
/// (unnamed) instance supplies the fallback, mirroring how the core resolves
/// <c>IOptionsMonitor&lt;LatticeOptions&gt;.Get(treeName)</c> per tree.
/// <para>
/// This type only <b>surfaces</b> the per-entry expiry that Orleans.Lattice core
/// already provides on <see cref="ILattice.SetAsync(string, byte[], System.TimeSpan, System.Threading.CancellationToken)"/>
/// (which converts a TTL to an absolute UTC expiry at write time; reads then hide
/// expired entries and background tombstone compaction reaps them). It introduces
/// no new expiry mechanism. The memory-writing tools that consume these options
/// are layered on separately.
/// </para>
/// </summary>
public sealed class RepoContextTtlOptions
{
    /// <summary>
    /// The default time-to-live applied to an agent-authored memory entry when
    /// the writer does not supply an explicit per-entry TTL, or
    /// <see langword="null"/> (the default) to leave memory entries durable
    /// unless a TTL is supplied explicitly at write time. When set it must be a
    /// positive, finite duration - the core write path rejects a non-positive
    /// TTL - which the paired <c>RepoContextTtlOptionsValidator</c> enforces at
    /// first resolve.
    /// </summary>
    public TimeSpan? DefaultMemoryTtl { get; set; }

    /// <summary>
    /// Policy switch guaranteeing that structural records (repo, package, file,
    /// and symbol nodes) never carry an expiry, so a durable model of the
    /// codebase is not silently reaped alongside ephemeral notes. Defaults to
    /// <see langword="true"/>; a consumer that writes a structural record must
    /// omit any TTL while this is set.
    /// </summary>
    public bool StructuralRecordsNeverExpire { get; set; } = true;
}
