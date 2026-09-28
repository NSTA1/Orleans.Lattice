namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Per-repository time-to-live policy for the repository-context surface. Bound
/// per repository through the named-options convention -
/// <c>IOptionsMonitor&lt;RepoContextTtlOptions&gt;.Get(repoId)</c> - so each
/// repository can carry its own memory-entry lifetime. The store resolves the
/// repository name directly; callers that want a fallback should configure that
/// name explicitly.
/// <para>
/// This type only <b>surfaces</b> the per-entry expiry that Orleans.Lattice core
/// already provides. Memory entries are written through the multi-value-register
/// accessor (<see cref="MvRegisterAccessor{T}"/>), whose time-to-live write reaches
/// the TTL overload of
/// <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], System.TimeSpan, System.Threading.CancellationToken)"/>:
/// the TTL is converted to an absolute UTC expiry at write time and joined with any
/// expiry the entry already carries by keeping the later of the two, so a TTL can
/// give a durable memory entry an expiry or push an existing expiry later, but
/// never move an existing expiry earlier; reads then hide expired
/// entries and background tombstone compaction reaps them. It introduces no new
/// expiry mechanism. The memory-writing tools that consume these options are
/// layered on separately.
/// </para>
/// </summary>
public sealed class RepoContextTtlOptions
{
    /// <summary>
    /// The default time-to-live applied to an agent-authored memory entry when it
    /// is created without an explicit per-entry TTL (updating an existing entry
    /// never applies it), or <see langword="null"/> (the default) to leave memory
    /// entries durable unless a TTL is supplied explicitly at write time. When set
    /// it must be strictly positive - the memory write path would treat a
    /// non-positive TTL as no TTL at all and leave the entry durable - which the
    /// paired <c>RepoContextTtlOptionsValidator</c> enforces at first resolve.
    /// </summary>
    public TimeSpan? DefaultMemoryTtl { get; set; }

    /// <summary>
    /// Policy switch reserved for structural-record TTL handling. It defaults to
    /// <see langword="true"/>, but the current structural write path does not
    /// read this flag and omits TTLs unconditionally.
    /// </summary>
    public bool StructuralRecordsNeverExpire { get; set; } = true;
}
