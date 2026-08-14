namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Centralised Orleans serialization alias constants for every type that
/// participates in the <c>Orleans.Lattice.Api.Mcp.RepoContext</c> wire format.
/// Each alias is a short, fixed string that provides a stable wire-format
/// identity independent of CLR type names - it is effectively part of the
/// stored-data contract and must never be renamed or removed.
/// <para>
/// The constants live in this package rather than the core
/// <c>Orleans.Lattice.TypeAliases</c> table because the core
/// <c>TypeAliasesTests.Every_alias_constant_is_referenced_by_exactly_one_type</c>
/// gate is scoped to the core assembly: a constant declared in core but
/// referenced only from a type in this (separate) assembly would be flagged as
/// dead. The <c>Orleans.Lattice.Scaling</c> and <c>Orleans.Lattice.Replication</c>
/// packages follow the same pattern with their own alias tables. The
/// repo-context aliases keep the canonical <c>ol.</c> prefix and are verified
/// unique against the core table at authoring time; the sibling
/// <c>RepoContextTypeAliasesTests</c> enforces the prefix, length, uniqueness,
/// and single-reference invariants for this assembly.
/// </para>
/// </summary>
internal static class RepoContextTypeAliases
{
    /// <summary>Alias for <see cref="RepoNode"/>.</summary>
    internal const string RepoNode = "ol.rcr";

    /// <summary>Alias for <see cref="PackageNode"/>.</summary>
    internal const string PackageNode = "ol.rcp";

    /// <summary>Alias for <see cref="FileNode"/>.</summary>
    internal const string FileNode = "ol.rcf";

    /// <summary>Alias for <see cref="SymbolRecord"/>.</summary>
    internal const string SymbolRecord = "ol.rcs";

    /// <summary>Alias for <see cref="MemoryRecord"/>.</summary>
    internal const string MemoryRecord = "ol.rcm";

    /// <summary>Alias for <see cref="RepoContext.SymbolKind"/>.</summary>
    internal const string SymbolKind = "ol.rck";

    /// <summary>Alias for <see cref="RepoContext.MemoryKind"/>.</summary>
    internal const string MemoryKind = "ol.rcn";

    /// <summary>Alias for <see cref="RepoContextRemainingLife"/>.</summary>
    internal const string RemainingLife = "ol.rcl";

    /// <summary>Alias for <see cref="RepoContext.VectorNormalization"/>.</summary>
    internal const string VectorNormalization = "ol.vn";

    /// <summary>Alias for <see cref="EmbeddingSpaceTag"/>.</summary>
    internal const string EmbeddingSpaceTag = "ol.vsp";

    /// <summary>Alias for <see cref="VectorMetadataRecord"/>.</summary>
    internal const string VectorMetadataRecord = "ol.vmd";

    /// <summary>Alias for <see cref="VectorPayloadRecord"/>.</summary>
    internal const string VectorPayloadRecord = "ol.vpl";

    /// <summary>Alias for <see cref="RepoContextSnapshotRecord"/>.</summary>
    internal const string SnapshotRecord = "ol.rcx";

    /// <summary>Alias for <see cref="RepoContext.RepoIndexStatus"/>.</summary>
    internal const string RepoIndexStatus = "ol.rct";

    /// <summary>Alias for <see cref="RepoContext.RepoIndexPhase"/>.</summary>
    internal const string RepoIndexPhase = "ol.rch";

    /// <summary>Alias for <see cref="RepoIndexJobRequest"/>.</summary>
    internal const string RepoIndexJobRequest = "ol.rcq";

    /// <summary>Alias for <see cref="RepoIndexProgress"/>.</summary>
    internal const string RepoIndexProgress = "ol.rcg";

    /// <summary>Alias for <see cref="RepoIndexJobState"/>.</summary>
    internal const string RepoIndexJobState = "ol.rcj";

    /// <summary>Alias for <see cref="RepoIndexProgressUpdate"/>.</summary>
    internal const string RepoIndexProgressUpdate = "ol.rcu";

    /// <summary>Alias for <see cref="RepoContextSelfIndexState"/>.</summary>
    internal const string RepoContextSelfIndexState = "ol.rcz";
}
