namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A partial progress delta reported by an indexing run at a phase boundary. Every
/// field is optional: a report carries only the values that changed, and the job
/// grain merges each non-null field into its durable state. It crosses the grain
/// boundary (runner to job grain), so it carries Orleans serialization metadata.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexProgressUpdate)]
[Immutable]
public readonly record struct RepoIndexProgressUpdate
{
    /// <summary>The phase now executing, or null to leave the phase unchanged.</summary>
    [Id(0)]
    public RepoIndexPhase? Phase { get; init; }

    /// <summary>Files the walk discovered after filtering, or null.</summary>
    [Id(1)]
    public int? FilesScanned { get; init; }

    /// <summary>Files newly ingested, or null.</summary>
    [Id(2)]
    public int? FilesAdded { get; init; }

    /// <summary>Files whose record was updated, or null.</summary>
    [Id(3)]
    public int? FilesUpdated { get; init; }

    /// <summary>Stored files pruned, or null.</summary>
    [Id(4)]
    public int? FilesRemoved { get; init; }

    /// <summary>Files left untouched, or null.</summary>
    [Id(5)]
    public int? FilesUnchanged { get; init; }

    /// <summary>The total atomic write chunks to commit, or null.</summary>
    [Id(6)]
    public int? ChunksTotal { get; init; }

    /// <summary>The atomic write chunks committed so far, or null.</summary>
    [Id(7)]
    public int? ChunksCommitted { get; init; }

    /// <summary>Changed files whose vectors have been stored, or null.</summary>
    [Id(8)]
    public int? FilesEmbedded { get; init; }

    /// <summary>
    /// Files whose searchable content projection was written this run (added,
    /// updated, and back-filled files), or null. The content-phase analogue of
    /// <see cref="FilesEmbedded"/>.
    /// </summary>
    [Id(9)]
    public int? FilesContentProjected { get; init; }
}
