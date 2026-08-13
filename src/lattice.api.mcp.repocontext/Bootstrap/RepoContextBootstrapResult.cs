namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The structured summary a <c>repocontext_bootstrap</c> run returns: how many
/// files the scan saw and how they were reconciled against the store, how many
/// symbols were captured, and how long the run took. An agent uses it to confirm
/// an onboarding pass populated the expected baseline and to see, on a re-run,
/// that an unchanged repository was a no-op (every counter but
/// <see cref="FilesScanned"/> and <see cref="FilesUnchanged"/> is zero).
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextBootstrapResult
{
    /// <summary>The repository identity records were filed under.</summary>
    public required string RepoId { get; init; }

    /// <summary>The total number of files the scan walked (after filtering).</summary>
    public required int FilesScanned { get; init; }

    /// <summary>Files newly ingested that had no prior stored record.</summary>
    public required int FilesAdded { get; init; }

    /// <summary>Files whose content digest changed and whose record was updated.</summary>
    public required int FilesUpdated { get; init; }

    /// <summary>Stored files that no longer exist in the tree and were pruned.</summary>
    public required int FilesRemoved { get; init; }

    /// <summary>Files whose digest matched the stored record and were left untouched.</summary>
    public required int FilesUnchanged { get; init; }

    /// <summary>
    /// The number of symbol records captured. Structural ingestion ships first, so
    /// this is zero until a language parser is wired into the walk by later work.
    /// </summary>
    public required int SymbolsCaptured { get; init; }

    /// <summary>The wall-clock duration of the run, in milliseconds.</summary>
    public required long ElapsedMilliseconds { get; init; }
}
