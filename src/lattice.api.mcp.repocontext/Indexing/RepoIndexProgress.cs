namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A point-in-time snapshot of a repository indexing job: its lifecycle status,
/// the phase it is executing, the running reconciliation counters, and timing.
/// It is both the value the <c>repocontext_index_status</c> tool returns and the
/// acknowledgement the onboarding tools return when they start (or re-attach to) a
/// job, so a caller can poll one shape to follow a long onboarding pass to
/// completion. It crosses the grain boundary and is projected to MCP JSON, so it
/// carries Orleans serialization metadata.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexProgress)]
[Immutable]
public sealed record RepoIndexProgress
{
    /// <summary>The repository identity this job indexes.</summary>
    [Id(0)]
    public required string RepoId { get; init; }

    /// <summary>The job lifecycle state.</summary>
    [Id(1)]
    public required RepoIndexStatus Status { get; init; }

    /// <summary>The phase currently executing (or the last phase reached).</summary>
    [Id(2)]
    public required RepoIndexPhase Phase { get; init; }

    /// <summary>Files the walk discovered after filtering. Zero until the walk completes.</summary>
    [Id(3)]
    public int FilesScanned { get; init; }

    /// <summary>Files newly ingested that had no prior stored record.</summary>
    [Id(4)]
    public int FilesAdded { get; init; }

    /// <summary>Files whose content digest changed and whose record was updated.</summary>
    [Id(5)]
    public int FilesUpdated { get; init; }

    /// <summary>Stored files that no longer exist in the tree and were pruned.</summary>
    [Id(6)]
    public int FilesRemoved { get; init; }

    /// <summary>Files whose digest matched the stored record and were left untouched.</summary>
    [Id(7)]
    public int FilesUnchanged { get; init; }

    /// <summary>
    /// The total number of atomic write chunks the apply phase must commit. Zero
    /// until the plan is computed. Together with <see cref="ChunksCommitted"/> it
    /// gives a durable-progress fraction that survives a restart.
    /// </summary>
    [Id(8)]
    public int ChunksTotal { get; init; }

    /// <summary>The number of atomic write chunks committed so far.</summary>
    [Id(9)]
    public int ChunksCommitted { get; init; }

    /// <summary>The number of changed files whose vectors have been embedded and stored.</summary>
    [Id(10)]
    public int FilesEmbedded { get; init; }

    /// <summary>
    /// How many times this job has been started or resumed. A value above one
    /// means an earlier attempt was interrupted (for example by a host restart)
    /// and the reminder resumed it.
    /// </summary>
    [Id(11)]
    public int Attempt { get; init; }

    /// <summary>When the current (or most recent) attempt started, in UTC.</summary>
    [Id(12)]
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>When progress was last recorded, in UTC.</summary>
    [Id(13)]
    public DateTimeOffset? UpdatedAt { get; init; }

    /// <summary>When the job reached a terminal state, in UTC; null while running.</summary>
    [Id(14)]
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    /// The wall-clock duration of a completed run in milliseconds; null until the
    /// job completes.
    /// </summary>
    [Id(15)]
    public long? ElapsedMilliseconds { get; init; }

    /// <summary>
    /// The failure reason when <see cref="Status"/> is
    /// <see cref="RepoIndexStatus.Failed"/>; null otherwise.
    /// </summary>
    [Id(16)]
    public string? Error { get; init; }

    /// <summary>
    /// The number of files whose searchable content projection was written during
    /// this run (added, updated, and content back-filled files). The content-phase
    /// analogue of <see cref="FilesEmbedded"/>: it lets a caller watch a large
    /// content back-fill - a repository indexed before the content projection
    /// existed re-reads every text file - converge alongside the embedding count.
    /// </summary>
    [Id(17)]
    public int FilesContentProjected { get; init; }
}
