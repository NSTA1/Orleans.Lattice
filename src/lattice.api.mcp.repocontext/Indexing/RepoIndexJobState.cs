namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The persisted state of a repository indexing job grain. It holds everything
/// needed to observe a run and to resume it after a host restart: the durable
/// request (so a resume needs no client), the lifecycle status and phase, the
/// running reconciliation counters, and timing. One instance is stored per
/// repository under the job grain's key.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexJobState)]
internal sealed class RepoIndexJobState
{
    /// <summary>
    /// The durable job inputs, or null before any job has been started. Present
    /// once a job runs so the reminder can resume it without the original call.
    /// </summary>
    [Id(0)]
    public RepoIndexJobRequest? Request { get; set; }

    /// <summary>The job lifecycle state.</summary>
    [Id(1)]
    public RepoIndexStatus Status { get; set; } = RepoIndexStatus.None;

    /// <summary>The phase currently executing (or the last phase reached).</summary>
    [Id(2)]
    public RepoIndexPhase Phase { get; set; } = RepoIndexPhase.Pending;

    /// <summary>Files the walk discovered after filtering.</summary>
    [Id(3)]
    public int FilesScanned { get; set; }

    /// <summary>Files newly ingested that had no prior stored record.</summary>
    [Id(4)]
    public int FilesAdded { get; set; }

    /// <summary>Files whose content digest changed and whose record was updated.</summary>
    [Id(5)]
    public int FilesUpdated { get; set; }

    /// <summary>Stored files that no longer exist in the tree and were pruned.</summary>
    [Id(6)]
    public int FilesRemoved { get; set; }

    /// <summary>Files whose digest matched the stored record and were left untouched.</summary>
    [Id(7)]
    public int FilesUnchanged { get; set; }

    /// <summary>The total number of atomic write chunks the apply phase must commit.</summary>
    [Id(8)]
    public int ChunksTotal { get; set; }

    /// <summary>The number of atomic write chunks committed so far.</summary>
    [Id(9)]
    public int ChunksCommitted { get; set; }

    /// <summary>The number of changed files whose vectors have been embedded and stored.</summary>
    [Id(10)]
    public int FilesEmbedded { get; set; }

    /// <summary>How many times this job has been started or resumed.</summary>
    [Id(11)]
    public int Attempt { get; set; }

    /// <summary>When the current (or most recent) attempt started, in UTC.</summary>
    [Id(12)]
    public DateTimeOffset? StartedAt { get; set; }

    /// <summary>When progress was last recorded, in UTC.</summary>
    [Id(13)]
    public DateTimeOffset? UpdatedAt { get; set; }

    /// <summary>When the job reached a terminal state, in UTC.</summary>
    [Id(14)]
    public DateTimeOffset? CompletedAt { get; set; }

    /// <summary>The wall-clock duration of the last completed run in milliseconds.</summary>
    [Id(15)]
    public long? ElapsedMilliseconds { get; set; }

    /// <summary>The failure reason when <see cref="Status"/> is failed; null otherwise.</summary>
    [Id(16)]
    public string? Error { get; set; }

    /// <summary>
    /// Projects the durable state into the immutable snapshot returned to callers.
    /// </summary>
    /// <param name="repoId">The repository identity carried in the grain key.</param>
    /// <returns>The point-in-time progress snapshot.</returns>
    public RepoIndexProgress ToProgress(string repoId) => new()
    {
        RepoId = repoId,
        Status = Status,
        Phase = Phase,
        FilesScanned = FilesScanned,
        FilesAdded = FilesAdded,
        FilesUpdated = FilesUpdated,
        FilesRemoved = FilesRemoved,
        FilesUnchanged = FilesUnchanged,
        ChunksTotal = ChunksTotal,
        ChunksCommitted = ChunksCommitted,
        FilesEmbedded = FilesEmbedded,
        Attempt = Attempt,
        StartedAt = StartedAt,
        UpdatedAt = UpdatedAt,
        CompletedAt = CompletedAt,
        ElapsedMilliseconds = ElapsedMilliseconds,
        Error = Error,
    };
}
