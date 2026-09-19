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
    /// The cumulative number of index runs that have been started for this
    /// repository. It counts run starts, not retries or failures: the first
    /// onboarding is one, and every subsequent re-drive adds one - each periodic
    /// reconcile that picks up on-disk edits and deletions, each gap back-fill,
    /// each re-drive of a failed run, and each reminder-driven resume after a
    /// host restart. Because the self-index grain reconciles on a schedule, this
    /// value rises steadily on a healthy, actively-maintained repository, so a
    /// high value is normal and is not by itself a sign of interruption or error.
    /// </summary>
    [Id(11)]
    public int Attempt { get; init; }

    /// <summary>When the current (or most recent) index run started, in UTC.</summary>
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

    /// <summary>
    /// The number of symbol passages whose vectors were embedded and stored during
    /// this run.
    /// <para>
    /// Reported separately from <see cref="FilesEmbedded"/> because the two count
    /// different things and conflating them made a healthy run look dead. The
    /// symbol arm embeds captured symbols rather than files, so on a pass whose
    /// file coverage is already complete <see cref="FilesEmbedded"/> is
    /// legitimately zero while the symbol arm runs for a long time - and before
    /// this field existed that arm reported no progress of any kind, so
    /// <see cref="UpdatedAt"/> froze at the file arm's last report and
    /// <c>index_status</c> was byte-identical across readings taken an hour apart.
    /// A repository converging at hundreds of vectors a minute was therefore
    /// indistinguishable from a stalled one, and the documented diagnostic rule
    /// ("a stalled updatedAt warrants giving up") pointed at a destructive
    /// re-onboard of a perfectly healthy index.
    /// </para>
    /// <para>
    /// <c>list_repos</c>'s <c>embeddedVectorCount</c> counts sources of every kind
    /// - files and symbols together - which is why it can be seen rising while
    /// <see cref="FilesEmbedded"/> stays at zero. That is not a contradiction
    /// between the two surfaces: it is the sum being observed against only one of
    /// its terms. This field supplies the missing term.
    /// </para>
    /// </summary>
    [Id(18)]
    public int SymbolsEmbedded { get; init; }

    /// <summary>
    /// The number of code-index trees a reset sweep has dropped so far, out of
    /// the fixed set it sweeps. Zero outside a reset. It is the reset's
    /// coarse-grained progress evidence: while <see cref="Phase"/> is
    /// <see cref="RepoIndexPhase.Resetting"/> this advances as each tree is
    /// tombstoned, so a caller can see a teardown making progress rather than
    /// merely a boolean "resetting" flag that a wedged reset would also show.
    /// </summary>
    [Id(19)]
    public int TreesSwept { get; init; }

    /// <summary>
    /// The number of entries a reset sweep has tombstoned across the code-index
    /// trees so far. Zero outside a reset. The fine-grained companion to
    /// <see cref="TreesSwept"/>: it advances within a single large tree's drain,
    /// so a reset dropping a big corpus is observably moving even while
    /// <see cref="TreesSwept"/> holds steady on one tree. On completion it equals
    /// the reset result's <c>EntriesDeleted</c>.
    /// </summary>
    [Id(20)]
    public int EntriesDeleted { get; init; }
}
