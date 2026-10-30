namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The optional cross-walk pruning context for <see cref="RepoTreeWalker"/>. It lets a
/// periodic reconcile skip the per-file <c>stat</c> of directories whose modification
/// time is unchanged since the previous walk - the dominant cost of a no-op walk over a
/// large tree on a slow (for example bind-mounted) filesystem - while still recursing
/// into every subdirectory, so a change nested under an unchanged ancestor is never
/// missed.
/// <para>
/// <b>What pruning detects, and what it cannot.</b> A directory's modification time
/// changes when an entry is added to, removed from, or renamed within it - including an
/// editor's write-to-temp-then-rename atomic save - so those are caught cheaply. It does
/// <i>not</i> change when a file already in the directory is edited in place, so an
/// in-place content edit is invisible to pruning. The periodic <see cref="ForceFull"/>
/// sweep is the backstop that re-stats every file and catches exactly those. Pruning is a
/// cost optimisation only: correctness for in-place edits rests on the force-full sweep,
/// not on pruning.
/// </para>
/// <para>
/// This is an in-memory reconcile artefact only - it never crosses an Orleans wire, so it
/// carries no serialization attributes. It is rebuilt from an empty prior snapshot after a
/// process restart, which makes the first post-restart walk a full one (correct by
/// construction).
/// </para>
/// </summary>
internal sealed class RepoWalkPruning
{
    /// <summary>
    /// The directory modification times captured by the previous walk (repository-relative
    /// POSIX directory path, with the root itself keyed by the empty string, to
    /// modification-time ticks). When <see langword="null"/> or empty every directory is
    /// walked in full - the cold behaviour - because there is no prior baseline to prune
    /// against.
    /// </summary>
    public IReadOnlyDictionary<string, long>? PreviousDirectoryMtimes { get; init; }

    /// <summary>
    /// The wall-clock UTC tick at which the previous walk began - the
    /// <see cref="WalkStartTicks"/> it recorded, handed forward with
    /// <see cref="PreviousDirectoryMtimes"/>. A directory is pruned only when its
    /// modification time is unchanged <i>and</i> at least <see cref="RacyWindow"/> older
    /// than this tick. <c>0</c> (unknown) prunes nothing.
    /// <para>
    /// <b>Why the second condition.</b> An unchanged modification time proves nothing
    /// for a directory modified in the same timestamp tick in which the previous walk
    /// read it: an entry added after that read leaves the time unchanged, because
    /// filesystem timestamps are coarser than the walk (the Linux kernel stamps from a
    /// coarse clock that lags real time by up to a scheduler tick; FAT and some
    /// bind-mounted filesystems record whole seconds or worse). Such a directory is
    /// "racily clean", the window git closes for its index the same way. Without this
    /// condition the new entry stays invisible until the next forced full sweep -
    /// issue #5268 - which the file-level fast path in <see cref="RepoTreeWalker"/>
    /// already guards against with its strict comparison to the ingest anchor.
    /// </para>
    /// </summary>
    public long PreviousWalkStartTicks { get; init; }

    /// <summary>
    /// The wall-clock UTC tick at which this walk began, read before any directory's
    /// modification time, so it is never later than a time this walk recorded. Hand it to
    /// the next walk as <see cref="PreviousWalkStartTicks"/>.
    /// </summary>
    public long WalkStartTicks { get; internal set; }

    /// <summary>
    /// How much older than <see cref="PreviousWalkStartTicks"/> a directory's modification
    /// time must be before an unchanged time is trusted. Generous on purpose: it must
    /// cover the coarsest timestamp resolution a walked filesystem uses plus any skew
    /// between the clock that stamps a bind-mounted file and the walker's clock, and its
    /// only cost is that a directory modified within it is walked in full once more.
    /// </summary>
    public static readonly TimeSpan RacyWindow = TimeSpan.FromSeconds(2);

    /// <summary>
    /// When <see langword="true"/>, the prior snapshot is ignored and every file is
    /// stat'd: the periodic full sweep whose whole purpose is to catch the in-place
    /// content edits pruning cannot see.
    /// </summary>
    public bool ForceFull { get; init; }

    /// <summary>
    /// The directory modification times observed by this walk, to hand to the next one.
    /// The walk records one entry for every directory it visits, whether that directory
    /// was pruned or walked in full, so the snapshot stays complete and self-heals as the
    /// tree changes.
    /// </summary>
    public Dictionary<string, long> CurrentDirectoryMtimes { get; } = new(StringComparer.Ordinal);

    /// <summary>The number of directories whose per-file stats were skipped this walk (diagnostics).</summary>
    public int PrunedDirectoryCount { get; set; }

    /// <summary>The number of files carried forward without a stat this walk (diagnostics).</summary>
    public int PrunedFileCount { get; set; }
}
