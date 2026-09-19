namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A startup statement about where this host's durable agent memory lives and what
/// does and does not protect it.
/// </summary>
/// <param name="Lines">The statement, one line per point, ready to be logged in order.</param>
/// <param name="IsWarning">Whether the statement should be emitted at warning level.</param>
/// <param name="IsArchived">Whether an out-of-store memory archive is configured at all.</param>
/// <param name="ArchiveSharesDataRoot">
/// Whether the configured archive directory resolves inside the store's own data root.
/// When it does, the archive is destroyed by the same gesture it exists to survive and
/// protects nothing, which is worse than having none because it reads as protection.
/// </param>
public sealed record RepoContextMemoryDurabilityStatement(
    IReadOnlyList<string> Lines,
    bool IsWarning,
    bool IsArchived,
    bool ArchiveSharesDataRoot);

/// <summary>
/// Builds the startup statement about durable-memory durability, so a host reports it
/// rather than restating what this package knows.
/// <para>
/// <b>The statement this makes, and why it is worded the way it is.</b> A
/// repository-context store holds two kinds of state that a wipe cannot tell apart.
/// The code index is derived and rebuilds by re-indexing, in minutes. Agent memory -
/// decisions, gotchas, conventions, glossary, authored across sessions - rebuilds from
/// nothing at all. They share a volume, and they cannot currently be separated: the
/// file write-ahead log has one configured root directory for every tree, and every
/// B+ tree grain persists through one storage provider, so both durable planes of the
/// memory tree are interleaved with every other tree's.
/// </para>
/// <para>
/// <b>What this deliberately does not say.</b> It never claims memory is on its own
/// volume, because it is not and cannot be. It never claims the archive is a backup,
/// because it covers one tree, keeps two generations, and has no manifest or
/// retention policy. It always states the residual window - protection reaches only
/// as far as the last successful export - because a reader who is told they are
/// protected, without being told until when, has had a visible risk converted into an
/// invisible one. An overclaiming reassurance is worse than no reassurance.
/// </para>
/// </summary>
public static class RepoContextMemoryDurabilityReport
{
    /// <summary>
    /// Describes durable-memory durability for a host with the given storage layout,
    /// reading the archive configuration from the process environment.
    /// </summary>
    /// <param name="dataRoot">The root directory holding this host's durable state, or <see langword="null"/> when unknown.</param>
    /// <param name="walDirectory">The write-ahead log root directory, or <see langword="null"/> when unknown.</param>
    /// <param name="grainStorePath">The grain-state store path (for example the SQLite file), or <see langword="null"/> when unknown.</param>
    /// <returns>The statement to emit at startup.</returns>
    public static RepoContextMemoryDurabilityStatement Describe(
        string? dataRoot,
        string? walDirectory,
        string? grainStorePath)
        => Describe(dataRoot, walDirectory, grainStorePath, RepoContextMemoryArchiveOptions.FromEnvironment());

    /// <summary>
    /// Describes durable-memory durability for an explicit storage layout and archive
    /// configuration. The environment-reading overload is what a host calls; this one
    /// exists so the statement is testable without mutating process state.
    /// </summary>
    /// <param name="dataRoot">The root directory holding this host's durable state, or <see langword="null"/> when unknown.</param>
    /// <param name="walDirectory">The write-ahead log root directory, or <see langword="null"/> when unknown.</param>
    /// <param name="grainStorePath">The grain-state store path, or <see langword="null"/> when unknown.</param>
    /// <param name="options">The resolved archive configuration. Must not be <see langword="null"/>.</param>
    /// <returns>The statement to emit at startup.</returns>
    internal static RepoContextMemoryDurabilityStatement Describe(
        string? dataRoot,
        string? walDirectory,
        string? grainStorePath,
        RepoContextMemoryArchiveOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var archiveInsideDataRoot = options.IsEnabled && IsInside(options.Directory!, dataRoot);
        var lines = new List<string>
        {
            "DURABLE AGENT MEMORY: irreplaceable state, stored with rebuildable state.",
            $"  Memory tree            : {RepoContextTrees.Memory}",
            $"  Write-ahead log        : {Describe(walDirectory)}",
            $"  Grain state            : {Describe(grainStorePath)}",
            "  Shares a volume with rebuildable index state: YES, and it cannot be separated. "
                + "The write-ahead log has one root directory for every tree and every B+ tree grain "
                + "persists through one storage provider, so the memory tree's durable state is "
                + "interleaved with every other tree's in both planes. Per-tree volume isolation is "
                + "not achievable by configuration.",
            "  Consequence: any gesture that removes this host's data volume - docker compose "
                + "down -v, docker volume rm, deleting the data directory - destroys authored memory "
                + "(decisions, gotchas, conventions, glossary) permanently. The code index rebuilds by "
                + "re-indexing; memory does not rebuild from anything.",
            "  To drop only the rebuildable index and keep memory, use the repocontext_reset_index "
                + "tool rather than a volume-level wipe.",
        };

        if (!options.IsEnabled)
        {
            lines.Add(
                $"  Memory archive         : NOT CONFIGURED (set {RepoContextMemoryArchiveOptions.DirectoryKey} "
                + "to a path outside this host's data volume). Nothing outside the volume holds a copy "
                + "of this store's memory, so a volume wipe is unrecoverable.");
            return new RepoContextMemoryDurabilityStatement(lines, IsWarning: true, IsArchived: false, ArchiveSharesDataRoot: false);
        }

        lines.Add($"  Memory archive         : {options.Directory}");
        lines.Add(
            $"  Archive cadence        : every {FormatInterval(options.EffectiveInterval)}, plus one "
            + $"bounded export on graceful shutdown (budget {FormatInterval(options.EffectiveStopTimeout)}).");
        lines.Add($"  Archive restore        : {options.RestoreMode.ToString().ToLowerInvariant()}{DescribeRestore(options.RestoreMode)}");

        if (archiveInsideDataRoot)
        {
            lines.Add(
                "  GUARDED AGAINST A VOLUME WIPE: NO. The archive directory resolves INSIDE this host's "
                + $"data root ({Describe(dataRoot)}), so the same gesture that destroys the store destroys "
                + "the archive with it. Move the archive to a host bind mount or an external volume; until "
                + "then it protects nothing, and its presence is misleading rather than reassuring.");
            return new RepoContextMemoryDurabilityStatement(lines, IsWarning: true, IsArchived: true, ArchiveSharesDataRoot: true);
        }

        lines.Add(
            "  GUARDED AGAINST: loss of this host's data volume, IF the archive path is a host bind "
            + "mount or an external volume. Verify that yourself - this host can see that the path is "
            + "outside its data root, but it cannot see what the path is mounted from, and a plain "
            + "directory inside the container is destroyed with the container.");
        lines.Add(
            "  NOT GUARDED AGAINST: loss of the archive path itself; memory authored since the last "
            + "successful export (protection reaches exactly as far as that export and no further); "
            + "anything outside the memory tree, which this archive does not carry. This is not a "
            + "backup and it does not make the container self-healing beyond restoring memory into an "
            + "empty store.");

        return new RepoContextMemoryDurabilityStatement(lines, IsWarning: true, IsArchived: true, ArchiveSharesDataRoot: false);
    }

    private static string DescribeRestore(RepoContextMemoryArchiveRestoreMode mode) => mode switch
    {
        RepoContextMemoryArchiveRestoreMode.Off =>
            " (the archive is written but is never imported automatically; restore it by hand)",
        RepoContextMemoryArchiveRestoreMode.Auto =>
            " (imported at startup only when the memory tree is empty, which is the state a wipe leaves)",
        _ =>
            " (imported at every startup and merged into whatever is already stored; the merge is a "
            + "CRDT join, so a newer live entry is not regressed by an older archived one)",
    };

    private static string Describe(string? path) =>
        string.IsNullOrWhiteSpace(path) ? "(not reported by this host)" : path;

    private static string FormatInterval(TimeSpan value) =>
        value.TotalSeconds < 90
            ? $"{value.TotalSeconds:0.###}s"
            : $"{value.TotalMinutes:0.###}m";

    /// <summary>
    /// Whether <paramref name="candidate"/> resolves to <paramref name="root"/> or a
    /// path beneath it. A path that cannot be resolved is reported as not inside,
    /// because asserting containment on a path this host could not read would be a
    /// claim it has no evidence for.
    /// </summary>
    private static bool IsInside(string candidate, string? root)
    {
        if (string.IsNullOrWhiteSpace(root))
        {
            return false;
        }

        try
        {
            var comparison = OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;

            var fullRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
            var fullCandidate = Path.TrimEndingDirectorySeparator(Path.GetFullPath(candidate));

            return string.Equals(fullCandidate, fullRoot, comparison)
                || fullCandidate.StartsWith(fullRoot + Path.DirectorySeparatorChar, comparison);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return false;
        }
    }
}
