using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>The outcome of one archive export attempt.</summary>
internal enum RepoContextMemoryArchiveExportOutcome
{
    /// <summary>The snapshot was written and renamed into place.</summary>
    Written,

    /// <summary>
    /// The export was refused because it would have replaced a non-empty archive with
    /// an empty one. See <see cref="RepoContextMemoryArchive"/> for why that case is a
    /// refusal rather than a write.
    /// </summary>
    RefusedEmptyOverNonEmpty,

    /// <summary>The export did not finish: it was cancelled, ran out of budget, or threw.</summary>
    Incomplete,
}

/// <summary>The result of one archive export attempt.</summary>
/// <param name="Outcome">What happened.</param>
/// <param name="RecordCount">The number of memory records written, or the number enumerated before the attempt ended.</param>
/// <param name="Reason">A short human-readable explanation, or <see langword="null"/> on success.</param>
internal readonly record struct RepoContextMemoryArchiveExport(
    RepoContextMemoryArchiveExportOutcome Outcome,
    long RecordCount,
    string? Reason);

/// <summary>The result of one archive restore attempt.</summary>
/// <param name="Restored">Whether any snapshot was imported.</param>
/// <param name="RecordsRead">The number of records read from the snapshot.</param>
/// <param name="SourcePath">The snapshot file that was imported, or <see langword="null"/> when none was.</param>
/// <param name="Reason">Why no restore happened, or <see langword="null"/> when one did.</param>
internal readonly record struct RepoContextMemoryArchiveRestore(
    bool Restored,
    long RecordsRead,
    string? SourcePath,
    string? Reason);

/// <summary>
/// Exports the durable agent-memory tree to a snapshot file outside the store's own
/// volume, and imports it back.
/// <para>
/// <b>The write is never in place.</b> Every export is written to a temporary file in
/// the archive directory, flushed to disk, and only then renamed over the live path,
/// after the live path has itself been renamed to the previous generation. An export
/// that is interrupted at any point - cancelled, out of budget, killed, or throwing
/// mid-stream - therefore leaves a stray temporary file and two intact, complete
/// snapshots behind it. This matters more than it looks: an archive is a recovery
/// mechanism, and a recovery mechanism that can half-overwrite its own only copy is a
/// second way to lose the data it was added to protect.
/// </para>
/// <para>
/// <b>An empty export never replaces a non-empty archive.</b> The sequence this
/// feature exists for is a wipe followed by a restart, and in that sequence the store
/// comes up empty with a good archive on disk. If the periodic export ran first and
/// wrote its empty view over that archive, the wipe would be completed rather than
/// survived. So a zero-record export against a non-empty archive is refused and
/// reported, not written.
/// </para>
/// <para>
/// The snapshot is the package's own portable format, so it is written and read by
/// the same primitive the store's other portability flows use, and an import folds
/// each record through the record model's CRDT join rather than overwriting.
/// </para>
/// </summary>
internal sealed class RepoContextMemoryArchive(RepoContextMemoryArchiveOptions options)
{
    /// <summary>The name of the live snapshot inside the archive directory.</summary>
    internal const string SnapshotFileName = "repo-context-memory.snapshot";

    /// <summary>The name the prior generation is renamed to before a new snapshot lands.</summary>
    internal const string PreviousSnapshotFileName = "repo-context-memory.previous.snapshot";

    /// <summary>The extension every in-flight temporary snapshot carries.</summary>
    internal const string TemporaryExtension = ".tmp";

    private readonly RepoContextMemoryArchiveOptions options = options
        ?? throw new ArgumentNullException(nameof(options));

    /// <summary>
    /// The file-open seam, so a test can interrupt a write part-way and assert the
    /// prior archive survived. Production always opens a real exclusive file.
    /// </summary>
    internal Func<string, Stream> OpenForWrite { get; init; } =
        path => new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);

    /// <summary>The archive directory, or <see langword="null"/> when no archive is configured.</summary>
    internal string? Directory => options.Directory;

    /// <summary>The path the live snapshot occupies, or <see langword="null"/> when no archive is configured.</summary>
    internal string? SnapshotPath =>
        options.Directory is null ? null : Path.Combine(options.Directory, SnapshotFileName);

    /// <summary>The path the prior generation occupies, or <see langword="null"/> when no archive is configured.</summary>
    internal string? PreviousSnapshotPath =>
        options.Directory is null ? null : Path.Combine(options.Directory, PreviousSnapshotFileName);

    /// <summary>
    /// Exports every live memory record to the archive, atomically.
    /// </summary>
    /// <param name="tree">The memory tree to export. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer for snapshot records. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the export. A cancelled export leaves the existing archive untouched.</param>
    /// <returns>The export outcome.</returns>
    internal async Task<RepoContextMemoryArchiveExport> ExportAsync(
        ILattice tree,
        Serializer serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(serializer);

        var directory = options.Directory
            ?? throw new InvalidOperationException("No memory archive directory is configured.");

        System.IO.Directory.CreateDirectory(directory);
        SweepStaleTemporaries(directory);

        var snapshotPath = Path.Combine(directory, SnapshotFileName);
        var previousPath = Path.Combine(directory, PreviousSnapshotFileName);
        var temporaryPath = Path.Combine(
            directory,
            SnapshotFileName + "." + Guid.NewGuid().ToString("N") + TemporaryExtension);

        long count;
        try
        {
            await using (var destination = OpenForWrite(temporaryPath))
            {
                count = await RepoContextPortability.ExportAsync(
                        tree,
                        RepoContextKeys.AllReposPrefix(),
                        destination,
                        serializer,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                await destination.FlushAsync(CancellationToken.None).ConfigureAwait(false);
                if (destination is FileStream file)
                {
                    // Flush the operating system's write-behind cache too: a rename is
                    // only atomic with respect to the data that actually reached disk.
                    file.Flush(flushToDisk: true);
                }
            }
        }
        catch (Exception ex)
        {
            TryDelete(temporaryPath);
            return new RepoContextMemoryArchiveExport(
                RepoContextMemoryArchiveExportOutcome.Incomplete,
                RecordCount: 0,
                Reason: ex is OperationCanceledException
                    ? "the export was cancelled before it completed"
                    : ex.Message);
        }

        if (count == 0 && SnapshotHasRecords(snapshotPath))
        {
            TryDelete(temporaryPath);
            return new RepoContextMemoryArchiveExport(
                RepoContextMemoryArchiveExportOutcome.RefusedEmptyOverNonEmpty,
                RecordCount: 0,
                Reason: "the store holds no memory records but the existing archive does; "
                    + "the archive was left intact rather than emptied");
        }

        // Rotate, then land. Between the two renames the previous generation is a
        // complete snapshot, so a crash in the gap loses no data - the restore path
        // falls back to it.
        if (File.Exists(snapshotPath))
        {
            File.Move(snapshotPath, previousPath, overwrite: true);
        }

        File.Move(temporaryPath, snapshotPath, overwrite: true);
        return new RepoContextMemoryArchiveExport(
            RepoContextMemoryArchiveExportOutcome.Written, count, Reason: null);
    }

    /// <summary>
    /// Imports the archived snapshot into <paramref name="tree"/>, honouring the
    /// configured restore mode. The live snapshot is preferred; the previous
    /// generation is used when the live one is missing or unreadable.
    /// </summary>
    /// <param name="tree">The memory tree to import into. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer for snapshot records. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the restore.</param>
    /// <returns>The restore outcome.</returns>
    internal async Task<RepoContextMemoryArchiveRestore> RestoreAsync(
        ILattice tree,
        Serializer serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(serializer);

        if (options.RestoreMode == RepoContextMemoryArchiveRestoreMode.Off)
        {
            return new RepoContextMemoryArchiveRestore(
                false, 0, null, $"restore is {RepoContextMemoryArchiveRestoreMode.Off}");
        }

        var directory = options.Directory
            ?? throw new InvalidOperationException("No memory archive directory is configured.");

        var candidates = new[]
        {
            Path.Combine(directory, SnapshotFileName),
            Path.Combine(directory, PreviousSnapshotFileName),
        };

        if (!candidates.Any(File.Exists))
        {
            return new RepoContextMemoryArchiveRestore(
                false, 0, null, "no archived snapshot exists yet");
        }

        if (options.RestoreMode == RepoContextMemoryArchiveRestoreMode.Auto
            && await HasAnyMemoryAsync(tree, cancellationToken).ConfigureAwait(false))
        {
            return new RepoContextMemoryArchiveRestore(
                false,
                0,
                null,
                "the store already holds memory records, so nothing was restored "
                    + "(restore mode auto only heals an empty store)");
        }

        string? lastFailure = null;
        foreach (var candidate in candidates)
        {
            if (!File.Exists(candidate))
            {
                continue;
            }

            try
            {
                await using var source = new FileStream(
                    candidate, FileMode.Open, FileAccess.Read, FileShare.Read);
                var result = await RepoContextPortability.ImportAsync(
                        tree, source, serializer, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return new RepoContextMemoryArchiveRestore(
                    true, result.RecordsRead, candidate, Reason: null);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastFailure = $"{Path.GetFileName(candidate)} could not be imported ({ex.Message})";
            }
        }

        return new RepoContextMemoryArchiveRestore(false, 0, null, lastFailure);
    }

    /// <summary>
    /// Whether the memory tree holds any record at all. One record is enough to
    /// answer, so this is a single-entry probe rather than a count.
    /// </summary>
    /// <param name="tree">The memory tree to probe.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    internal static async Task<bool> HasAnyMemoryAsync(
        ILattice tree, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);

        var page = await RepoContextPortability.EnumerateAsync(
                tree,
                RepoContextKeys.AllReposPrefix(),
                continuationToken: null,
                pageSize: 1,
                vectorExport: null,
                cancellationToken)
            .ConfigureAwait(false);

        return page.Records.Count > 0;
    }

    /// <summary>
    /// Whether a snapshot file exists and carries at least one record. A snapshot with
    /// only its header is a legitimately empty export, not a corrupt file, so it is
    /// treated as empty rather than as something worth preserving.
    /// </summary>
    private static bool SnapshotHasRecords(string path)
    {
        try
        {
            return File.Exists(path) && new FileInfo(path).Length > RepoContextSnapshotFormat.HeaderLength;
        }
        catch (IOException)
        {
            // Unreadable is not the same as empty. Assume it carries records, which
            // biases towards refusing the overwrite - the conservative direction.
            return true;
        }
    }

    /// <summary>
    /// Removes temporary files left by an export that never completed. They are dead
    /// weight rather than a hazard (nothing ever reads them), but an archive directory
    /// that accumulates one per interrupted shutdown reads as a fault.
    /// </summary>
    private static void SweepStaleTemporaries(string directory)
    {
        try
        {
            foreach (var stale in System.IO.Directory.EnumerateFiles(
                directory, "*" + TemporaryExtension, SearchOption.TopDirectoryOnly))
            {
                TryDelete(stale);
            }
        }
        catch (IOException)
        {
            // Sweeping is housekeeping; failing it must never fail an export.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
