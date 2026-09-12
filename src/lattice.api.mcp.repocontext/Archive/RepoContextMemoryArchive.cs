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
/// <param name="Outcome">
/// What the attempt did. This is the discriminator: <see cref="Restored"/> alone
/// cannot distinguish "the tree is complete" from "the tree holds a partial import
/// that this attempt refused to touch", and those need different operator responses.
/// </param>
/// <param name="RecordsRead">The number of records read from the snapshot.</param>
/// <param name="RecordsInStore">
/// The number of memory records the tree holds after the attempt. On a failed import
/// this is the count that actually landed, which is the number
/// <paramref name="RecordsRead"/> cannot report: the reader counts records taken off
/// the wire, and an import that throws mid-stream has already written everything it
/// read up to that point.
/// </param>
/// <param name="SourcePath">The snapshot file that was imported, or <see langword="null"/> when none was.</param>
/// <param name="Reason">Why no restore happened, or <see langword="null"/> when one did.</param>
internal readonly record struct RepoContextMemoryArchiveRestore(
    RepoContextMemoryRestoreOutcome Outcome,
    long RecordsRead,
    long RecordsInStore,
    string? SourcePath,
    string? Reason)
{
    /// <summary>Whether a snapshot was imported and verified complete.</summary>
    internal bool Restored => Outcome == RepoContextMemoryRestoreOutcome.Restored;
}

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
                RepoContextMemoryRestoreOutcome.NotAttempted,
                0,
                0,
                null,
                $"restore is {RepoContextMemoryArchiveRestoreMode.Off}");
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
                RepoContextMemoryRestoreOutcome.NotAttempted,
                0,
                0,
                null,
                "no archived snapshot exists yet");
        }

        var marker = RepoContextMemoryRestoreState.Decode(
            await tree.GetAsync(RepoContextMemoryRestoreState.Key, cancellationToken)
                .ConfigureAwait(false));

        if (options.RestoreMode == RepoContextMemoryArchiveRestoreMode.Auto)
        {
            // The marker overrides the emptiness probe in exactly one direction. A
            // recorded partial means a previous attempt wrote records and never
            // finished, so the non-empty store it left behind is wreckage rather than
            // state, and declining because of it would disarm the recovery with the
            // damage it is recovering from.
            var healing = marker is { Outcome: RepoContextMemoryRestoreOutcome.Partial };
            if (!healing && await HasAnyMemoryAsync(tree, cancellationToken).ConfigureAwait(false))
            {
                var held = await CountMemoryAsync(tree, cancellationToken).ConfigureAwait(false);
                return new RepoContextMemoryArchiveRestore(
                    RepoContextMemoryRestoreOutcome.NothingToRestore,
                    0,
                    held,
                    null,
                    marker is null
                        ? $"the store already holds {held} memory record(s) and carries no restore-state "
                            + "marker, so no restore of this store has ever been recorded as incomplete "
                            + "(restore mode auto heals an empty store or a recorded partial one)"
                        : $"the store already holds {held} memory record(s) from a restore recorded as "
                            + $"{marker.Value.Outcome}, so there is nothing to heal");
            }
        }

        string? lastFailure = null;
        foreach (var candidate in candidates)
        {
            if (!File.Exists(candidate))
            {
                continue;
            }

            // Write-ahead: record the intent to mutate the tree BEFORE the first
            // record lands. An import that never returns cannot stamp itself, so the
            // only marker that can describe it is one written in advance.
            await StampAsync(
                    tree,
                    new RepoContextMemoryRestoreState(
                        RepoContextMemoryRestoreOutcome.Partial,
                        0,
                        DateTimeOffset.UtcNow.UtcTicks,
                        Path.GetFileName(candidate)),
                    cancellationToken)
                .ConfigureAwait(false);

            try
            {
                long recordsRead;
                await using (var source = new FileStream(
                    candidate, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    var result = await RepoContextPortability.ImportAsync(
                            tree, source, serializer, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                    recordsRead = result.RecordsRead;
                }

                // Verify before stamping complete, and stamp from here rather than
                // from the import: a completion mark applied by the same code path
                // that writes the records inherits that path's failure modes, and a
                // partial restore could then present as a complete one.
                var held = await CountMemoryAsync(tree, cancellationToken).ConfigureAwait(false);
                if (held < recordsRead)
                {
                    lastFailure =
                        $"{Path.GetFileName(candidate)} imported {recordsRead} record(s) but the tree "
                        + $"holds {held}, so the restore did not land completely";
                    continue;
                }

                await StampAsync(
                        tree,
                        new RepoContextMemoryRestoreState(
                            RepoContextMemoryRestoreOutcome.Restored,
                            held,
                            DateTimeOffset.UtcNow.UtcTicks,
                            Path.GetFileName(candidate)),
                        cancellationToken)
                    .ConfigureAwait(false);

                return new RepoContextMemoryArchiveRestore(
                    RepoContextMemoryRestoreOutcome.Restored, recordsRead, held, candidate, Reason: null);
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

        // Every candidate failed. Report what actually landed rather than zero: an
        // import that threw mid-stream has already written everything it read, and a
        // result claiming nothing was read beside a store holding records is the
        // specific reading that made this state unrecoverable by inspection.
        var landed = await CountMemoryAsync(tree, cancellationToken).ConfigureAwait(false);
        return new RepoContextMemoryArchiveRestore(
            landed > 0 ? RepoContextMemoryRestoreOutcome.Partial : RepoContextMemoryRestoreOutcome.Failed,
            0,
            landed,
            null,
            landed > 0
                ? $"{lastFailure}; the tree now holds {landed} partially imported record(s) and is "
                    + "marked for healing on the next restore"
                : lastFailure);
    }

    /// <summary>
    /// Writes the restore-state marker. Failures are swallowed deliberately: the
    /// marker is bookkeeping, and a store that cannot record it is no worse off than
    /// one that never had it, whereas failing the restore over it would turn a
    /// recoverable tree into an unrestored one.
    /// </summary>
    /// <param name="tree">The memory tree to stamp.</param>
    /// <param name="state">The state to record.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    private static async Task StampAsync(
        ILattice tree, RepoContextMemoryRestoreState state, CancellationToken cancellationToken)
    {
        try
        {
            await tree.SetAsync(RepoContextMemoryRestoreState.Key, state.Encode(), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            // Intentionally ignored. See the summary.
        }
    }

    /// <summary>
    /// Counts the memory records the tree holds. The emptiness probe answers "any",
    /// which is what cannot distinguish a healthy tree from a partially imported one;
    /// this answers "how many", which can.
    /// </summary>
    /// <param name="tree">The memory tree to count. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the count.</param>
    /// <returns>The number of records under the repository prefix.</returns>
    internal static async Task<long> CountMemoryAsync(
        ILattice tree, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);

        long count = 0;
        string? continuation = null;
        do
        {
            var page = await RepoContextPortability.EnumerateAsync(
                    tree,
                    RepoContextKeys.AllReposPrefix(),
                    continuation,
                    pageSize: 500,
                    vectorExport: null,
                    cancellationToken)
                .ConfigureAwait(false);

            count += page.Records.Count;
            continuation = page.ContinuationToken;
        }
        while (!string.IsNullOrEmpty(continuation));

        return count;
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
