namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The pure, deterministic diff at the heart of an idempotent, resumable
/// bootstrap: given the digests already stored for a repository and the files a
/// fresh scan produced, it partitions the scan into files that are new, files
/// whose content changed, files that are byte-for-byte unchanged, and files whose
/// content is unchanged but whose ingest anchor must be refreshed, and lists the
/// stored paths that no longer exist and must be pruned.
/// <para>
/// Because the partition is driven entirely by content digests, re-running on an
/// unchanged tree yields an empty <see cref="Added"/>, <see cref="Updated"/>, and
/// <see cref="RemovedPaths"/> (a no-op), a changed tree yields only the changed
/// files, and a crashed run that already persisted some files sees those as
/// <see cref="Unchanged"/> on the next attempt - so the write path never
/// duplicates work.
/// </para>
/// </summary>
internal sealed class RepoContextBootstrapPlan
{
    private RepoContextBootstrapPlan(
        IReadOnlyList<RepoFileEntry> added,
        IReadOnlyList<RepoFileEntry> updated,
        IReadOnlyList<RepoFileEntry> unchanged,
        IReadOnlyList<RepoFileEntry> metadataChanged,
        IReadOnlyList<string> removedPaths)
    {
        Added = added;
        Updated = updated;
        Unchanged = unchanged;
        MetadataChanged = metadataChanged;
        RemovedPaths = removedPaths;
    }

    /// <summary>Scanned files with no stored digest (first-time ingestion).</summary>
    public IReadOnlyList<RepoFileEntry> Added { get; }

    /// <summary>Scanned files whose digest differs from the stored digest.</summary>
    public IReadOnlyList<RepoFileEntry> Updated { get; }

    /// <summary>Scanned files whose digest matches the stored digest (skipped).</summary>
    public IReadOnlyList<RepoFileEntry> Unchanged { get; }

    /// <summary>
    /// Scanned files whose content is byte-for-byte the stored content but whose
    /// stored ingest anchor is stale (they had to be re-hashed because their on-disk
    /// stat looked changed). Their nodes are rewritten to refresh the anchor - so the
    /// stat fast-path skips them next time - but they are not re-embedded, since the
    /// content did not change. They are content-unchanged, so they are reported to
    /// callers as part of the unchanged tally.
    /// </summary>
    public IReadOnlyList<RepoFileEntry> MetadataChanged { get; }

    /// <summary>Stored file paths that the scan no longer contains (to be pruned).</summary>
    public IReadOnlyList<string> RemovedPaths { get; }

    /// <summary>Whether the plan changes nothing: no adds, updates, anchor refreshes, or removals.</summary>
    public bool IsNoOp =>
        Added.Count == 0 && Updated.Count == 0 && MetadataChanged.Count == 0 && RemovedPaths.Count == 0;

    /// <summary>
    /// The number of files present in the repository after this plan is applied:
    /// every scanned file, whether added, updated, unchanged, or metadata-refreshed
    /// (pruned paths are gone). Recorded on the repository root marker so
    /// <c>list_repos</c> can report a per-repository file count without a subtree
    /// scan.
    /// </summary>
    public int LiveFileCount => Added.Count + Updated.Count + Unchanged.Count + MetadataChanged.Count;

    /// <summary>
    /// Computes the plan from the currently stored digests and a fresh scan.
    /// </summary>
    /// <param name="storedDigests">Map of repository-relative path to the digest
    /// currently stored for that file. Must not be <see langword="null"/>.</param>
    /// <param name="scanned">The files produced by the current scan. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The computed bootstrap plan.</returns>
    /// <exception cref="ArgumentNullException">Either argument is null.</exception>
    public static RepoContextBootstrapPlan Compute(
        IReadOnlyDictionary<string, string> storedDigests,
        IReadOnlyList<RepoFileEntry> scanned)
    {
        ArgumentNullException.ThrowIfNull(storedDigests);
        ArgumentNullException.ThrowIfNull(scanned);

        var added = new List<RepoFileEntry>();
        var updated = new List<RepoFileEntry>();
        var unchanged = new List<RepoFileEntry>();
        var metadataChanged = new List<RepoFileEntry>();
        var scannedPaths = new HashSet<string>(scanned.Count, StringComparer.Ordinal);

        foreach (var entry in scanned)
        {
            scannedPaths.Add(entry.RelativePath);
            if (!storedDigests.TryGetValue(entry.RelativePath, out var storedDigest))
            {
                added.Add(entry);
            }
            else if (!string.Equals(storedDigest, entry.Digest, StringComparison.Ordinal))
            {
                updated.Add(entry);
            }
            else if (entry.AnchorStale)
            {
                // Content identical to the stored digest, but the file had to be
                // re-hashed because its stat looked stale: rewrite the node to
                // refresh the ingest anchor without re-embedding.
                metadataChanged.Add(entry);
            }
            else
            {
                unchanged.Add(entry);
            }
        }

        var removed = new List<string>();
        foreach (var storedPath in storedDigests.Keys)
        {
            if (!scannedPaths.Contains(storedPath))
            {
                removed.Add(storedPath);
            }
        }

        removed.Sort(StringComparer.Ordinal);
        return new RepoContextBootstrapPlan(added, updated, unchanged, metadataChanged, removed);
    }
}
