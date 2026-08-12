using System.IO;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Walks a repository working tree and yields one <see cref="RepoFileEntry"/> per
/// included file, with a content digest computed for each. The walk is pure over
/// the filesystem: given the same tree and filters it produces the same ordered
/// result, so it composes with the idempotent diff in
/// <see cref="RepoContextBootstrapPlan"/>.
/// <para>
/// <b>Filtering.</b> The version-control metadata directory <c>.git</c> is always
/// skipped. When <c>respectGitignore</c> is set, the tree's <c>.gitignore</c>
/// files are honoured hierarchically (see <see cref="GitignoreScope"/>): an ignored
/// directory is pruned before it is descended and an ignored file is dropped. When
/// <c>excludeBinary</c> is set, a file whose leading bytes look non-text (a NUL byte
/// is present) is dropped before it is hashed. When any include globs are supplied a
/// file is kept only if it matches at least one of them; when none are supplied every
/// surviving file is a candidate. Exclude globs are applied last and always win. All
/// matching is done on the repository-relative, <c>'/'</c>-separated path (see
/// <see cref="GlobMatcher"/>).
/// </para>
/// <para>
/// <b>Symlink safety.</b> The walk descends real directories only: any reparse
/// point (a symbolic link or junction, file or directory) is skipped rather than
/// followed. This keeps ingestion inside the workspace boundary the
/// <see cref="RepoContextWorkspaceGuard"/> established for the root and makes the
/// walk immune to cycles a self-referential link would otherwise create.
/// </para>
/// <para>
/// <b>Parallelism.</b> The walk runs in two phases: a serial, symlink-safe
/// directory traversal discovers the included files (a cheap, stat-only pass),
/// then the read-and-hash of those files - the dominant cost - is fanned out
/// across cores into a pre-sized result array (one write per slot, so no lock or
/// concurrent collection). The final ordinal sort makes the output identical
/// regardless of completion order, so determinism is preserved. Peak memory is
/// bounded to roughly the degree of parallelism times the largest file, not the
/// whole tree, because each file's bytes are released as soon as it is hashed.
/// </para>
/// </summary>
internal static class RepoTreeWalker
{
    private const string GitDirectorySegment = ".git";
    private const string GitignoreFileName = ".gitignore";

    // The leading window scanned for a NUL byte to classify a file as binary. This
    // matches the size Git samples (its FIRST_FEW_BYTES) for the same decision.
    private const int BinarySniffByteCount = 8000;

    /// <summary>
    /// Walks <paramref name="rootPath"/> and returns the included files in
    /// ascending ordinal path order.
    /// </summary>
    /// <param name="rootPath">The absolute path to the repository root. Must not
    /// be <see langword="null"/> and must be an existing directory.</param>
    /// <param name="includeGlobs">Optional include patterns; when non-empty a file
    /// must match at least one to be kept.</param>
    /// <param name="excludeGlobs">Optional exclude patterns; a match removes the
    /// file even when it also matched an include.</param>
    /// <param name="respectGitignore">When <see langword="true"/>, the
    /// <c>.gitignore</c> files found in the tree are honoured hierarchically: an
    /// ignored directory is pruned (never descended, so its whole subtree is
    /// excluded cheaply) and an ignored file is dropped. Include and exclude globs
    /// still layer on top of the result.</param>
    /// <param name="excludeBinary">When <see langword="true"/>, a file whose leading
    /// bytes look non-text (a NUL byte is present) is dropped before it is hashed or
    /// returned, so compiled artefacts, images, and other blobs never enter the
    /// index. Text files are unaffected.</param>
    /// <param name="onProgress">An optional callback invoked with the running count
    /// of processed files (hashed or skipped via the stat fast-path) as the parallel
    /// phase progresses, so a caller can report live walk progress. It is called from
    /// pool threads and must be cheap and thread-safe; the walk emits one final call
    /// with the exact total before returning. May be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the walk between files.</param>
    /// <param name="knownFiles">Optional map of repository-relative path to the facts
    /// already stored for that file. When supplied, a candidate whose size is
    /// unchanged and whose on-disk modification time is strictly older than its
    /// stored ingest anchor is assumed unchanged and its stored digest and language
    /// are reused without a read - the stat fast-path that makes a periodic reconcile
    /// cheap. When <see langword="null"/> every file is read and hashed (the cold
    /// behaviour), so existing callers and tests are unaffected.</param>
    /// <returns>The included files, ordered by repository-relative path.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="rootPath"/> is null.</exception>
    /// <exception cref="DirectoryNotFoundException"><paramref name="rootPath"/> does not exist.</exception>
    internal static IReadOnlyList<RepoFileEntry> Walk(
        string rootPath,
        IReadOnlyList<string>? includeGlobs,
        IReadOnlyList<string>? excludeGlobs,
        bool respectGitignore = false,
        bool excludeBinary = false,
        Action<int>? onProgress = null,
        CancellationToken cancellationToken = default,
        IReadOnlyDictionary<string, StoredFileMeta>? knownFiles = null)
    {
        ArgumentNullException.ThrowIfNull(rootPath);
        if (!Directory.Exists(rootPath))
        {
            throw new DirectoryNotFoundException(
                $"The repository root '{rootPath}' does not exist or is not a directory.");
        }

        var includes = Compile(includeGlobs);
        var excludes = Compile(excludeGlobs);

        var root = Path.GetFullPath(rootPath);

        // Phase 1: serial, symlink-safe discovery of the included files. This is a
        // cheap stat-only pass; the expensive read-and-hash is deferred to phase 2.
        // Each candidate carries the size and modification time the enumeration
        // already populated (at zero extra stat cost) so phase 2 can apply the
        // fast-path without a second stat.
        var candidates = DiscoverIncludedFiles(root, includes, excludes, respectGitignore, cancellationToken);
        if (candidates.Count == 0)
        {
            return Array.Empty<RepoFileEntry>();
        }

        // Phase 2: fan the read-and-hash out across cores into a pre-sized array.
        // Each index is written at most once, so no synchronisation is needed. A
        // slot is left null when the file is dropped as binary (see excludeBinary),
        // so the array is compacted before the deterministic ordinal sort.
        var results = new RepoFileEntry?[candidates.Count];
        var processedCount = 0;
        var options = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount),
        };

        try
        {
            Parallel.For(0, candidates.Count, options, index =>
            {
                var candidate = candidates[index];
                var relativePath = candidate.Relative;

                StoredFileMeta meta = default;
                var known = knownFiles is not null
                    && knownFiles.TryGetValue(relativePath, out meta);

                // Stat fast-path: a stored file whose size is unchanged and whose
                // modification time is strictly older than the ingest anchor is
                // assumed unchanged. Reuse its stored digest and language without a
                // read. The strict comparison stays clear of the racy-clean window
                // (a file touched in the same tick as its last ingest is re-hashed),
                // and the content digest remains the sole source of truth: a false
                // "unchanged" is impossible here because a real edit either grows or
                // shrinks the file or leaves a modification time at or after the
                // anchor, both of which fall through to the read below.
                if (known
                    && candidate.Length == meta.SizeBytes
                    && meta.IngestWallTicks > 0
                    && candidate.MtimeTicks < meta.IngestWallTicks)
                {
                    results[index] = new RepoFileEntry(
                        relativePath, meta.Digest, candidate.Length, meta.Language);
                    if (onProgress is not null)
                    {
                        onProgress(Interlocked.Increment(ref processedCount));
                    }

                    return;
                }

                var content = File.ReadAllBytes(candidate.Absolute);

                // Drop a non-text file before it is hashed, embedded, or indexed:
                // a NUL byte in the leading window is the same cheap, language- and
                // extension-agnostic heuristic Git uses to classify a blob as binary.
                if (excludeBinary && IsProbablyBinary(content))
                {
                    return;
                }

                string digest;
                var anchorStale = false;
                if (known && FileDigest.Matches(meta.Digest, content))
                {
                    // The stat looked stale (bumped modification time or a size the
                    // fast-path could not clear) but the content is byte-for-byte the
                    // stored content. Keep the stored digest verbatim - so the plan
                    // sees it as unchanged, not a spurious update - and flag the
                    // anchor stale so the node is rewritten to refresh the ingest
                    // anchor, letting the fast-path skip this file from now on.
                    digest = meta.Digest;
                    anchorStale = true;
                }
                else
                {
                    // A new file, or genuinely changed content: stamp the modern
                    // digest so the store migrates off any older algorithm as content
                    // evolves.
                    digest = FileDigest.Compute(content);
                }

                results[index] = new RepoFileEntry(
                    relativePath, digest, content.LongLength,
                    LanguageClassifier.Classify(relativePath))
                {
                    AnchorStale = anchorStale,
                };

                // Publish the running processed-file count for live progress. The
                // sampled value is approximate under concurrency; the exact total is
                // published once below after the loop drains.
                if (onProgress is not null)
                {
                    onProgress(Interlocked.Increment(ref processedCount));
                }
            });
        }
        catch (AggregateException aggregate) when (aggregate.InnerException is not null)
        {
            // Surface the underlying filesystem error (for example a file removed
            // mid-walk) with its original type and stack, not a wrapper.
            System.Runtime.ExceptionServices.ExceptionDispatchInfo
                .Capture(aggregate.InnerException).Throw();
        }

        var included = new List<RepoFileEntry>(results.Length);
        foreach (var entry in results)
        {
            if (entry.HasValue)
            {
                included.Add(entry.Value);
            }
        }

        // Publish the exact final count once, so a progress consumer settles on the
        // authoritative total rather than a racily-sampled near-final value.
        onProgress?.Invoke(included.Count);

        if (included.Count == 0)
        {
            return Array.Empty<RepoFileEntry>();
        }

        var ordered = included.ToArray();
        Array.Sort(ordered, static (left, right) =>
            string.CompareOrdinal(left.RelativePath, right.RelativePath));
        return ordered;
    }

    /// <summary>
    /// Reports whether <paramref name="content"/> looks like a binary (non-text)
    /// blob: a <c>NUL</c> byte anywhere in the leading window is treated as the
    /// signal, matching the classic heuristic Git applies. This deliberately reads
    /// only a bounded prefix so a large file costs a fixed scan.
    /// </summary>
    private static bool IsProbablyBinary(ReadOnlySpan<byte> content)
    {
        var window = content.Length <= BinarySniffByteCount
            ? content
            : content[..BinarySniffByteCount];
        return window.IndexOf((byte)0) >= 0;
    }

    /// <summary>
    /// Runs the serial, symlink-safe directory traversal and returns the absolute
    /// and repository-relative path of every file that passes the <c>.git</c>,
    /// <c>.gitignore</c> (when honoured), include, and exclude filters, in
    /// discovery order.
    /// </summary>
    private static List<(string Absolute, string Relative, long Length, long MtimeTicks)> DiscoverIncludedFiles(
        string root,
        List<GlobMatcher> includes,
        List<GlobMatcher> excludes,
        bool respectGitignore,
        CancellationToken cancellationToken)
    {
        var included = new List<(string Absolute, string Relative, long Length, long MtimeTicks)>();

        // Explicit depth-first walk over real directories only, so a symlinked or
        // junctioned directory is never descended (escape prevention + cycle
        // safety) and a symlinked file is never read. Each frame carries the
        // .gitignore scope in effect for that directory's children, layered from
        // the root down, so an ignored directory is pruned before it is descended.
        var pending = new Stack<(string Directory, string RelativeDir, GitignoreScope Scope)>();
        var rootScope = respectGitignore
            ? GitignoreScope.Empty.Add(string.Empty, ReadGitignore(root))
            : GitignoreScope.Empty;
        pending.Push((root, string.Empty, rootScope));
        while (pending.Count != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (directory, _, scope) = pending.Pop();

            foreach (var child in new DirectoryInfo(directory).EnumerateFileSystemInfos())
            {
                cancellationToken.ThrowIfCancellationRequested();

                if ((child.Attributes & FileAttributes.ReparsePoint) != 0)
                {
                    // Symbolic link or junction: skip without following.
                    continue;
                }

                if ((child.Attributes & FileAttributes.Directory) != 0)
                {
                    var relativeDir = ToRelativePosixPath(root, child.FullName);
                    if (relativeDir.Equals(GitDirectorySegment, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    // Prune an ignored directory: never descend it, so its whole
                    // subtree is excluded by a single stat rather than a deep walk.
                    if (respectGitignore && scope.IsIgnored(relativeDir, isDirectory: true))
                    {
                        continue;
                    }

                    // Layer this directory's own .gitignore (if any) for its children.
                    var childScope = respectGitignore
                        ? scope.Add(relativeDir, ReadGitignore(child.FullName))
                        : scope;
                    pending.Push((child.FullName, relativeDir, childScope));
                    continue;
                }

                var relativePath = ToRelativePosixPath(root, child.FullName);
                if (IsUnderGitDirectory(relativePath))
                {
                    continue;
                }

                if (respectGitignore && scope.IsIgnored(relativePath, isDirectory: false))
                {
                    continue;
                }

                if (includes.Count != 0 && !MatchesAny(includes, relativePath))
                {
                    continue;
                }

                if (MatchesAny(excludes, relativePath))
                {
                    continue;
                }

                // The enumeration already populated Length and LastWriteTimeUtc for
                // this entry, so reading them here costs no extra stat. Both feed the
                // phase-2 stat fast-path; the modification time is an advisory local
                // read only (never persisted or ordered across clusters).
                var file = (FileInfo)child;
                included.Add((child.FullName, relativePath, file.Length, file.LastWriteTimeUtc.Ticks));
            }
        }

        return included;
    }

    /// <summary>
    /// Reads the <c>.gitignore</c> text at <paramref name="directoryFullPath"/>, or
    /// the empty string when it is absent or cannot be read - a missing or
    /// unreadable ignore file simply contributes no rules.
    /// </summary>
    private static string ReadGitignore(string directoryFullPath)
    {
        var path = Path.Combine(directoryFullPath, GitignoreFileName);
        try
        {
            return File.Exists(path) ? File.ReadAllText(path) : string.Empty;
        }
        catch (IOException)
        {
            return string.Empty;
        }
        catch (UnauthorizedAccessException)
        {
            return string.Empty;
        }
    }

    private static List<GlobMatcher> Compile(IReadOnlyList<string>? globs)
    {
        if (globs is null || globs.Count == 0)
        {
            return [];
        }

        var matchers = new List<GlobMatcher>(globs.Count);
        foreach (var glob in globs)
        {
            if (!string.IsNullOrWhiteSpace(glob))
            {
                matchers.Add(GlobMatcher.Compile(glob));
            }
        }

        return matchers;
    }

    private static bool MatchesAny(List<GlobMatcher> matchers, string relativePath)
    {
        foreach (var matcher in matchers)
        {
            if (matcher.IsMatch(relativePath))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsUnderGitDirectory(string relativePath) =>
        relativePath.Equals(GitDirectorySegment, StringComparison.Ordinal)
        || relativePath.StartsWith(GitDirectorySegment + "/", StringComparison.Ordinal);

    private static string ToRelativePosixPath(string root, string absolutePath)
    {
        var relative = Path.GetRelativePath(root, absolutePath);
        return relative.Replace(Path.DirectorySeparatorChar, '/').Replace('\\', '/');
    }
}
