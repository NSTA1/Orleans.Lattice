using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The read-only adapter behind the three structural-graph tools -
/// <c>repocontext_outline</c>, <c>repocontext_changed</c>, and
/// <c>repocontext_related</c>. Each is a pure projection over records the reconcilers
/// already maintain: the structural file nodes and their declared-symbol sets, the
/// symbol records (their signatures, spans, and outbound <see cref="SymbolRecord.References"/>),
/// the reverse <see cref="CrossReferenceNode"/> cross-reference projection, the
/// per-file content projection, and the indexed per-file token count. It adds no
/// storage primitive of its own and never mutates the store.
/// <para>
/// <b>Boundary.</b> <see cref="OutlineAsync"/> and <see cref="RelatedAsync"/> read only
/// stored records addressed by a repository-relative path, so they never touch disk.
/// Only <see cref="ChangedAsync"/> reads the workspace, and it does so exclusively
/// through the <see cref="RepoContextWorkspaceGuard"/> boundary, so a caller-supplied
/// path can never escape the mounted workspace.
/// </para>
/// </summary>
internal sealed class RepoContextGraphService
{
    private readonly IGrainFactory _grainFactory;
    private readonly Orleans.Serialization.Serializer _serializer;
    private readonly IRepoContextTokenCounter _tokenCounter;
    private readonly RepoContextWorkspaceGuard _workspaceGuard;

    /// <summary>Creates the graph service.</summary>
    /// <param name="grainFactory">The grain factory used to reach the context trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode stored records. Must not be <see langword="null"/>.</param>
    /// <param name="tokenCounter">The shared token counter used for an outline's full-read cost fallback. Must not be <see langword="null"/>.</param>
    /// <param name="workspaceGuard">The workspace boundary the <c>changed</c> walk resolves paths through. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextGraphService(
        IGrainFactory grainFactory,
        Orleans.Serialization.Serializer serializer,
        IRepoContextTokenCounter tokenCounter,
        RepoContextWorkspaceGuard workspaceGuard)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(tokenCounter);
        ArgumentNullException.ThrowIfNull(workspaceGuard);

        _grainFactory = grainFactory;
        _serializer = serializer;
        _tokenCounter = tokenCounter;
        _workspaceGuard = workspaceGuard;
    }

    /// <summary>
    /// Builds the skeleton of one file: its declared symbols with kind, signature, and
    /// line span, plus the token cost of reading the whole file. A file with no stored
    /// node reports <see cref="RepoContextOutlineResult.Exists"/> = <see langword="false"/>.
    /// </summary>
    /// <param name="repoId">The repository the file belongs to. Must not be <see langword="null"/>.</param>
    /// <param name="path">The repository-relative file path. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The outline result.</returns>
    public async Task<RepoContextOutlineResult> OutlineAsync(
        string repoId, string path, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(path);

        var structural = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var nodeBytes = await structural.GetAsync(RepoContextKeys.File(repoId, path), cancellationToken)
            .ConfigureAwait(false);
        if (nodeBytes is null)
        {
            return new RepoContextOutlineResult
            {
                RepoId = repoId,
                Path = path,
                Exists = false,
                FullReadTokenCount = null,
                Symbols = [],
            };
        }

        var node = _serializer.Deserialize<FileNode>(nodeBytes);
        var tokenCount = await FullReadTokenCountAsync(repoId, path, node, cancellationToken).ConfigureAwait(false);

        var declared = DeclaredSymbolNames.Decode(RepoContextValues.ReadString(node.DeclaredSymbols));
        var symbolTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var symbols = new List<RepoContextOutlineSymbol>(declared.Count);
        foreach (var fqName in declared)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var recordBytes = await symbolTree.GetAsync(RepoContextKeys.Symbol(repoId, fqName), cancellationToken)
                .ConfigureAwait(false);
            if (recordBytes is null)
            {
                continue;
            }

            var record = _serializer.Deserialize<SymbolRecord>(recordBytes);
            symbols.Add(new RepoContextOutlineSymbol
            {
                FullyQualifiedName = fqName,
                Kind = record.Kind.ToString(),
                Signature = RepoContextValues.ReadString(record.Signature) ?? string.Empty,
                StartLine = RepoContextValues.ReadInt64(record.StartLine) ?? 0,
                EndLine = RepoContextValues.ReadInt64(record.EndLine) ?? 0,
            });
        }

        symbols.Sort(static (left, right) =>
        {
            var byLine = left.StartLine.CompareTo(right.StartLine);
            return byLine != 0
                ? byLine
                : string.CompareOrdinal(left.FullyQualifiedName, right.FullyQualifiedName);
        });

        return new RepoContextOutlineResult
        {
            RepoId = repoId,
            Path = path,
            Exists = true,
            FullReadTokenCount = tokenCount,
            Symbols = symbols,
        };
    }

    /// <summary>
    /// Computes the drift between the stored index and the current workspace by content
    /// digest, without git, and the impacted dependents of the changed files.
    /// <para>
    /// The walk is rooted at the repository's <b>indexed</b> root and uses the filters the
    /// repository was ingested with, both read from the durable index request. That is what
    /// keeps the comparison meaningful: the stored records are addressed by paths relative to
    /// the indexed root, so walking a different root (or with different filters) would compare
    /// two different path spaces and report every scanned file as added and every stored file
    /// as removed. <paramref name="workspacePath"/> is therefore a <i>scope</i>, not the walk
    /// root - supply the repository root to compare the whole tree, or a directory inside it to
    /// restrict the report to that subtree.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository whose index is compared. Must not be <see langword="null"/>.</param>
    /// <param name="workspacePath">The repository root, or a directory inside it to scope the
    /// report to. Resolved through the workspace guard. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the walk and reads.</param>
    /// <returns>The added, updated, removed, and dependent file lists.</returns>
    /// <exception cref="RepoContextWorkspaceViolationException">The path resolves outside the workspace.</exception>
    /// <exception cref="ArgumentException">The path is null, empty, or whitespace, or resolves outside the indexed repository root.</exception>
    /// <exception cref="DirectoryNotFoundException">The resolved path is not an existing directory.</exception>
    public async Task<RepoContextChangedResult> ChangedAsync(
        string repoId, string workspacePath, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(workspacePath);

        var requestedPath = _workspaceGuard.Resolve(workspacePath);

        // The durable index request is the authority for how this repository was walked.
        // Without it the caller's path is all we have, so the legacy behaviour (walk the
        // supplied path as the root, with default filters) is kept for a repository that
        // was never indexed through the job grain.
        var indexRequest = await _grainFactory
            .GetGrain<IRepoIndexJobGrain>(repoId).GetRequestAsync().ConfigureAwait(false);

        var walkRoot = requestedPath;
        string? scopePrefix = null;
        IReadOnlyList<string>? includeGlobs = null;
        IReadOnlyList<string>? excludeGlobs = null;
        var respectGitignore = true;
        var excludeBinary = true;

        if (indexRequest is not null && !string.IsNullOrWhiteSpace(indexRequest.RepoRoot))
        {
            // Re-resolve the persisted root through the guard rather than trusting it: the
            // mount may have changed since the repository was indexed, and the walk must stay
            // inside the workspace boundary regardless of what was persisted.
            walkRoot = _workspaceGuard.Resolve(indexRequest.RepoRoot);
            scopePrefix = ResolveScopePrefix(walkRoot, requestedPath);
            includeGlobs = indexRequest.IncludeGlobs;
            excludeGlobs = indexRequest.ExcludeGlobs;
            respectGitignore = indexRequest.RespectGitignore;
            excludeBinary = indexRequest.ExcludeBinary;
        }

        var stored = await ReadStoredFilesAsync(repoId, cancellationToken).ConfigureAwait(false);

        // Hand the walk the facts already stored per file so an unchanged file is settled by
        // a stat instead of a full read-and-hash - the same fast path the periodic reconcile
        // uses. Without it every file in the repository is re-hashed on every call, which is
        // what makes a whole-repository drift report too slow to answer on a large tree.
        var knownFiles = new Dictionary<string, StoredFileMeta>(stored.Count, StringComparer.Ordinal);
        foreach (var (storedPath, entry) in stored)
        {
            knownFiles[storedPath] = entry.ToStoredFileMeta();
        }

        var scanned = RepoTreeWalker.Walk(
            walkRoot,
            includeGlobs,
            excludeGlobs,
            respectGitignore,
            excludeBinary,
            onProgress: null,
            cancellationToken,
            knownFiles);

        var storedDigests = new Dictionary<string, string>(stored.Count, StringComparer.Ordinal);
        foreach (var (storedPath, entry) in stored)
        {
            if (IsInScope(storedPath, scopePrefix))
            {
                storedDigests[storedPath] = entry.Digest;
            }
        }

        var scopedScan = scopePrefix is null
            ? scanned
            : scanned.Where(entry => IsInScope(entry.RelativePath, scopePrefix)).ToList();

        var plan = RepoContextBootstrapPlan.Compute(storedDigests, scopedScan);
        var added = ToOrderedList(plan.Added.Select(static entry => entry.RelativePath));
        var updated = ToOrderedList(plan.Updated.Select(static entry => entry.RelativePath));
        var removed = ToOrderedList(plan.RemovedPaths);

        var changedPaths = new HashSet<string>(StringComparer.Ordinal);
        changedPaths.UnionWith(added);
        changedPaths.UnionWith(updated);
        changedPaths.UnionWith(removed);

        var dependents = await ResolveDependentsAsync(
            repoId, plan.Updated, plan.RemovedPaths, stored, changedPaths, cancellationToken)
            .ConfigureAwait(false);

        return new RepoContextChangedResult
        {
            RepoId = repoId,
            Added = added,
            Updated = updated,
            Removed = removed,
            Dependents = dependents,
        };
    }

    /// <summary>
    /// Resolves the requested path to a repository-relative POSIX directory prefix, or
    /// <see langword="null"/> when it is the repository root itself (an unscoped report).
    /// A path outside the indexed root is refused outright rather than silently compared
    /// against a path space it does not belong to.
    /// </summary>
    private static string? ResolveScopePrefix(string repoRoot, string requestedPath)
    {
        var comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;

        var root = requestedPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var indexedRoot = repoRoot.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        if (root.Equals(indexedRoot, comparison))
        {
            return null;
        }

        if (!root.StartsWith(indexedRoot + Path.DirectorySeparatorChar, comparison))
        {
            throw new ArgumentException(
                $"The path '{requestedPath}' is outside the indexed root of repository '{repoRoot}'. "
                + "Supply the repository root, or a directory inside it, so the report compares the same path space.",
                nameof(requestedPath));
        }

        var relative = root[(indexedRoot.Length + 1)..]
            .Replace(Path.DirectorySeparatorChar, '/')
            .Replace('\\', '/')
            .Trim('/');

        return relative.Length == 0 ? null : relative + "/";
    }

    /// <summary>
    /// Reports whether a repository-relative path falls inside the requested scope. An
    /// unscoped report (a null prefix) admits every path.
    /// </summary>
    private static bool IsInScope(string relativePath, string? scopePrefix)
        => scopePrefix is null || relativePath.StartsWith(scopePrefix, StringComparison.Ordinal);

    /// <summary>
    /// Resolves the structural neighbourhood of one file: its outbound referenced
    /// type-names, the indexed symbols that reference its declarations (inbound
    /// dependents), and the test types that cover them. A file with no stored node
    /// reports <see cref="RepoContextRelatedResult.Exists"/> = <see langword="false"/>.
    /// </summary>
    /// <param name="repoId">The repository the file belongs to. Must not be <see langword="null"/>.</param>
    /// <param name="path">The repository-relative file path. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The related-neighbourhood result.</returns>
    public async Task<RepoContextRelatedResult> RelatedAsync(
        string repoId, string path, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(path);

        var structural = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var nodeBytes = await structural.GetAsync(RepoContextKeys.File(repoId, path), cancellationToken)
            .ConfigureAwait(false);
        if (nodeBytes is null)
        {
            return new RepoContextRelatedResult
            {
                RepoId = repoId,
                Path = path,
                Exists = false,
                Imports = [],
                Dependents = [],
                Tests = [],
            };
        }

        var node = _serializer.Deserialize<FileNode>(nodeBytes);
        var declared = DeclaredSymbolNames.Decode(RepoContextValues.ReadString(node.DeclaredSymbols));
        var ownFqNames = new HashSet<string>(declared, StringComparer.Ordinal);

        var symbolTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var crossReferenceTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.CrossReference);

        var imports = new SortedSet<string>(StringComparer.Ordinal);
        var simpleNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var fqName in declared)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var recordBytes = await symbolTree.GetAsync(RepoContextKeys.Symbol(repoId, fqName), cancellationToken)
                .ConfigureAwait(false);
            if (recordBytes is not null)
            {
                var record = _serializer.Deserialize<SymbolRecord>(recordBytes);
                foreach (var reference in record.References.Elements())
                {
                    imports.Add(Encoding.UTF8.GetString(reference));
                }
            }

            simpleNames.Add(SymbolNaming.SimpleName(fqName));
        }

        // A per-referrer declaring-file cache: a symbol referenced from several places
        // in this file resolves its file exactly once.
        var fileBySymbol = new Dictionary<string, string?>(StringComparer.Ordinal);
        var dependents = new List<RepoContextRelatedEdge>();
        var tests = new List<RepoContextRelatedEdge>();
        var seenDependents = new HashSet<string>(StringComparer.Ordinal);
        var seenTests = new HashSet<string>(StringComparer.Ordinal);

        foreach (var name in simpleNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var xrefBytes = await crossReferenceTree
                .GetAsync(RepoContextKeys.CrossReference(repoId, name), cancellationToken)
                .ConfigureAwait(false);
            if (xrefBytes is null)
            {
                continue;
            }

            var xref = _serializer.Deserialize<CrossReferenceNode>(xrefBytes);

            foreach (var referrer in xref.Referrers.Elements())
            {
                var referrerFq = Encoding.UTF8.GetString(referrer);
                if (ownFqNames.Contains(referrerFq) || !seenDependents.Add(referrerFq))
                {
                    continue;
                }

                var file = await ResolveDeclaringFileAsync(
                    repoId, referrerFq, symbolTree, fileBySymbol, cancellationToken).ConfigureAwait(false);
                if (string.Equals(file, path, StringComparison.Ordinal))
                {
                    // A symbol declared in this same file (a distinct partial or nested
                    // declaration) is not an inbound dependency of the file on itself.
                    continue;
                }

                dependents.Add(new RepoContextRelatedEdge { Symbol = referrerFq, Path = file });
            }

            foreach (var test in xref.Tests.Elements())
            {
                var testFq = Encoding.UTF8.GetString(test);
                if (!seenTests.Add(testFq))
                {
                    continue;
                }

                var file = await ResolveDeclaringFileAsync(
                    repoId, testFq, symbolTree, fileBySymbol, cancellationToken).ConfigureAwait(false);
                tests.Add(new RepoContextRelatedEdge { Symbol = testFq, Path = file });
            }
        }

        dependents.Sort(CompareEdges);
        tests.Sort(CompareEdges);

        return new RepoContextRelatedResult
        {
            RepoId = repoId,
            Path = path,
            Exists = true,
            Imports = [.. imports],
            Dependents = dependents,
            Tests = tests,
        };
    }

    private async Task<int?> FullReadTokenCountAsync(
        string repoId, string path, FileNode node, CancellationToken cancellationToken)
    {
        // Prefer the indexed per-file token count, which is computed from the full body
        // where it was already in hand. Fall back to counting the stored content
        // projection (bounded at ContentRecord.MaxContentChars), and report null when
        // the file was never content-processed so a caller can tell "unknown" from zero.
        var indexed = RepoContextValues.ReadInt64(node.TokenCount);
        if (indexed is { } value)
        {
            return (int)value;
        }

        var contentTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Content);
        var contentBytes = await contentTree.GetAsync(RepoContextKeys.Content(repoId, path), cancellationToken)
            .ConfigureAwait(false);
        if (contentBytes is null)
        {
            return null;
        }

        var text = RepoContextValues.ReadString(_serializer.Deserialize<ContentRecord>(contentBytes).Text);
        return text is null ? null : _tokenCounter.CountTokens(text);
    }

    private async Task<IReadOnlyList<string>> ResolveDependentsAsync(
        string repoId,
        IReadOnlyList<RepoFileEntry> updated,
        IReadOnlyList<string> removedPaths,
        IReadOnlyDictionary<string, StoredFileEntry> stored,
        HashSet<string> changedPaths,
        CancellationToken cancellationToken)
    {
        // The simple type-names declared by every changed file, resolved against the
        // reverse cross-reference projection to the files that reference them.
        var simpleNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in updated)
        {
            CollectSimpleNames(stored, entry.RelativePath, simpleNames);
        }

        foreach (var removedPath in removedPaths)
        {
            CollectSimpleNames(stored, removedPath, simpleNames);
        }

        if (simpleNames.Count == 0)
        {
            return [];
        }

        var crossReferenceTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.CrossReference);
        var symbolTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var fileBySymbol = new Dictionary<string, string?>(StringComparer.Ordinal);
        var dependents = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var name in simpleNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var xrefBytes = await crossReferenceTree
                .GetAsync(RepoContextKeys.CrossReference(repoId, name), cancellationToken)
                .ConfigureAwait(false);
            if (xrefBytes is null)
            {
                continue;
            }

            var xref = _serializer.Deserialize<CrossReferenceNode>(xrefBytes);
            foreach (var referrer in xref.Referrers.Elements())
            {
                var referrerFq = Encoding.UTF8.GetString(referrer);
                var file = await ResolveDeclaringFileAsync(
                    repoId, referrerFq, symbolTree, fileBySymbol, cancellationToken).ConfigureAwait(false);

                // A dependent is a file that is not itself part of the change set.
                if (file is not null && !changedPaths.Contains(file))
                {
                    dependents.Add(file);
                }
            }
        }

        return [.. dependents];
    }

    private static void CollectSimpleNames(
        IReadOnlyDictionary<string, StoredFileEntry> stored, string path, HashSet<string> into)
    {
        if (!stored.TryGetValue(path, out var entry))
        {
            return;
        }

        foreach (var fqName in entry.DeclaredSymbols)
        {
            into.Add(SymbolNaming.SimpleName(fqName));
        }
    }

    private async Task<string?> ResolveDeclaringFileAsync(
        string repoId,
        string fqName,
        ILattice symbolTree,
        Dictionary<string, string?> cache,
        CancellationToken cancellationToken)
    {
        if (cache.TryGetValue(fqName, out var cached))
        {
            return cached;
        }

        string? file = null;
        var recordBytes = await symbolTree.GetAsync(RepoContextKeys.Symbol(repoId, fqName), cancellationToken)
            .ConfigureAwait(false);
        if (recordBytes is not null)
        {
            var record = _serializer.Deserialize<SymbolRecord>(recordBytes);
            file = RepoContextValues.ReadString(record.FilePath);
            if (file is null)
            {
                foreach (var declaring in record.DeclaringFiles.Elements())
                {
                    file = Encoding.UTF8.GetString(declaring);
                    break;
                }
            }
        }

        cache[fqName] = file;
        return file;
    }

    private async Task<Dictionary<string, StoredFileEntry>> ReadStoredFilesAsync(
        string repoId, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var prefix = RepoContextKeys.FilesPrefix(repoId);
        var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
        var stored = new Dictionary<string, StoredFileEntry>(StringComparer.Ordinal);

        await foreach (var entry in tree
            .ScanEntriesAsync(prefix, endExclusive, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!RepoContextKeys.TryParse(entry.Key, out var parsed)
                || parsed.Kind != RepoContextRecordKind.File
                || parsed.Path is not { } path)
            {
                continue;
            }

            var node = _serializer.Deserialize<FileNode>(entry.Value);
            var digest = RepoContextValues.ReadString(node.Digest);
            if (digest is not null)
            {
                stored[path] = new StoredFileEntry(
                    digest,
                    DeclaredSymbolNames.Decode(RepoContextValues.ReadString(node.DeclaredSymbols)),
                    RepoContextValues.ReadString(node.Language) ?? string.Empty,
                    RepoContextValues.ReadInt64(node.SizeBytes) ?? -1,
                    RepoContextValues.ReadHlcWallTicks(node.Digest) ?? 0);
            }
        }

        return stored;
    }

    private static List<string> ToOrderedList(IEnumerable<string> values)
    {
        var ordered = new SortedSet<string>(values, StringComparer.Ordinal);
        return [.. ordered];
    }

    private static int CompareEdges(RepoContextRelatedEdge left, RepoContextRelatedEdge right)
    {
        var byPath = string.CompareOrdinal(left.Path ?? string.Empty, right.Path ?? string.Empty);
        return byPath != 0 ? byPath : string.CompareOrdinal(left.Symbol, right.Symbol);
    }

    /// <summary>
    /// The reconcile-relevant facts read for a stored file: its content digest, the
    /// fully-qualified names of the symbols it declares, and the three quantities the
    /// walk's stat fast-path needs to settle the file as unchanged without reading it
    /// (its stored language, size, and ingest anchor). Kept minimal so the
    /// <c>changed</c> scan reads only what the drift and dependent computation need.
    /// </summary>
    private readonly record struct StoredFileEntry(
        string Digest,
        IReadOnlyList<string> DeclaredSymbols,
        string Language,
        long SizeBytes,
        long IngestWallTicks)
    {
        /// <summary>
        /// Projects the entry onto the walker's fast-path shape. Only the digest, language,
        /// size, and ingest anchor participate in the stat comparison; the back-fill markers
        /// are irrelevant to a read-only drift report and are left at their defaults, which
        /// the walker never consults on this path.
        /// </summary>
        public StoredFileMeta ToStoredFileMeta() => new(
            Digest,
            Language,
            SizeBytes,
            IngestWallTicks,
            DeclaredSymbols,
            SymbolsProcessed: true,
            ContentProcessed: true,
            TokenCount: -1,
            CrossReferenced: true);
    }
}
