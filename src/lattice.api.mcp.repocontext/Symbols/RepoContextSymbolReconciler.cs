using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Reconciles the per-symbol structural records in the dedicated symbol tree against
/// a bootstrap plan. For every added or updated file it extracts the declared
/// symbols and upserts their records (last-writer-wins location and shape scalars,
/// plus an add-wins record of the declaring file); for every symbol a changed or
/// removed file no longer declares it drops that file from the symbol's declaring
/// set, pruning the whole record only once no file declares it any more.
/// <para>
/// <b>Ownership is a set, not a scalar.</b> A symbol may be declared by more than one
/// file - C# partial types are the canonical case - so
/// <see cref="SymbolRecord.DeclaringFiles"/> is an observed-remove set and a symbol
/// survives as long as any file still declares it. A read-merge-write per touched
/// record preserves the declaring files contributed by unchanged sibling files that
/// this incremental pass never re-read.
/// </para>
/// <para>
/// <b>Resumable by construction.</b> The reconcile runs before the file nodes are
/// rewritten, so a crash between the symbol write and the file-node write leaves the
/// file node with its old digest; the next run sees the file as changed again and
/// re-drives the same idempotent symbol upsert (last-writer-wins scalars and a
/// deterministic per-file add-wins tag), so nothing is lost or double-counted.
/// </para>
/// </summary>
internal sealed class RepoContextSymbolReconciler
{
    private const int WriteChunkSize = 256;
    private const long MaxParseBytes = 4L * 1024 * 1024;

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer<SymbolRecord> _symbolSerializer;
    private readonly Serializer<CrossReferenceNode> _crossReferenceSerializer;
    private readonly ISymbolExtractor _extractor;
    private readonly ILogger<RepoContextSymbolReconciler> _logger;

    /// <summary>
    /// Creates the symbol reconciler.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to reach the symbol tree.
    /// Must not be <see langword="null"/>.</param>
    /// <param name="symbolSerializer">The Orleans serializer for
    /// <see cref="SymbolRecord"/>. Must not be <see langword="null"/>.</param>
    /// <param name="crossReferenceSerializer">The Orleans serializer for
    /// <see cref="CrossReferenceNode"/>, used to maintain the reverse cross-reference
    /// projection. Must not be <see langword="null"/>.</param>
    /// <param name="extractor">The language-dispatching symbol extractor. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    public RepoContextSymbolReconciler(
        IGrainFactory grainFactory,
        Serializer<SymbolRecord> symbolSerializer,
        Serializer<CrossReferenceNode> crossReferenceSerializer,
        ISymbolExtractor extractor,
        ILogger<RepoContextSymbolReconciler> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(symbolSerializer);
        ArgumentNullException.ThrowIfNull(crossReferenceSerializer);
        ArgumentNullException.ThrowIfNull(extractor);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _symbolSerializer = symbolSerializer;
        _crossReferenceSerializer = crossReferenceSerializer;
        _extractor = extractor;
        _logger = logger;
    }

    /// <summary>
    /// Reconciles the symbol tree for one bootstrap pass.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="repoRoot">The already-resolved absolute repository root the
    /// changed files are read under. Must not be <see langword="null"/>.</param>
    /// <param name="added">The newly added files to extract. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="updated">The content-changed files to re-extract. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="removedPaths">The pruned file paths whose symbols must drop that
    /// file. Must not be <see langword="null"/>.</param>
    /// <param name="backfill">Content-unchanged files whose symbols were never
    /// extracted (their node predates symbol extraction), to be extracted now exactly
    /// like an added file. Their prior declared set is empty, so no pruning diff runs
    /// for them. Must not be <see langword="null"/>.</param>
    /// <param name="storedMeta">The pre-run stored file metadata, read for the prior
    /// declared-symbol set of each changed or removed file. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the reconcile.</param>
    /// <returns>The reconcile outcome, including the per-file declared sets to stamp
    /// onto the rewritten file nodes.</returns>
    public async Task<SymbolReconcileResult> ReconcileAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> added,
        IReadOnlyList<RepoFileEntry> updated,
        IReadOnlyList<string> removedPaths,
        IReadOnlyList<RepoFileEntry> backfill,
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(repoRoot);
        ArgumentNullException.ThrowIfNull(added);
        ArgumentNullException.ThrowIfNull(updated);
        ArgumentNullException.ThrowIfNull(removedPaths);
        ArgumentNullException.ThrowIfNull(backfill);
        ArgumentNullException.ThrowIfNull(storedMeta);

        var declaredByPath = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        var upsertInfo = new Dictionary<string, (ExtractedSymbol Symbol, string File)>(StringComparer.Ordinal);
        var declaringFilesByFq = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var removeFilesByFq = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (var entry in Concat(added, updated, backfill))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var path = entry.RelativePath;

            if (!_extractor.Supports(entry.Language))
            {
                // An unsupported file declares no symbols and is never a back-fill
                // candidate; leaving it out of the declared map means its node is not
                // stamped as symbol-processed, which is correct - there was nothing to
                // process.
                continue;
            }

            var content = await ReadContentAsync(repoRoot, path, cancellationToken).ConfigureAwait(false);
            if (content is null)
            {
                // A file that cannot be read this pass keeps its prior declared set so
                // its symbol projection is preserved and no symbol-tree change is made
                // on a transient read error. A file with no prior set (a new or
                // never-processed file) is simply omitted, so it is not falsely marked
                // processed and is retried on the next pass.
                var priorOnError = PriorDeclared(storedMeta, path);
                if (priorOnError.Count != 0)
                {
                    declaredByPath[path] = priorOnError;
                }

                continue;
            }

            var extracted = _extractor.Extract(path, entry.Language, content);
            var names = new SortedSet<string>(StringComparer.Ordinal);
            foreach (var symbol in extracted)
            {
                if (string.IsNullOrEmpty(symbol.FullyQualifiedName) || !names.Add(symbol.FullyQualifiedName))
                {
                    continue;
                }

                upsertInfo[symbol.FullyQualifiedName] = (symbol, path);
                AddToSet(declaringFilesByFq, symbol.FullyQualifiedName, path);
            }

            // A supported, read file is recorded even when it declares nothing, so its
            // node is stamped symbol-processed and the back-fill never re-selects it.
            declaredByPath[path] = names.Count == 0 ? [] : [.. names];

            // Updated files may drop symbols they used to declare; compute prior\new.
            var prior = PriorDeclared(storedMeta, path);
            if (prior.Count != 0)
            {
                var newSet = new HashSet<string>(names, StringComparer.Ordinal);
                foreach (var name in prior)
                {
                    if (!newSet.Contains(name))
                    {
                        AddToSet(removeFilesByFq, name, path);
                    }
                }
            }
        }

        foreach (var path in removedPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var name in PriorDeclared(storedMeta, path))
            {
                AddToSet(removeFilesByFq, name, path);
            }
        }

        var applied = await ApplySymbolChangesAsync(
            repoId, upsertInfo, declaringFilesByFq, removeFilesByFq, cancellationToken)
            .ConfigureAwait(false);

        return new SymbolReconcileResult(
            applied.Captured, declaredByPath, applied.ChangedKeys, applied.PrunedKeys);
    }

    private async Task<SymbolApplyOutcome> ApplySymbolChangesAsync(
        string repoId,
        IReadOnlyDictionary<string, (ExtractedSymbol Symbol, string File)> upsertInfo,
        IReadOnlyDictionary<string, HashSet<string>> declaringFilesByFq,
        IReadOnlyDictionary<string, HashSet<string>> removeFilesByFq,
        CancellationToken cancellationToken)
    {
        var touched = new SortedSet<string>(StringComparer.Ordinal);
        touched.UnionWith(upsertInfo.Keys);
        touched.UnionWith(removeFilesByFq.Keys);
        if (touched.Count == 0)
        {
            return new SymbolApplyOutcome(0, Array.Empty<string>(), Array.Empty<string>());
        }

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var writes = new List<KeyValuePair<string, byte[]>>();
        var deletes = new List<string>();
        var changedKeys = new List<string>();
        var prunedKeys = new List<string>();
        var captured = 0;

        // Reverse cross-reference deltas accumulated across this batch and applied in a
        // second phase against the cross-reference tree. Each is keyed by the referenced
        // simple type-name; the value is the set of referrer / test fully-qualified names
        // to add or retire. They stay empty (and allocate nothing beyond the empty maps)
        // for a batch of symbols that neither reference types nor look like tests.
        var referrerAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var referrerRemoves = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var testAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var testRemoves = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (var fqName in touched)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = RepoContextKeys.Symbol(repoId, fqName);
            var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
            var isUpsert = upsertInfo.TryGetValue(fqName, out var info);

            SymbolRecord record;
            if (existing is not null)
            {
                record = _symbolSerializer.Deserialize(existing);
            }
            else if (isUpsert)
            {
                record = new SymbolRecord
                {
                    RepoId = repoId,
                    FullyQualifiedName = fqName,
                    Kind = info.Symbol.Kind,
                };
            }
            else
            {
                // A removal targeting a symbol that no longer exists: nothing to do.
                continue;
            }

            if (removeFilesByFq.TryGetValue(fqName, out var toRemove))
            {
                foreach (var file in toRemove)
                {
                    record.DeclaringFiles.Remove(Encoding.UTF8.GetBytes(file));
                }
            }

            if (isUpsert)
            {
                clock = HybridLogicalClock.Tick(clock);
                record = record with
                {
                    FilePath = RepoContextValues.Lww(info.File, clock),
                    StartLine = RepoContextValues.Lww(info.Symbol.StartLine, clock),
                    EndLine = RepoContextValues.Lww(info.Symbol.EndLine, clock),
                    Signature = RepoContextValues.Lww(info.Symbol.Signature, clock),
                    Digest = RepoContextValues.Lww(info.Symbol.BodyDigest, clock),
                };

                if (declaringFilesByFq.TryGetValue(fqName, out var declaring))
                {
                    foreach (var file in declaring)
                    {
                        // A deterministic per-file tag makes a re-add idempotent and
                        // lets a later removal tombstone exactly this file's dot.
                        record.DeclaringFiles.Add(Encoding.UTF8.GetBytes(file), file, counter: 0);
                    }
                }
            }

            // A type declaration carries the reference and test-linkage edges; the kind
            // comes from the fresh extraction on an upsert and from the stored record on
            // a prune.
            var kind = isUpsert ? info.Symbol.Kind : record.Kind;

            if (record.DeclaringFiles.IsEmpty)
            {
                // The record is going away: retire every inbound reference edge it
                // authored and any test edge it contributed, then delete it.
                foreach (var referenced in DecodeElements(record.References))
                {
                    AddToSet(referrerRemoves, referenced, fqName);
                }

                if (kind == SymbolKind.Type && SymbolNaming.TestSubject(fqName) is { } removedSubject)
                {
                    AddToSet(testRemoves, removedSubject, fqName);
                }

                deletes.Add(key);
                prunedKeys.Add(key);
                continue;
            }

            if (isUpsert)
            {
                // Converge the stored outbound reference set to the freshly extracted
                // one and record the matching inbound-edge deltas.
                UpdateReferences(record.References, info.Symbol.ReferencedNames, fqName, referrerAdds, referrerRemoves);

                if (kind == SymbolKind.Type && SymbolNaming.TestSubject(fqName) is { } subject)
                {
                    AddToSet(testAdds, subject, fqName);
                }
            }

            writes.Add(new KeyValuePair<string, byte[]>(key, _symbolSerializer.SerializeToArray(record)));
            if (isUpsert)
            {
                captured++;
                changedKeys.Add(key);
            }
        }

        await CommitAsync(tree, repoId, writes, deletes, cancellationToken).ConfigureAwait(false);
        await ApplyCrossReferenceChangesAsync(
            repoId, referrerAdds, referrerRemoves, testAdds, testRemoves, cancellationToken)
            .ConfigureAwait(false);
        return new SymbolApplyOutcome(captured, changedKeys, prunedKeys);
    }

    /// <summary>
    /// Applies the accumulated reverse cross-reference deltas to the cross-reference
    /// tree in a read-merge-write pass, one node per referenced simple type-name.
    /// Removes are applied before adds so a name re-added in the same batch wins, and a
    /// node is deleted once both its referrer and test edge sets are empty so the
    /// projection does not leak tombstoned nodes.
    /// </summary>
    private async Task ApplyCrossReferenceChangesAsync(
        string repoId,
        Dictionary<string, HashSet<string>> referrerAdds,
        Dictionary<string, HashSet<string>> referrerRemoves,
        Dictionary<string, HashSet<string>> testAdds,
        Dictionary<string, HashSet<string>> testRemoves,
        CancellationToken cancellationToken)
    {
        var names = new SortedSet<string>(StringComparer.Ordinal);
        names.UnionWith(referrerAdds.Keys);
        names.UnionWith(referrerRemoves.Keys);
        names.UnionWith(testAdds.Keys);
        names.UnionWith(testRemoves.Keys);
        if (names.Count == 0)
        {
            return;
        }

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.CrossReference);
        var writes = new List<KeyValuePair<string, byte[]>>();
        var deletes = new List<string>();

        foreach (var name in names)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = RepoContextKeys.CrossReference(repoId, name);
            var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
            var node = existing is not null
                ? _crossReferenceSerializer.Deserialize(existing)
                : new CrossReferenceNode { RepoId = repoId, Name = name };

            ApplyEdges(node.Referrers, referrerAdds, referrerRemoves, name);
            ApplyEdges(node.Tests, testAdds, testRemoves, name);

            if (node.Referrers.IsEmpty && node.Tests.IsEmpty)
            {
                // Only a stored node needs an explicit delete; a name that resolved to no
                // node and gained no live edge is simply skipped.
                if (existing is not null)
                {
                    deletes.Add(key);
                }
            }
            else
            {
                writes.Add(new KeyValuePair<string, byte[]>(
                    key, _crossReferenceSerializer.SerializeToArray(node)));
            }
        }

        await CommitAsync(tree, repoId, writes, deletes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The outcome of applying one batch of symbol upserts and prunes: the number of
    /// symbols written live, and the canonical record keys upserted (to refresh their
    /// embeddings) and pruned (to retire theirs).
    /// </summary>
    private readonly record struct SymbolApplyOutcome(
        int Captured, IReadOnlyList<string> ChangedKeys, IReadOnlyList<string> PrunedKeys);

    private static async Task CommitAsync(
        ILattice tree,
        string repoId,
        List<KeyValuePair<string, byte[]>> writes,
        List<string> deletes,
        CancellationToken cancellationToken)
    {
        var chunkIndex = 0;
        var remainingDeletes = deletes;
        for (var offset = 0; offset < writes.Count; offset += WriteChunkSize)
        {
            var chunk = writes.GetRange(offset, Math.Min(WriteChunkSize, writes.Count - offset));
            var chunkDeletes = remainingDeletes;
            remainingDeletes = [];
            var operationId = BuildOperationId(repoId, chunkIndex, chunk, chunkDeletes);
            await tree.SetManyAtomicAsync(chunk, chunkDeletes, operationId, cancellationToken).ConfigureAwait(false);
            chunkIndex++;
        }

        if (remainingDeletes.Count != 0)
        {
            var operationId = BuildOperationId(repoId, chunkIndex, [], remainingDeletes);
            await tree.SetManyAtomicAsync([], remainingDeletes, operationId, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task<string?> ReadContentAsync(
        string repoRoot, string relativePath, CancellationToken cancellationToken)
    {
        var fullPath = Path.Combine(repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        try
        {
            var info = new FileInfo(fullPath);
            if (!info.Exists || info.Length > MaxParseBytes)
            {
                return null;
            }

            return await File.ReadAllTextAsync(fullPath, cancellationToken).ConfigureAwait(false);
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static IReadOnlyList<string> PriorDeclared(
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta, string path) =>
        storedMeta.TryGetValue(path, out var meta) ? meta.DeclaredSymbols : [];

    private static void AddToSet(Dictionary<string, HashSet<string>> map, string key, string value)
    {
        if (!map.TryGetValue(key, out var set))
        {
            set = new HashSet<string>(StringComparer.Ordinal);
            map[key] = set;
        }

        set.Add(value);
    }

    /// <summary>
    /// Converges a symbol's stored outbound reference set to <paramref name="newNames"/>
    /// and records the inverse edges: a newly referenced name yields a referrer add, a
    /// dropped name a referrer remove. The prior element set is materialised only when
    /// the record already carried references, and the new set only when the extraction
    /// produced any, so a symbol that neither has nor gains references allocates nothing.
    /// </summary>
    private static void UpdateReferences(
        OrSet references,
        IReadOnlyList<string> newNames,
        string referrer,
        Dictionary<string, HashSet<string>> referrerAdds,
        Dictionary<string, HashSet<string>> referrerRemoves)
    {
        HashSet<string>? prior = null;
        foreach (var bytes in references.Elements())
        {
            (prior ??= new HashSet<string>(StringComparer.Ordinal)).Add(Encoding.UTF8.GetString(bytes));
        }

        var next = newNames.Count == 0 ? null : new HashSet<string>(newNames, StringComparer.Ordinal);

        if (next is not null)
        {
            foreach (var name in next)
            {
                if (prior is null || !prior.Contains(name))
                {
                    references.Add(Encoding.UTF8.GetBytes(name), name, counter: 0);
                    AddToSet(referrerAdds, name, referrer);
                }
            }
        }

        if (prior is not null)
        {
            foreach (var name in prior)
            {
                if (next is null || !next.Contains(name))
                {
                    references.Remove(Encoding.UTF8.GetBytes(name));
                    AddToSet(referrerRemoves, name, referrer);
                }
            }
        }
    }

    private static void ApplyEdges(
        OrSet set,
        Dictionary<string, HashSet<string>> adds,
        Dictionary<string, HashSet<string>> removes,
        string name)
    {
        if (removes.TryGetValue(name, out var toRemove))
        {
            foreach (var element in toRemove)
            {
                set.Remove(Encoding.UTF8.GetBytes(element));
            }
        }

        if (adds.TryGetValue(name, out var toAdd))
        {
            foreach (var element in toAdd)
            {
                set.Add(Encoding.UTF8.GetBytes(element), element, counter: 0);
            }
        }
    }

    private static IEnumerable<string> DecodeElements(OrSet set)
    {
        foreach (var bytes in set.Elements())
        {
            yield return Encoding.UTF8.GetString(bytes);
        }
    }


    private static IEnumerable<RepoFileEntry> Concat(
        IReadOnlyList<RepoFileEntry> first,
        IReadOnlyList<RepoFileEntry> second,
        IReadOnlyList<RepoFileEntry> third)
    {
        foreach (var entry in first)
        {
            yield return entry;
        }

        foreach (var entry in second)
        {
            yield return entry;
        }

        foreach (var entry in third)
        {
            yield return entry;
        }
    }

    private static string BuildOperationId(
        string repoId,
        int chunkIndex,
        IReadOnlyList<KeyValuePair<string, byte[]>> upserts,
        IReadOnlyList<string> deletes)
    {
        var builder = new StringBuilder();
        builder.Append(repoId).Append('\n').Append(chunkIndex);
        foreach (var upsert in upserts)
        {
            builder.Append("\nU").Append(upsert.Key).Append('=').Append(FileDigest.Compute(upsert.Value));
        }

        foreach (var delete in deletes)
        {
            builder.Append("\nD").Append(delete);
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return "rcs-" + Convert.ToHexStringLower(hash.AsSpan(0, 16));
    }
}
