using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The coordinator behind the <c>repocontext_bootstrap</c> tool: it walks a
/// repository, reconciles the scan against the structural records already stored
/// for that repository, and applies exactly the difference - creating new file
/// nodes, updating changed ones, and pruning nodes whose files are gone - using
/// the core atomic batch write primitive.
/// <para>
/// <b>Idempotent and resumable.</b> Every file node stores a content digest, so a
/// re-run over an unchanged tree computes an empty plan and writes nothing; a
/// changed tree writes only the changed files; and a crashed run resumes cleanly
/// because the next attempt sees the already-persisted files as unchanged and
/// skips them. Writes are committed in bounded chunks via
/// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, IReadOnlyList{string}, string, CancellationToken)"/>,
/// each keyed by a deterministic operation id derived from the chunk's exact keys
/// and content, so re-submitting an identical chunk safely re-attaches to the
/// original all-or-nothing saga instead of duplicating work.
/// </para>
/// <para>
/// <b>Vectorisation boundary.</b> Structural ingestion is the whole deliverable
/// here. Changed files are offered to the injected
/// <see cref="IRepoContextVectorIngestor"/> seam, whose shipped binding is a
/// no-op: the vector record shape and the vector write / retrieval path are owned
/// by separate work, so bootstrap does not persist vectors and does not race that
/// surface.
/// </para>
/// </summary>
internal sealed class RepoContextBootstrapService
{
    private const int WriteChunkSize = 256;

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer<FileNode> _fileNodeSerializer;
    private readonly Serializer<RepoNode> _repoNodeSerializer;
    private readonly IRepoContextVectorIngestor _vectorIngestor;
    private readonly ILogger<RepoContextBootstrapService> _logger;

    /// <summary>
    /// Creates the bootstrap coordinator.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to reach the structural
    /// Lattice tree. Must not be <see langword="null"/>.</param>
    /// <param name="fileNodeSerializer">The Orleans serializer for
    /// <see cref="FileNode"/>. Must not be <see langword="null"/>.</param>
    /// <param name="repoNodeSerializer">The Orleans serializer for
    /// <see cref="RepoNode"/>. Must not be <see langword="null"/>.</param>
    /// <param name="vectorIngestor">The vectorisation seam (a no-op by default).
    /// Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    public RepoContextBootstrapService(
        IGrainFactory grainFactory,
        Serializer<FileNode> fileNodeSerializer,
        Serializer<RepoNode> repoNodeSerializer,
        IRepoContextVectorIngestor vectorIngestor,
        ILogger<RepoContextBootstrapService> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(fileNodeSerializer);
        ArgumentNullException.ThrowIfNull(repoNodeSerializer);
        ArgumentNullException.ThrowIfNull(vectorIngestor);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _fileNodeSerializer = fileNodeSerializer;
        _repoNodeSerializer = repoNodeSerializer;
        _vectorIngestor = vectorIngestor;
        _logger = logger;
    }

    /// <summary>
    /// Runs one idempotent ingestion pass and returns a summary of what changed.
    /// </summary>
    /// <param name="request">The ingestion inputs. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>A summary of files scanned, added, updated, removed, and unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is null.</exception>
    /// <exception cref="ArgumentException">The request omits a repository root or id.</exception>
    public async Task<RepoContextBootstrapResult> RunAsync(
        RepoContextBootstrapRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.RepoRoot))
        {
            throw new ArgumentException("The repository root must be provided.", nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.RepoId))
        {
            throw new ArgumentException("The repository id must be provided.", nameof(request));
        }

        var stopwatch = Stopwatch.StartNew();
        var repoRoot = Path.GetFullPath(request.RepoRoot);
        var repoId = request.RepoId;

        var scanned = RepoTreeWalker.Walk(
            repoRoot, request.IncludeGlobs, request.ExcludeGlobs, cancellationToken);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var storedDigests = await ReadStoredDigestsAsync(tree, repoId, cancellationToken)
            .ConfigureAwait(false);

        var plan = RepoContextBootstrapPlan.Compute(storedDigests, scanned);

        if (!plan.IsNoOp)
        {
            await ApplyPlanAsync(tree, repoId, plan, cancellationToken).ConfigureAwait(false);

            var changed = new List<RepoFileEntry>(plan.Added.Count + plan.Updated.Count);
            changed.AddRange(plan.Added);
            changed.AddRange(plan.Updated);
            await _vectorIngestor.IngestAsync(repoId, repoRoot, changed, cancellationToken)
                .ConfigureAwait(false);
        }

        stopwatch.Stop();

        _logger.LogInformation(
            "Bootstrap of repository {RepoId} scanned {Scanned} files: {Added} added, {Updated} updated, {Removed} removed, {Unchanged} unchanged in {Elapsed} ms.",
            repoId,
            scanned.Count,
            plan.Added.Count,
            plan.Updated.Count,
            plan.RemovedPaths.Count,
            plan.Unchanged.Count,
            stopwatch.ElapsedMilliseconds);

        return new RepoContextBootstrapResult
        {
            RepoId = repoId,
            FilesScanned = scanned.Count,
            FilesAdded = plan.Added.Count,
            FilesUpdated = plan.Updated.Count,
            FilesRemoved = plan.RemovedPaths.Count,
            FilesUnchanged = plan.Unchanged.Count,
            SymbolsCaptured = 0,
            ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
        };
    }

    private async Task<Dictionary<string, string>> ReadStoredDigestsAsync(
        ILattice tree,
        string repoId,
        CancellationToken cancellationToken)
    {
        var prefix = RepoContextKeys.FilesPrefix(repoId);
        var endExclusive = PrefixUpperBound(prefix);
        var digests = new Dictionary<string, string>(StringComparer.Ordinal);

        var cursorId = await tree.OpenEntryCursorAsync(
            prefix, endExclusive, reverse: false, pointInTime: false, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var page = await tree.NextEntriesAsync(cursorId, WriteChunkSize, cancellationToken)
                    .ConfigureAwait(false);

                foreach (var entry in page.Entries)
                {
                    if (!RepoContextKeys.TryParse(entry.Key, out var parsed)
                        || parsed.Kind != RepoContextRecordKind.File
                        || parsed.Path is not { } path)
                    {
                        continue;
                    }

                    var node = _fileNodeSerializer.Deserialize(entry.Value);
                    var digest = RepoContextValues.ReadString(node.Digest);
                    if (digest is not null)
                    {
                        digests[path] = digest;
                    }
                }

                if (!page.HasMore)
                {
                    break;
                }
            }
        }
        finally
        {
            await tree.CloseCursorAsync(cursorId, CancellationToken.None).ConfigureAwait(false);
        }

        return digests;
    }

    private async Task ApplyPlanAsync(
        ILattice tree,
        string repoId,
        RepoContextBootstrapPlan plan,
        CancellationToken cancellationToken)
    {
        var ingestToken = DateTimeOffset.UtcNow.ToString("O");
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        var upserts = new List<KeyValuePair<string, byte[]>>(plan.Added.Count + plan.Updated.Count + 1);

        // Refresh the repository root marker in the same pass that mutates its files.
        clock = HybridLogicalClock.Tick(clock);
        upserts.Add(new KeyValuePair<string, byte[]>(
            RepoContextKeys.Repo(repoId), BuildRepoNode(repoId, ingestToken, clock)));

        foreach (var entry in plan.Added)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, ingestToken, clock)));
        }

        foreach (var entry in plan.Updated)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, ingestToken, clock)));
        }

        var deletes = new List<string>(plan.RemovedPaths.Count);
        foreach (var path in plan.RemovedPaths)
        {
            deletes.Add(RepoContextKeys.File(repoId, path));
        }

        // Commit in bounded chunks; each chunk is an all-or-nothing atomic batch
        // keyed by a deterministic operation id so an interrupted run's retry
        // re-attaches to the original saga rather than duplicating writes. Deletes
        // ride with the first chunk so a pure prune still commits atomically.
        var chunkIndex = 0;
        var remainingDeletes = deletes;
        for (var offset = 0; offset < upserts.Count; offset += WriteChunkSize)
        {
            var chunk = upserts.GetRange(offset, Math.Min(WriteChunkSize, upserts.Count - offset));
            var chunkDeletes = remainingDeletes;
            remainingDeletes = [];

            var operationId = BuildOperationId(repoId, chunkIndex, chunk, chunkDeletes);
            await tree.SetManyAtomicAsync(chunk, chunkDeletes, operationId, cancellationToken)
                .ConfigureAwait(false);
            chunkIndex++;
        }

        if (remainingDeletes.Count != 0)
        {
            var operationId = BuildOperationId(repoId, chunkIndex, [], remainingDeletes);
            await tree.SetManyAtomicAsync([], remainingDeletes, operationId, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private byte[] BuildFileNode(
        string repoId, RepoFileEntry entry, string ingestToken, HybridLogicalClock clock)
    {
        var node = new FileNode
        {
            RepoId = repoId,
            Path = entry.RelativePath,
            Digest = RepoContextValues.Lww(entry.Digest, clock),
            Language = RepoContextValues.Lww(entry.Language, clock),
            SizeBytes = RepoContextValues.Lww(entry.SizeBytes, clock),
            LastIngested = RepoContextValues.Lww(ingestToken, clock),
        };
        return _fileNodeSerializer.SerializeToArray(node);
    }

    private byte[] BuildRepoNode(string repoId, string ingestToken, HybridLogicalClock clock)
    {
        var node = new RepoNode
        {
            RepoId = repoId,
            LastIngested = RepoContextValues.Lww(ingestToken, clock),
        };
        return _repoNodeSerializer.SerializeToArray(node);
    }

    /// <summary>
    /// Derives a deterministic, filesystem-safe operation id from a chunk's exact
    /// keys and content, so an identical retry re-attaches to the original atomic
    /// saga while any genuine content change starts a fresh one.
    /// </summary>
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
        return "rcb-" + Convert.ToHexStringLower(hash.AsSpan(0, 16));
    }

    private static string PrefixUpperBound(string prefix)
    {
        // The exclusive upper bound of a prefix range is the prefix with its last
        // character incremented, which sorts immediately after every key the
        // prefix covers.
        var last = prefix[^1];
        return string.Concat(prefix.AsSpan(0, prefix.Length - 1), ((char)(last + 1)).ToString());
    }
}
