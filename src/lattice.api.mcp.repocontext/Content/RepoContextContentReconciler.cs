using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Reconciles the per-file searchable-content projection in the dedicated
/// <see cref="RepoContextTrees.Content"/> tree against a bootstrap plan. For every
/// added, updated, or back-filled file it reads the file's body text (bounded and
/// truncated to <see cref="ContentRecord.MaxContentChars"/>) and upserts one
/// <see cref="ContentRecord"/> keyed by <c>repo/{repoId}/content/{path}</c>; for
/// every removed file it deletes that file's content record.
/// <para>
/// <b>Decoupled from embeddings.</b> The whole point of the projection is to improve
/// the keyword/degraded search path that runs when no embedder is bound, so content
/// is written here - during the structural reconcile, where the changed-file bytes
/// are already being read for hashing - and never in the embedding ingestor. Every
/// walked file is a text file (the walk excludes binary), so all files are indexed;
/// a file larger than <see cref="MaxReadBytes"/> is skipped so a single huge
/// generated file cannot stall the pass.
/// </para>
/// <para>
/// <b>Resumable by construction.</b> The reconcile runs before the file nodes are
/// rewritten, so a crash between the content write and the file-node write leaves
/// the file node without its <see cref="FileNode.ContentProcessed"/> marker; the
/// next run re-detects the file as changed - or as an un-processed back-fill
/// candidate - and re-drives the same idempotent overwrite.
/// </para>
/// </summary>
internal sealed class RepoContextContentReconciler
{
    private const int WriteChunkSize = 256;
    private const long MaxReadBytes = 4L * 1024 * 1024;

    /// <summary>
    /// The empty processed-path set returned when content projection is skipped this
    /// pass because the content tree is terminally stale. Every affected file is then
    /// left unmarked so the content back-fill re-projects it once the tree is healed.
    /// A shared immutable instance keeps the degraded path allocation-free.
    /// </summary>
    private static readonly IReadOnlySet<string> EmptyProcessed =
        new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// The empty token-count map returned alongside <see cref="EmptyProcessed"/> on the
    /// degraded (terminally-stale content tree) path, where no body is projected and so
    /// no token count is computed. A shared immutable instance keeps that path
    /// allocation-free.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, int> EmptyTokenCounts =
        new Dictionary<string, int>(StringComparer.Ordinal);

    /// <summary>
    /// How many additional files must be projected between content-projection
    /// heartbeat log lines. Mirrors the vectorising phase's heartbeat: a large
    /// content back-fill (a repository indexed before the content projection existed
    /// re-reads every text file) is otherwise a single silent await, so throttling
    /// one line per this many freshly read files keeps the back-fill observable in
    /// the log without emitting a line per file. A normal incremental pass touches
    /// far fewer files than this and so stays silent.
    /// </summary>
    private const int ProgressHeartbeatInterval = 500;

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer<ContentRecord> _contentSerializer;
    private readonly IRepoContextTokenCounter _tokenCounter;
    private readonly ILogger<RepoContextContentReconciler> _logger;

    /// <summary>
    /// Creates the content reconciler.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to reach the content tree.
    /// Must not be <see langword="null"/>.</param>
    /// <param name="contentSerializer">The Orleans serializer for
    /// <see cref="ContentRecord"/>. Must not be <see langword="null"/>.</param>
    /// <param name="tokenCounter">The shared BPE token counter used to measure each
    /// file's decoded body once while it is in hand. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    public RepoContextContentReconciler(
        IGrainFactory grainFactory,
        Serializer<ContentRecord> contentSerializer,
        IRepoContextTokenCounter tokenCounter,
        ILogger<RepoContextContentReconciler> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(contentSerializer);
        ArgumentNullException.ThrowIfNull(tokenCounter);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _contentSerializer = contentSerializer;
        _tokenCounter = tokenCounter;
        _logger = logger;
    }

    /// <summary>
    /// Reconciles the content tree for one bootstrap pass.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="repoRoot">The already-resolved absolute repository root the
    /// files are read under. Must not be <see langword="null"/>.</param>
    /// <param name="added">The newly added files to project. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="updated">The content-changed files to re-project. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="removedPaths">The pruned file paths whose content records must be
    /// deleted. Must not be <see langword="null"/>.</param>
    /// <param name="backfill">Content-unchanged files whose content was never
    /// projected (their node predates the content projection), to be projected now
    /// exactly like an added file. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the reconcile.</param>
    /// <returns>The reconcile outcome, including the per-file processed set to stamp
    /// onto the rewritten file nodes.</returns>
    public async Task<ContentReconcileResult> ReconcileAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> added,
        IReadOnlyList<RepoFileEntry> updated,
        IReadOnlyList<string> removedPaths,
        IReadOnlyList<RepoFileEntry> backfill,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(repoRoot);
        ArgumentNullException.ThrowIfNull(added);
        ArgumentNullException.ThrowIfNull(updated);
        ArgumentNullException.ThrowIfNull(removedPaths);
        ArgumentNullException.ThrowIfNull(backfill);

        var processed = new HashSet<string>(StringComparer.Ordinal);
        var tokenCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        var writes = new List<KeyValuePair<string, byte[]>>();
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var total = added.Count + updated.Count + backfill.Count;
        var stopwatch = Stopwatch.StartNew();
        var lastHeartbeat = 0;

        foreach (var entry in Concat(added, updated, backfill))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var path = entry.RelativePath;

            var text = await ReadContentAsync(repoRoot, path, cancellationToken).ConfigureAwait(false);
            if (text is null)
            {
                // A file that cannot be read this pass is left unmarked and retried by
                // the content back-fill on a later pass; it is not falsely recorded as
                // processed.
                continue;
            }

            clock = HybridLogicalClock.Tick(clock);
            var record = ContentRecord.Create(repoId, path, text, clock);
            writes.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.Content(repoId, path), _contentSerializer.SerializeToArray(record)));
            processed.Add(path);

            // Count the file's BPE tokens once, while the decoded body is already in
            // hand, so the per-file token count is stored (not recomputed per call).
            // The reconciler truncates the body to ContentRecord.MaxContentChars for the
            // projection but the token count measures the full decoded text.
            tokenCounts[path] = _tokenCounter.CountTokens(text);

            if (processed.Count - lastHeartbeat >= ProgressHeartbeatInterval)
            {
                lastHeartbeat = processed.Count;
                _logger.LogInformation(
                    "Repo {RepoId}: content projection progress - {Written} of {Total} file(s) projected after {Elapsed} ms.",
                    repoId, processed.Count, total, stopwatch.ElapsedMilliseconds);
            }
        }

        var deletes = new List<string>(removedPaths.Count);
        foreach (var path in removedPaths)
        {
            deletes.Add(RepoContextKeys.Content(repoId, path));
        }

        try
        {
            await CommitAsync(repoId, writes, deletes, cancellationToken).ConfigureAwait(false);
        }
        catch (LeafProjectionStaleException stale)
        {
            // The content tree is a rebuildable derived projection, not a store of
            // record (see RepoContextTrees.Content), so a terminally-stale content
            // leaf - one whose durable projection checkpoint fell off its write-ahead
            // log with no covering snapshot - must never fail the whole repository
            // index. Skip content projection this pass and leave every file unmarked
            // so the content back-fill re-projects it once the content tree is healed
            // out of band. Structural, symbol, and semantic ingest are independent and
            // proceed, and the keyword search path already isolates a faulting content
            // tree, so retrieval stays correct (minus body ranking) in the meantime.
            // This tolerance is scoped to the content tree because CommitAsync only
            // ever writes there; a store-of-record tree keeps the throwing default.
            _logger.LogWarning(
                stale,
                "Repo {RepoId}: content projection is stale and could not be written (the content " +
                "tree's durable checkpoint fell off its write-ahead log); skipping content projection " +
                "this pass. {Written} pending record(s) were dropped and their files left unmarked so " +
                "the back-fill retries them once the content tree is healed.",
                repoId, writes.Count);
            return new ContentReconcileResult(EmptyProcessed, 0, EmptyTokenCounts);
        }

        if (writes.Count > 0 || deletes.Count > 0)
        {
            _logger.LogInformation(
                "Repo {RepoId}: projected {Written} file content record(s), {Deleted} deleted, in {Elapsed} ms.",
                repoId, writes.Count, deletes.Count, stopwatch.ElapsedMilliseconds);
        }

        return new ContentReconcileResult(processed, writes.Count, tokenCounts);
    }

    private async Task CommitAsync(
        string repoId,
        List<KeyValuePair<string, byte[]>> writes,
        List<string> deletes,
        CancellationToken cancellationToken)
    {
        if (writes.Count == 0 && deletes.Count == 0)
        {
            return;
        }

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Content);
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
            if (!info.Exists || info.Length > MaxReadBytes)
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
        return "rcc-" + Convert.ToHexStringLower(hash.AsSpan(0, 16));
    }
}
