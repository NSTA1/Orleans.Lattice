using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File;

/// <summary>
/// Durable, cloud-free <see cref="IWalStorageProvider"/> that persists
/// each per-shard write-ahead log to a segmented append-only file on the
/// local disk. Registered through
/// <see cref="LatticeFileServiceCollectionExtensions.AddFileWalStorage"/>.
/// <para>
/// The provider matches the observable durability contract of the Azure
/// Table Storage provider without any cloud dependency, making it the
/// enabler for a single-container, "codebase memory in a box"
/// deployment:
/// </para>
/// <list type="bullet">
/// <item><description><b>All-or-nothing batch append.</b> A batch is
/// framed as a run of data records sealed by a single commit trailer and
/// written with one <c>write</c> + fsync; a crash before the trailer is
/// durable rolls the whole batch back on recovery.</description></item>
/// <item><description><b>Monotonic durable tail.</b>
/// <see cref="GetHighestOffsetAsync"/> returns the highest committed
/// offset, which only advances as batches commit.</description></item>
/// <item><description><b>Verbatim, dense, non-overlapping offsets.</b>
/// Caller-assigned offsets are stored verbatim; overlap with any
/// persisted offset is rejected and contiguity with the tail is never
/// assumed, so out-of-order concurrent appends (<see cref="LatticeOptions.WalMaxPendingBatches"/>
/// &gt; 1) are supported and a failed flush surfaces as an honest gap.</description></item>
/// <item><description><b>Crash reconciliation.</b>
/// <see cref="ReconcileAsync"/> rolls forward every committed batch,
/// discards any torn tail, and reclaims trimmed space.</description></item>
/// </list>
/// <para>
/// The on-disk payload for each entry is the
/// <see cref="WalRecord"/>-shaped Orleans-serialised bytes, identical to
/// the Azure Table provider's row payload, so
/// <see cref="AppendEncodedBatchAsync"/> stores the caller's pre-encoded
/// segments verbatim (zero re-encode) and
/// <see cref="ReadEncodedAsync"/> returns them verbatim (zero
/// re-materialisation).
/// </para>
/// </summary>
public sealed class FileWalStorageProvider : IWalStorageProvider, IDisposable
{
    private readonly FileWalStorageOptions _options;
    private readonly Serializer<WalRecord> _serializer;
    private readonly ConcurrentDictionary<(string TreeId, int ShardIndex), FileWalShard> _shards = new();
    private bool _disposed;

    /// <summary>
    /// Creates a provider that stores every tree/shard WAL under
    /// <see cref="FileWalStorageOptions.RootDirectory"/>.
    /// </summary>
    /// <param name="options">The file provider options. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">Orleans serializer used to project a
    /// provider-boundary <see cref="WalEntry"/> onto the durability-shaped
    /// <see cref="WalRecord"/> stored on disk, matching the Azure Table
    /// provider's on-disk format. Must not be <see langword="null"/>.</param>
    public FileWalStorageProvider(IOptions<FileWalStorageOptions> options, Serializer<WalRecord> serializer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serializer);
        _options = options.Value ?? throw new ArgumentException(
            $"{nameof(IOptions<FileWalStorageOptions>)}.{nameof(IOptions<FileWalStorageOptions>.Value)} returned null.",
            nameof(options));
        if (string.IsNullOrWhiteSpace(_options.RootDirectory))
        {
            throw new ArgumentException(
                $"{nameof(FileWalStorageOptions)}.{nameof(FileWalStorageOptions.RootDirectory)} must be a non-empty filesystem path.",
                nameof(options));
        }

        _serializer = serializer;
    }

    /// <inheritdoc />
    public Task AppendBatchAsync(
        string treeId,
        int shardIndex,
        IReadOnlyList<WalEntry> entries,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        if (entries.Count == 0)
        {
            return Task.CompletedTask;
        }

        // Legacy WalEntry seam: project each mutation onto the durability-
        // shaped WalRecord and serialise it so the on-disk format matches
        // AppendEncodedBatchAsync exactly. The mutation's own declared
        // merge mode is carried through so a delta-only CRDT entry keeps
        // its authored mode on disk (issue #926); origin falls back to
        // empty because the converter preserves the mutation's own origin
        // id when present. One ArrayBufferWriter per entry - the WalEntry
        // path is the fallback seam; the hot commit path is the zero-copy
        // AppendEncodedBatchAsync overload.
        var prepared = new PreparedWalRecord[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            var record = WalRecordConverter.ToWalRecord(
                entries[i].Mutation,
                entries[i].Mutation.Mode,
                string.Empty);
            var writer = new ArrayBufferWriter<byte>();
            _serializer.Serialize(record, writer);
            prepared[i] = new PreparedWalRecord(entries[i].Offset, writer.WrittenMemory);
        }

        return GetShard(treeId, shardIndex).AppendAsync(prepared, cancellationToken);
    }

    /// <inheritdoc />
    public Task AppendEncodedBatchAsync(
        string treeId,
        int shardIndex,
        ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
        ReadOnlyMemory<long> offsets,
        IWalRecordEncoder encoder,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(encoder);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();

        if (encodedEntries.Length != offsets.Length)
        {
            throw new ArgumentException(
                $"Encoded segment count ({encodedEntries.Length}) does not match offset count ({offsets.Length}); the two sequences must be parallel.",
                nameof(encodedEntries));
        }

        if (encodedEntries.Length == 0)
        {
            return Task.CompletedTask;
        }

        // Zero-copy fast path: the producer already paid the encode cost
        // via IWalRecordEncoder, so the segments are stored verbatim as the
        // durable payload - no decode/re-encode round-trip.
        var segments = encodedEntries.Span;
        var offsetSpan = offsets.Span;
        var prepared = new PreparedWalRecord[segments.Length];
        for (var i = 0; i < segments.Length; i++)
        {
            prepared[i] = new PreparedWalRecord(offsetSpan[i], segments[i].AsMemory());
        }

        return GetShard(treeId, shardIndex).AppendAsync(prepared, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<WalEntry> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per read.");
        }

        ThrowIfDisposed();

        var (offsets, payloads) = await GetShard(treeId, shardIndex)
            .SnapshotAsync(fromOffsetExclusive, maxEntries, cancellationToken)
            .ConfigureAwait(false);

        for (var i = 0; i < offsets.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = _serializer.Deserialize(new ReadOnlyMemory<byte>(payloads[i]));
            yield return new WalEntry
            {
                Offset = offsets[i],
                Mutation = WalRecordConverter.FromWalRecord(in record),
            };
        }
    }

    /// <inheritdoc />
    public async Task<WalShardEncodedPage> ReadEncodedAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        IWalRecordEncoder encoder,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(encoder);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per read.");
        }

        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        // The stored payload is already the encoded WalRecord bytes, so the
        // segments are returned verbatim - no per-entry WalRecord
        // materialisation and re-encode. Each payload is a freshly-owned
        // array read from disk, so it outlives the synchronous return.
        var (offsets, payloads) = await GetShard(treeId, shardIndex)
            .SnapshotAsync(fromOffsetExclusive, maxEntries, cancellationToken)
            .ConfigureAwait(false);

        var segments = new ArraySegment<byte>[payloads.Length];
        for (var i = 0; i < payloads.Length; i++)
        {
            segments[i] = new ArraySegment<byte>(payloads[i]);
        }

        return new WalShardEncodedPage
        {
            EncodedEntries = segments,
            Offsets = offsets,
            HighestOffsetInclusive = offsets.Length == 0 ? -1L : offsets[^1],
        };
    }

    /// <inheritdoc />
    public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        return GetShard(treeId, shardIndex).GetHighestOffsetAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        return GetShard(treeId, shardIndex).GetLowestOffsetAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        return GetShard(treeId, shardIndex).TrimAsync(throughOffsetInclusive, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        return GetShard(treeId, shardIndex).GetRetainedByteSizeAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        return GetShard(treeId, shardIndex).ReconcileAsync(cancellationToken);
    }

    /// <summary>
    /// Resolves (creating on first use) the <see cref="FileWalShard"/> for
    /// a tree/shard pair. The shard directory is
    /// <c>{RootDirectory}/{encodedTreeId}/shard-{shardIndex}</c>.
    /// </summary>
    private FileWalShard GetShard(string treeId, int shardIndex)
    {
        return _shards.GetOrAdd((treeId, shardIndex), static (key, options) =>
        {
            var directory = Path.Combine(
                options.RootDirectory,
                EncodePathSegment(key.TreeId),
                "shard-" + key.ShardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture));
            return new FileWalShard(directory, options);
        }, _options);
    }

    /// <summary>
    /// Percent-encodes a tree id into an injective, filesystem-safe path
    /// segment. Every byte outside the unreserved set
    /// <c>[A-Za-z0-9-._]</c> is written as <c>%XX</c> (uppercase hex of
    /// its UTF-8 byte), so distinct tree ids always map to distinct
    /// directories.
    /// </summary>
    /// <remarks>
    /// A tree id is an opaque, caller-supplied string, so the encoder is a
    /// security boundary: the encoded segment must always name a directory
    /// rather than a relative path token, and two distinct ids must never name
    /// the same directory. <c>.</c> is in the unreserved set (it is legitimate
    /// inside a tree name), which on its own would leave two defects:
    /// <list type="bullet">
    /// <item>
    /// <description>
    /// The ids <c>"."</c> and <c>".."</c> would encode to themselves, and
    /// <see cref="Path.Combine(string, string, string)"/> performs no
    /// normalisation, so <c>".."</c> would resolve the shard directory outside
    /// the operator-configured <see cref="FileWalStorageOptions.RootDirectory"/>
    /// and write a WAL beyond the ACLs, quotas, and retention policy scoped to
    /// that root.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// Windows strips trailing dots from a path component, so the ids <c>"a"</c>
    /// and <c>"a."</c> would both resolve to the directory <c>a</c> - two
    /// distinct trees silently sharing one WAL directory, each overwriting the
    /// other's log - while an id such as <c>"a.."</c> would fail the directory
    /// creation outright. That breaks the injectivity this encoder exists to
    /// provide, on the platform the library is developed and tested on.
    /// </description>
    /// </item>
    /// </list>
    /// Escaping the segment's <b>trailing dot run</b> closes both: a segment can
    /// then neither consist solely of relative-path tokens nor end in a
    /// character the filesystem will strip. Dots elsewhere are untouched, so
    /// <c>a.b</c> and <c>..a</c> keep their natural spelling. Injectivity is
    /// preserved because <c>%</c> is itself always escaped, so a literal
    /// <c>%2E</c> in a tree id encodes to <c>%252E</c> and no other id can
    /// encode to a given output.
    /// </remarks>
    internal static string EncodePathSegment(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var bytes = Encoding.UTF8.GetBytes(treeId);
        var builder = new StringBuilder(bytes.Length);
        var trailingDots = 0;
        foreach (var b in bytes)
        {
            var isUnreserved = b is (>= (byte)'A' and <= (byte)'Z')
                or (>= (byte)'a' and <= (byte)'z')
                or (>= (byte)'0' and <= (byte)'9')
                or (byte)'-' or (byte)'.' or (byte)'_';
            if (isUnreserved)
            {
                // Track the run of dots ending the segment; any other unreserved
                // byte resets it.
                trailingDots = b == (byte)'.' ? trailingDots + 1 : 0;
                builder.Append((char)b);
            }
            else
            {
                trailingDots = 0;
                builder.Append('%');
                builder.Append(b.ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            }
        }

        // Guard against an empty segment (an empty tree id) so Path.Combine
        // never collapses the directory.
        if (builder.Length == 0)
        {
            return "_";
        }

        // Escape the trailing dot run. This subsumes the all-dot segments
        // (".", "..", "...") - which are relative path tokens Path.Combine would
        // let escape the configured WAL root - and the trailing-dot ids ("a.",
        // "a..") that Windows would otherwise fold onto a sibling tree's
        // directory or reject outright. Only allocated on this cold path; an
        // ordinary tree id leaves the fast path untouched.
        if (trailingDots > 0)
        {
            var keep = builder.Length - trailingDots;
            var escaped = new StringBuilder(keep + (trailingDots * 3));
            escaped.Append(builder, 0, keep);
            for (var i = 0; i < trailingDots; i++)
            {
                escaped.Append("%2E");
            }

            return escaped.ToString();
        }

        return builder.ToString();
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    /// <summary>
    /// Disposes every open shard file handle. Called at silo shutdown; the
    /// durable log survives on disk and is recovered on the next
    /// activation via <see cref="ReconcileAsync"/>.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var shard in _shards.Values)
        {
            shard.Dispose();
        }

        _shards.Clear();
    }
}
