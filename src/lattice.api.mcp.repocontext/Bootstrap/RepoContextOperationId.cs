using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Derives the deterministic, filesystem-safe operation id each reconciler stamps
/// onto a chunk's atomic saga, so an identical retry re-attaches to the original
/// saga while any genuine content change starts a fresh one.
/// <para>
/// <b>The id is an idempotency key, so its bytes are load-bearing.</b> The hashed
/// stream is the concatenation, in order, of the optional operation scope, the
/// repository id, the chunk index, then <c>"\nU"</c> + key + <c>'='</c> + the
/// value's tagged content digest for every upsert, then <c>"\nD"</c> + key for
/// every delete - each part encoded as UTF-8. That is byte-for-byte the stream the
/// three reconcilers produced when each staged the same parts through a
/// <see cref="StringBuilder"/>, materialised it with <c>ToString</c> and re-encoded
/// it with <c>Encoding.UTF8.GetBytes</c>; every part boundary falls next to an
/// ASCII character, so no surrogate pair can straddle one and the split encoding is
/// identical to the whole-string encoding.
/// </para>
/// <para>
/// <b>Why it stages into one rented buffer rather than folding incrementally.</b>
/// The staged shape allocated the builder's chunk chain, the materialised string,
/// its whole UTF-8 encoding and the returned hash array - plus one digest string
/// per upsert - none of which outlived the call, on a path that runs once per chunk
/// for every pass of every repository. Transcoding each part straight into a single
/// pooled UTF-8 buffer removes all of them while still hashing exactly once.
/// Folding the parts into an <see cref="IncrementalHash"/> instead removes the same
/// allocations but costs a managed-to-platform transition per part, which a
/// measured lane put at 34% slower for a 16-upsert chunk and 48% slower for a
/// 256-upsert one - so it is deliberately not the shape used here. Nor is this the
/// losing shape recorded for an exact-size encoder: the capacity bound below is
/// pure arithmetic over the part lengths, so no part is ever walked twice and the
/// buffer never grows.
/// </para>
/// </summary>
internal static class RepoContextOperationId
{
    /// <summary>
    /// The UTF-8 scratch budget the whole stream takes on the stack before renting.
    /// A chunk carrying a handful of short keys fits, so the smallest passes rent
    /// nothing at all.
    /// </summary>
    private const int StackStageBytes = 512;

    /// <summary>The widest invariant decimal spelling a chunk index can take.</summary>
    private const int MaxChunkIndexBytes = 16;

    /// <summary>The leading bytes of the SHA-256 digest the id spells out in hex.</summary>
    private const int IdHashBytes = 16;

    /// <summary>The widest prefix tag an id may carry.</summary>
    private const int MaxPrefixChars = 8;

    /// <summary>
    /// Builds the operation id for one chunk.
    /// </summary>
    /// <param name="prefix">The reconciler's id tag, for example <c>"rcb-"</c>.</param>
    /// <param name="operationScope">An optional leading scope part, staged ahead of
    /// the repository id when present.</param>
    /// <param name="repoId">The repository the chunk belongs to.</param>
    /// <param name="chunkIndex">The chunk's ordinal within the pass.</param>
    /// <param name="upserts">The chunk's upserts, in order.</param>
    /// <param name="deletes">The chunk's deletes, in order.</param>
    /// <returns>The deterministic operation id.</returns>
    internal static string Build(
        string prefix,
        string? operationScope,
        string repoId,
        int chunkIndex,
        IReadOnlyList<KeyValuePair<string, byte[]>> upserts,
        IReadOnlyList<string> deletes)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(upserts);
        ArgumentNullException.ThrowIfNull(deletes);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(prefix.Length, MaxPrefixChars);

        var capacity = StageCapacity(operationScope, repoId, upserts, deletes);
        byte[]? rented = null;
        var stage = capacity <= StackStageBytes
            ? stackalloc byte[StackStageBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(capacity));
        var written = 0;
        try
        {
            if (operationScope is not null)
            {
                written += Encoding.UTF8.GetBytes(operationScope, stage[written..]);
                stage[written++] = (byte)'\n';
            }

            written += Encoding.UTF8.GetBytes(repoId, stage[written..]);
            stage[written++] = (byte)'\n';
            chunkIndex.TryFormat(stage[written..], out var indexBytes, default, CultureInfo.InvariantCulture);
            written += indexBytes;

            foreach (var upsert in upserts)
            {
                "\nU"u8.CopyTo(stage[written..]);
                written += 2;
                written += Encoding.UTF8.GetBytes(upsert.Key, stage[written..]);
                stage[written++] = (byte)'=';
                written += FileDigest.ComputeUtf8(upsert.Value, stage[written..]);
            }

            foreach (var delete in deletes)
            {
                "\nD"u8.CopyTo(stage[written..]);
                written += 2;
                written += Encoding.UTF8.GetBytes(delete, stage[written..]);
            }

            Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(stage[..written], hash);

            Span<char> id = stackalloc char[MaxPrefixChars + (IdHashBytes * 2)];
            prefix.CopyTo(id);
            Convert.TryToHexStringLower(hash[..IdHashBytes], id[prefix.Length..], out var hexChars);
            return new string(id[..(prefix.Length + hexChars)]);
        }
        finally
        {
            if (rented is not null)
            {
                // Record keys carry repository paths, so the written prefix is
                // cleared - but only that prefix, never the pool's round-up slack.
                rented.AsSpan(0, written).Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Bounds the UTF-8 stream the chunk will stage. Every term is arithmetic over
    /// a string's <c>Length</c>, so no part is transcoded or even walked here - the
    /// bound is deliberately loose rather than exact, which is what keeps this a
    /// single pass over the parts instead of two.
    /// </summary>
    private static int StageCapacity(
        string? operationScope,
        string repoId,
        IReadOnlyList<KeyValuePair<string, byte[]>> upserts,
        IReadOnlyList<string> deletes)
    {
        long capacity = MaxChunkIndexBytes + 2;
        if (operationScope is not null)
        {
            capacity += Encoding.UTF8.GetMaxByteCount(operationScope.Length);
        }

        capacity += Encoding.UTF8.GetMaxByteCount(repoId.Length);
        foreach (var upsert in upserts)
        {
            capacity += 3 + Encoding.UTF8.GetMaxByteCount(upsert.Key.Length) + FileDigest.Utf8DigestBytes;
        }

        foreach (var delete in deletes)
        {
            capacity += 2 + Encoding.UTF8.GetMaxByteCount(delete.Length);
        }

        return capacity > Array.MaxLength
            ? throw new ArgumentOutOfRangeException(
                nameof(upserts),
                "The chunk's parts exceed the largest operation-id stream that can be staged.")
            : (int)capacity;
    }
}
