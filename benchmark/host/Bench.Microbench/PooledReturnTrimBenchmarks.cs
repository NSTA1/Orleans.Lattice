using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Security.Cryptography;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three independent pooled-buffer and digest-staging trims so each one's
/// per-call time and byte delta is measurable in the clear, with no Orleans cluster
/// and no indexer in the loop. It is the direct sibling of
/// <see cref="RepoContextHashStagingBenchmarks"/> and follows the same convention:
/// every production shape is reproduced here rather than called, because all of
/// them sit behind <see langword="internal"/> or <see langword="private"/> members
/// of packages the microbench host does not reference. Each reproduction is
/// byte-identical to production, so the reported delta is precisely what the change
/// removes.
/// <para>
/// The pairs mirror the shipped edits verbatim:
/// (1) <b>Pooled return over-clear.</b> Every site that rents a UTF-8 staging
/// buffer from <c>ArrayPool</c> sized by <c>GetMaxByteCount</c> and returns it with
/// <c>clearArray: true</c> memsets the <em>whole rounded-up rental</em>, not the
/// bytes it wrote. For ASCII input that is <c>3n+3</c> rounded up to a power of
/// two against <c>n</c> written, so between three and six times the necessary work,
/// on a path whose real job is a hash of the written prefix. The shipped lane
/// clears exactly the written prefix and returns the array unflagged. This pair is
/// parameterized on input size because the trim's whole character is that it scales
/// with the rental, not with a fixed per-call constant;
/// (2) <b>Operation id staging.</b> The three repository-context reconcilers each
/// derived a chunk's saga idempotency key by staging every part into a
/// <see cref="StringBuilder"/>, materialising it, re-encoding the whole string as
/// UTF-8 and hashing that - plus one digest string per upsert. The shipped lane
/// transcodes the same parts straight into one pooled UTF-8 buffer, sized by a
/// single arithmetic pass over the part lengths, and hashes it once. A third lane
/// carries the shape that was <em>rejected</em> - folding the parts into an
/// <see cref="IncrementalHash"/> - because it removes the identical allocations
/// and is nonetheless far slower, which is the only reason the shipped lane looks
/// the way it does. Parameterized on upserts per chunk;
/// (3) <b>Digest comparison and formatting.</b> <c>FileDigest.Matches</c> ran once
/// per file on every reconcile walk and materialised the recomputed digest as a
/// string only to compare and discard it; the shipped lane formats into a stack
/// buffer and compares spans. <c>FileDigest.Compute</c> built its tagged digest as
/// a prefix concatenated with a separately-allocated hex string - two allocations
/// and a copy - where one <c>new string(ReadOnlySpan&lt;char&gt;)</c> over a stack
/// buffer does it in one.
/// </para>
/// <para>
/// Both lanes of every pair build the identical inputs and pay the identical
/// dispatch, so the sole per-lane difference is the work under test.
/// <see cref="Setup"/> asserts that each pair's two lanes agree on the exact
/// output at every parameter, so a lane that stopped computing the same thing
/// fails the run rather than reporting a cheaper number. Pair (1) additionally
/// carries a below-threshold parameter whose rental is small, which is the control
/// for the over-clear thesis: the saving must shrink with the rental rather than
/// appearing uniformly.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=pooledreturntrims</c> (or
/// <c>--suite pooledreturntrims</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class PooledReturnTrimBenchmarks
{
    /// <summary>Mirrors the private <c>FileDigest.StackTranscodeBytes</c>.</summary>
    private const int StackTranscodeBytes = 512;

    /// <summary>Mirrors the private <c>FileDigest.XxHash128Prefix</c>.</summary>
    private const string XxHash128Prefix = "xx128:";

    /// <summary>Mirrors the private <c>FileDigest.MaxDigestChars</c>.</summary>
    private const int MaxDigestChars = 7 + (SHA256.HashSizeInBytes * 2);

    /// <summary>Mirrors the internal <c>FileDigest.Utf8DigestBytes</c>.</summary>
    private const int Utf8DigestBytes = 6 + 32;

    /// <summary>Mirrors the private <c>RepoContextOperationId.IdHashBytes</c>.</summary>
    private const int IdHashBytes = 16;

    /// <summary>The repository identifier every operation-id lane folds in.</summary>
    private const string RepoId = "lattice";

    /// <summary>Staging inputs keyed by the char length the over-clear lanes are parameterized on.</summary>
    private readonly Dictionary<int, string> _stagingText = [];

    /// <summary>Chunk upserts keyed by the upsert count the operation-id lanes are parameterized on.</summary>
    private readonly Dictionary<int, KeyValuePair<string, byte[]>[]> _upserts = [];

    /// <summary>Chunk deletes keyed by the upsert count the operation-id lanes are parameterized on.</summary>
    private readonly Dictionary<int, string[]> _deletes = [];

    /// <summary>File bodies keyed by the file count the digest lanes are parameterized on.</summary>
    private readonly Dictionary<int, byte[][]> _files = [];

    /// <summary>Stored digests keyed by the file count the comparison lanes are parameterized on.</summary>
    private readonly Dictionary<int, string[]> _storedDigests = [];

    /// <summary>
    /// Builds the per-parameter inputs and asserts that both lanes of all three
    /// pairs produce the identical output, so the measured lanes are known to be
    /// computing the same function before any timing is reported.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        foreach (var length in new[] { 128, 4096, 65536 })
        {
            _stagingText[length] = BuildText(length);
        }

        foreach (var upsertCount in new[] { 16, 256 })
        {
            var upserts = new KeyValuePair<string, byte[]>[upsertCount];
            var deletes = new string[Math.Max(1, upsertCount / 8)];
            for (var i = 0; i < upsertCount; i++)
            {
                var key = string.Create(
                    CultureInfo.InvariantCulture,
                    $"repo/lattice/file/src/lattice/BPlusTree/Grains/ShardRootGrain.Part{i:D4}.cs");
                var value = new byte[512];
                for (var b = 0; b < value.Length; b++)
                {
                    value[b] = (byte)((i + b) % 251);
                }

                upserts[i] = new KeyValuePair<string, byte[]>(key, value);
            }

            for (var i = 0; i < deletes.Length; i++)
            {
                deletes[i] = string.Create(
                    CultureInfo.InvariantCulture,
                    $"repo/lattice/file/src/lattice/Removed{i:D4}.cs");
            }

            _upserts[upsertCount] = upserts;
            _deletes[upsertCount] = deletes;
        }

        foreach (var fileCount in new[] { 256, 2048 })
        {
            var files = new byte[fileCount][];
            var digests = new string[fileCount];
            for (var i = 0; i < fileCount; i++)
            {
                var body = new byte[1024];
                for (var b = 0; b < body.Length; b++)
                {
                    body[b] = (byte)((i * 7) + b);
                }

                files[i] = body;
                digests[i] = ComputeDigestOptimized(body);
            }

            _files[fileCount] = files;
            _storedDigests[fileCount] = digests;
        }

        foreach (var length in new[] { 128, 4096, 65536 })
        {
            AssertEqual(
                PooledReturn_Baseline(length),
                PooledReturn_Optimized(length),
                $"pooled staging digest, {length} chars");
        }

        foreach (var upsertCount in new[] { 16, 256 })
        {
            AssertEqual(
                OperationId_Baseline(upsertCount),
                OperationId_Optimized(upsertCount),
                $"operation id, {upsertCount} upserts");
            AssertEqual(
                OperationId_Baseline(upsertCount),
                OperationId_Folded(upsertCount),
                $"operation id (folded), {upsertCount} upserts");
        }

        foreach (var fileCount in new[] { 256, 2048 })
        {
            AssertEqual(
                DigestMatch_Baseline(fileCount).ToString(CultureInfo.InvariantCulture),
                DigestMatch_Optimized(fileCount).ToString(CultureInfo.InvariantCulture),
                $"digest comparison, {fileCount} files");
            AssertEqual(
                DigestFormat_Baseline(fileCount),
                DigestFormat_Optimized(fileCount),
                $"digest formatting, {fileCount} files");
        }
    }

    private static string BuildText(int length) => string.Create(
        length,
        length,
        static (span, _) =>
        {
            for (var i = 0; i < span.Length; i++)
            {
                span[i] = (char)('A' + (i % 26));
            }
        });

    private static void AssertEqual(string baseline, string optimized, string what)
    {
        if (!string.Equals(baseline, optimized, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Lane equivalence failed for {what}: baseline '{baseline}' != optimized '{optimized}'.");
        }
    }

    // ------------------------------------------------------------------
    // (1) Pooled return over-clear
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - stage the text into a rental sized by
    /// <c>GetMaxByteCount</c>, hash the written prefix, then hand the array back
    /// with <c>clearArray: true</c>, which memsets the entire rounded-up rental.
    /// </summary>
    /// <param name="length">Characters staged per call.</param>
    [Benchmark(Description = "Pooled staging: Return(clearArray: true) (baseline)")]
    [Arguments(128)]
    [Arguments(4096)]
    [Arguments(65536)]
    public string PooledReturn_Baseline(int length) => StageAndDigestClearingWholeRental(_stagingText[length]);

    /// <summary>
    /// Shipped: clear exactly the bytes this call wrote, then return the array
    /// unflagged. Identical hygiene for our own data, without the pool's round-up
    /// slack in the memset.
    /// </summary>
    /// <param name="length">Characters staged per call.</param>
    [Benchmark(Description = "Pooled staging: clear written prefix (optimized)")]
    [Arguments(128)]
    [Arguments(4096)]
    [Arguments(65536)]
    public string PooledReturn_Optimized(int length) => StageAndDigestClearingWrittenPrefix(_stagingText[length]);

    private static string StageAndDigestClearingWholeRental(string text)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(text.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackTranscodeBytes
            ? stackalloc byte[StackTranscodeBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        try
        {
            var written = Encoding.UTF8.GetBytes(text, buffer);
            return ComputeDigestOptimized(buffer[..written]);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented, clearArray: true);
            }
        }
    }

    private static string StageAndDigestClearingWrittenPrefix(string text)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(text.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackTranscodeBytes
            ? stackalloc byte[StackTranscodeBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        var written = 0;
        try
        {
            written = Encoding.UTF8.GetBytes(text, buffer);
            return ComputeDigestOptimized(buffer[..written]);
        }
        finally
        {
            if (rented is not null)
            {
                rented.AsSpan(0, written).Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    // ------------------------------------------------------------------
    // (2) Operation id staging
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape every reconciler carried - stage each part into a
    /// <see cref="StringBuilder"/>, materialise it, re-encode the whole string as
    /// UTF-8 and hash that, with one digest string allocated per upsert.
    /// </summary>
    /// <param name="upsertCount">Upserts in the chunk.</param>
    [Benchmark(Description = "Operation id: StringBuilder staging (baseline)")]
    [Arguments(16)]
    [Arguments(256)]
    public string OperationId_Baseline(int upsertCount)
    {
        var upserts = _upserts[upsertCount];
        var deletes = _deletes[upsertCount];
        var builder = new StringBuilder();
        builder.Append(RepoId).Append('\n').Append(0);
        for (var i = 0; i < upserts.Length; i++)
        {
            builder.Append("\nU").Append(upserts[i].Key).Append('=').Append(ComputeDigestOptimized(upserts[i].Value));
        }

        for (var i = 0; i < deletes.Length; i++)
        {
            builder.Append("\nD").Append(deletes[i]);
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return "rcb-" + Convert.ToHexStringLower(hash.AsSpan(0, IdHashBytes));
    }

    /// <summary>
    /// Rejected: fold the same parts into an <see cref="IncrementalHash"/>. It
    /// removes the identical allocations, but costs a managed-to-platform
    /// transition per part. This lane is kept because that cost is the entire
    /// reason the shipped lane stages instead, and a rationale nothing measures is
    /// a rationale nobody can check.
    /// </summary>
    /// <param name="upsertCount">Upserts in the chunk.</param>
    [Benchmark(Description = "Operation id: IncrementalHash fold (rejected)")]
    [Arguments(16)]
    [Arguments(256)]
    public string OperationId_Folded(int upsertCount)
    {
        var upserts = _upserts[upsertCount];
        var deletes = _deletes[upsertCount];
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendText(hasher, RepoId);
        hasher.AppendData("\n"u8);
        Span<byte> digits = stackalloc byte[16];
        0.TryFormat(digits, out var digitCount, default, CultureInfo.InvariantCulture);
        hasher.AppendData(digits[..digitCount]);

        Span<byte> digest = stackalloc byte[Utf8DigestBytes];
        for (var i = 0; i < upserts.Length; i++)
        {
            hasher.AppendData("\nU"u8);
            AppendText(hasher, upserts[i].Key);
            hasher.AppendData("="u8);
            hasher.AppendData(digest[..ComputeDigestUtf8(upserts[i].Value, digest)]);
        }

        for (var i = 0; i < deletes.Length; i++)
        {
            hasher.AppendData("\nD"u8);
            AppendText(hasher, deletes[i]);
        }

        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        hasher.GetHashAndReset(hash);
        Span<char> id = stackalloc char[8 + (IdHashBytes * 2)];
        "rcb-".CopyTo(id);
        Convert.TryToHexStringLower(hash[..IdHashBytes], id[4..], out var hexChars);
        return new string(id[..(4 + hexChars)]);
    }

    /// <summary>
    /// Shipped: transcode every part into one pooled UTF-8 buffer, sized by a
    /// single arithmetic pass over the part lengths, and hash it exactly once.
    /// </summary>
    /// <param name="upsertCount">Upserts in the chunk.</param>
    [Benchmark(Description = "Operation id: pooled stage, hash once (optimized)")]
    [Arguments(16)]
    [Arguments(256)]
    public string OperationId_Optimized(int upsertCount)
    {
        var upserts = _upserts[upsertCount];
        var deletes = _deletes[upsertCount];

        long bound = 16 + 2 + Encoding.UTF8.GetMaxByteCount(RepoId.Length);
        for (var i = 0; i < upserts.Length; i++)
        {
            bound += 3 + Encoding.UTF8.GetMaxByteCount(upserts[i].Key.Length) + Utf8DigestBytes;
        }

        for (var i = 0; i < deletes.Length; i++)
        {
            bound += 2 + Encoding.UTF8.GetMaxByteCount(deletes[i].Length);
        }

        byte[]? rented = null;
        var stage = bound <= StackTranscodeBytes
            ? stackalloc byte[StackTranscodeBytes]
            : (rented = ArrayPool<byte>.Shared.Rent((int)bound));
        var written = 0;
        try
        {
            written += Encoding.UTF8.GetBytes(RepoId, stage[written..]);
            stage[written++] = (byte)'\n';
            0.TryFormat(stage[written..], out var indexBytes, default, CultureInfo.InvariantCulture);
            written += indexBytes;

            for (var i = 0; i < upserts.Length; i++)
            {
                "\nU"u8.CopyTo(stage[written..]);
                written += 2;
                written += Encoding.UTF8.GetBytes(upserts[i].Key, stage[written..]);
                stage[written++] = (byte)'=';
                written += ComputeDigestUtf8(upserts[i].Value, stage[written..]);
            }

            for (var i = 0; i < deletes.Length; i++)
            {
                "\nD"u8.CopyTo(stage[written..]);
                written += 2;
                written += Encoding.UTF8.GetBytes(deletes[i], stage[written..]);
            }

            Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(stage[..written], hash);
            Span<char> id = stackalloc char[8 + (IdHashBytes * 2)];
            "rcb-".CopyTo(id);
            Convert.TryToHexStringLower(hash[..IdHashBytes], id[4..], out var hexChars);
            return new string(id[..(4 + hexChars)]);
        }
        finally
        {
            if (rented is not null)
            {
                rented.AsSpan(0, written).Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static void AppendText(IncrementalHash hasher, string text)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(text.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackTranscodeBytes
            ? stackalloc byte[StackTranscodeBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        var written = 0;
        try
        {
            written = Encoding.UTF8.GetBytes(text, buffer);
            hasher.AppendData(buffer[..written]);
        }
        finally
        {
            if (rented is not null)
            {
                rented.AsSpan(0, written).Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    // ------------------------------------------------------------------
    // (3) Digest comparison and formatting
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape of the reconcile walk's per-file freshness check -
    /// recompute the digest as a string and compare the two strings.
    /// </summary>
    /// <param name="fileCount">Files the walk visits.</param>
    [Benchmark(Description = "Digest compare: recompute as string (baseline)")]
    [Arguments(256)]
    [Arguments(2048)]
    public int DigestMatch_Baseline(int fileCount)
    {
        var files = _files[fileCount];
        var stored = _storedDigests[fileCount];
        var matched = 0;
        for (var i = 0; i < files.Length; i++)
        {
            if (string.Equals(ComputeDigestBaseline(files[i]), stored[i], StringComparison.Ordinal))
            {
                matched++;
            }
        }

        return matched;
    }

    /// <summary>
    /// Shipped: format the recomputed digest into a stack buffer and compare spans,
    /// so the walk allocates nothing per file.
    /// </summary>
    /// <param name="fileCount">Files the walk visits.</param>
    [Benchmark(Description = "Digest compare: span compare (optimized)")]
    [Arguments(256)]
    [Arguments(2048)]
    public int DigestMatch_Optimized(int fileCount)
    {
        var files = _files[fileCount];
        var stored = _storedDigests[fileCount];
        var matched = 0;
        for (var i = 0; i < files.Length; i++)
        {
            if (DigestMatches(stored[i], files[i]))
            {
                matched++;
            }
        }

        return matched;
    }

    /// <summary>
    /// Baseline: the prior tagged-digest formatting - a hex string allocated on its
    /// own, then concatenated onto the algorithm tag.
    /// </summary>
    /// <param name="fileCount">Digests formatted per call.</param>
    [Benchmark(Description = "Digest format: prefix + ToHexStringLower (baseline)")]
    [Arguments(256)]
    [Arguments(2048)]
    public string DigestFormat_Baseline(int fileCount)
    {
        var files = _files[fileCount];
        var last = string.Empty;
        for (var i = 0; i < files.Length; i++)
        {
            last = ComputeDigestBaseline(files[i]);
        }

        return last;
    }

    /// <summary>
    /// Shipped: format the tag and the hex into one stack buffer and materialise
    /// the result once.
    /// </summary>
    /// <param name="fileCount">Digests formatted per call.</param>
    [Benchmark(Description = "Digest format: single stack-formatted string (optimized)")]
    [Arguments(256)]
    [Arguments(2048)]
    public string DigestFormat_Optimized(int fileCount)
    {
        var files = _files[fileCount];
        var last = string.Empty;
        for (var i = 0; i < files.Length; i++)
        {
            last = ComputeDigestOptimized(files[i]);
        }

        return last;
    }

    private static string ComputeDigestBaseline(ReadOnlySpan<byte> content)
    {
        Span<byte> hash = stackalloc byte[16];
        XxHash128.Hash(content, hash);
        return XxHash128Prefix + Convert.ToHexStringLower(hash);
    }

    private static string ComputeDigestOptimized(ReadOnlySpan<byte> content)
    {
        Span<char> text = stackalloc char[MaxDigestChars];
        return new string(text[..FormatDigest(content, text)]);
    }

    private static int ComputeDigestUtf8(ReadOnlySpan<byte> content, Span<byte> destination)
    {
        Span<char> text = stackalloc char[Utf8DigestBytes];
        var length = FormatDigest(content, text);
        for (var i = 0; i < length; i++)
        {
            destination[i] = (byte)text[i];
        }

        return length;
    }

    private static int FormatDigest(ReadOnlySpan<byte> content, Span<char> destination)
    {
        Span<byte> hash = stackalloc byte[16];
        XxHash128.Hash(content, hash);
        XxHash128Prefix.CopyTo(destination);
        Convert.TryToHexStringLower(hash, destination[XxHash128Prefix.Length..], out var hexChars);
        return XxHash128Prefix.Length + hexChars;
    }

    private static bool DigestMatches(string storedDigest, ReadOnlySpan<byte> content)
    {
        if (storedDigest.Length > MaxDigestChars)
        {
            return false;
        }

        Span<char> recomputed = stackalloc char[MaxDigestChars];
        var length = FormatDigest(content, recomputed);
        return storedDigest.AsSpan().SequenceEqual(recomputed[..length]);
    }
}
