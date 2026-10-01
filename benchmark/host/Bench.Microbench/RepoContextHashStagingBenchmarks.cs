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
/// Isolates the four repository-context hash-staging allocation trims so their
/// per-call byte deltas are measurable in the clear with no Orleans cluster and
/// no indexer in the loop. It is the repository-context sibling of
/// <see cref="IdentityDigestAllocationBenchmarks"/> and follows the same
/// convention: each production shape is reproduced here rather than called,
/// because all four sit behind <see langword="internal"/> members of a package
/// the microbench host does not reference, and each reproduction is
/// byte-identical to production, so the <c>Allocated</c> delta is precisely the
/// heap the change removes.
/// <para>
/// The four pairs mirror the production edits verbatim:
/// (1) <c>VectorCodec.SourceId</c> - the per-source identifier a membership or
/// coverage probe derives once for every source it checks. The baseline encodes
/// the record key with <c>Encoding.UTF8.GetBytes</c> into a throwaway array; the
/// shipped lane transcodes into a constant stack buffer and hashes from that;
/// (2) <c>RepoContextReuse.ContentHash</c> - the file-version hash the context
/// bundler computes per delivered file, where the throwaway array is the size of
/// the whole body;
/// (3) <c>RepoContextReuse.Receipt</c> and <c>PossessionToken</c> - the per-unit
/// reuse tokens. The baseline stages the receipt's five parts as an interpolated
/// string and re-encodes it, and builds the possession token through
/// <c>char.ToString()</c>; the shipped lane writes the parts straight to UTF-8
/// and composes the token with <c>string.Create</c>;
/// (4) <c>CSharpSymbolExtractor.Build</c> - the declaration digest taken for
/// every extracted symbol during ingest. The baseline materialises the
/// declaration a second time as a string (<c>node.ToString()</c>) and a third
/// time as its UTF-8 encoding; the shipped lane digests the extractor's own
/// slice of the file text in place.
/// </para>
/// <para>
/// Pairs (2) and (4) are threshold-gated on a constant stack budget, so each
/// carries an above-threshold parameter that drives the pooled path as well as
/// the stack path. Pairs (1) and (3) are parameterized on the per-pass work item
/// count instead, because every realistic input to those two sits inside the
/// budget and the question they answer is how a fixed per-call saving scales.
/// Both lanes of every pair build the identical inputs, so the sole per-lane
/// difference is the heap under test. <see cref="Setup"/> asserts that each
/// pair's two lanes agree on the exact digest or token string at every
/// parameter, so a lane that stopped computing the same thing fails the run
/// rather than reporting a cheaper number.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=repocontexthashstaging</c> (or
/// <c>--suite repocontexthashstaging</c>); see <c>Program.cs</c>. The suite has
/// no Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class RepoContextHashStagingBenchmarks
{
    /// <summary>Mirrors the private <c>VectorCodec.StackKeyBytes</c>.</summary>
    private const int StackKeyBytes = 512;

    /// <summary>Mirrors the private <c>RepoContextReuse.StackTranscodeBytes</c>.</summary>
    private const int StackReuseBytes = 1024;

    /// <summary>Mirrors the private <c>FileDigest.StackTranscodeBytes</c>.</summary>
    private const int StackDigestBytes = 512;

    /// <summary>Mirrors the private <c>RepoContextReuse.PossessionSeparator</c>.</summary>
    private const char PossessionSeparator = '\u0000';

    /// <summary>Mirrors the private <c>RepoContextReuse.SeparatorByte</c>.</summary>
    private const byte SeparatorByte = 0;

    /// <summary>Mirrors the private <c>RepoContextReuse.SeparatorCount</c>.</summary>
    private const int SeparatorCount = 4;

    /// <summary>Mirrors the private <c>FileDigest.XxHash128Prefix</c>.</summary>
    private const string XxHash128Prefix = "xx128:";

    /// <summary>Canonical record keys keyed by the source count the lanes are parameterized on.</summary>
    private readonly Dictionary<int, string[]> _sourceKeys = [];

    /// <summary>File bodies keyed by the char length the lanes are parameterized on.</summary>
    private readonly Dictionary<int, string> _bodies = [];

    /// <summary>Delivered-unit descriptors keyed by the unit count the lanes are parameterized on.</summary>
    private readonly Dictionary<int, (string Path, string Hash, string Kind, string UnitKey)[]> _units = [];

    /// <summary>The synthetic source file the declaration-digest lanes slice.</summary>
    private string _declarationSource = null!;

    /// <summary>Declaration spans keyed by the symbol count the lanes are parameterized on.</summary>
    private readonly Dictionary<int, (int Start, int Length)[]> _declarationSpans = [];

    /// <summary>The repository identifier every receipt lane digests.</summary>
    private const string RepoId = "lattice";

    /// <summary>
    /// Builds the per-parameter inputs and asserts that both lanes of all four
    /// pairs produce the identical string, so the measured lanes are known to be
    /// computing the same function before any timing is reported.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        foreach (var sourceCount in new[] { 64, 512 })
        {
            var keys = new string[sourceCount];
            for (var i = 0; i < sourceCount; i++)
            {
                keys[i] = string.Create(
                    CultureInfo.InvariantCulture,
                    $"repo/lattice/file/src/lattice/BPlusTree/Grains/ShardRootGrain.Part{i:D4}.cs");
            }

            _sourceKeys[sourceCount] = keys;
        }

        foreach (var length in new[] { 2048, 65536 })
        {
            _bodies[length] = BuildText(length);
        }

        foreach (var unitCount in new[] { 8, 64 })
        {
            var units = new (string, string, string, string)[unitCount];
            for (var i = 0; i < unitCount; i++)
            {
                units[i] = (
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"src/lattice.api.mcp.repocontext/Retrieval/RepoContextBundleService.Part{i:D3}.cs"),
                    Convert.ToHexStringLower(new byte[32]),
                    "outline",
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"Orleans.Lattice.Api.Mcp.RepoContext.RepoContextBundleService.Member{i:D3}"));
            }

            _units[unitCount] = units;
        }

        _declarationSource = BuildText(48 * 1024);
        foreach (var symbolCount in new[] { 16, 128 })
        {
            var spans = new (int, int)[symbolCount];
            for (var i = 0; i < symbolCount; i++)
            {
                // Declarations nest, so the extractor digests a handful of large
                // enclosing nodes as well as many small members: every eighth span
                // is a whole-type slice that exceeds the stack budget and rents.
                var length = i % 8 == 0 ? 6 * 1024 : 180 + (i % 5 * 60);
                var start = i * 97 % (_declarationSource.Length - length);
                spans[i] = (start, length);
            }

            _declarationSpans[symbolCount] = spans;
        }

        foreach (var sourceCount in new[] { 64, 512 })
        {
            AssertEqual(
                SourceId_Baseline(sourceCount),
                SourceId_Optimized(sourceCount),
                $"source id, {sourceCount} sources");
        }

        foreach (var length in new[] { 2048, 65536 })
        {
            AssertEqual(
                ContentHash_Baseline(length),
                ContentHash_Optimized(length),
                $"content hash, {length} chars");
        }

        foreach (var unitCount in new[] { 8, 64 })
        {
            AssertEqual(
                ReuseTokens_Baseline(unitCount),
                ReuseTokens_Optimized(unitCount),
                $"reuse tokens, {unitCount} units");
        }

        foreach (var symbolCount in new[] { 16, 128 })
        {
            AssertEqual(
                DeclarationDigest_Baseline(symbolCount),
                DeclarationDigest_Optimized(symbolCount),
                $"declaration digest, {symbolCount} symbols");
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
    // (1) VectorCodec.SourceId - per-source membership/coverage probe id
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - the record key encoded into a throwaway array
    /// by <c>Encoding.UTF8.GetBytes</c> for every source the probe loop visits.
    /// </summary>
    /// <param name="sourceCount">Sources visited in one probe pass.</param>
    [Benchmark(Description = "Source id: GetBytes array per source (baseline)")]
    [Arguments(64)]
    [Arguments(512)]
    public string SourceId_Baseline(int sourceCount)
    {
        var keys = _sourceKeys[sourceCount];
        var last = string.Empty;
        for (var i = 0; i < keys.Length; i++)
        {
            last = SourceIdBaseline(keys[i]);
        }

        return last;
    }

    /// <summary>
    /// Shipped: transcode the key into a constant-size stack buffer, renting only
    /// for a key that overflows it, and hash from that buffer.
    /// </summary>
    /// <param name="sourceCount">Sources visited in one probe pass.</param>
    [Benchmark(Description = "Source id: stack transcode (optimized)")]
    [Arguments(64)]
    [Arguments(512)]
    public string SourceId_Optimized(int sourceCount)
    {
        var keys = _sourceKeys[sourceCount];
        var last = string.Empty;
        for (var i = 0; i < keys.Length; i++)
        {
            last = SourceIdOptimized(keys[i]);
        }

        return last;
    }

    private static string SourceIdBaseline(string sourceKey)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(sourceKey), hash);
        return Convert.ToHexStringLower(hash[..8]);
    }

    private static string SourceIdOptimized(string sourceKey)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(sourceKey.Length);
        byte[]? rented = null;
        var key = maxBytes <= StackKeyBytes
            ? stackalloc byte[StackKeyBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        var written = 0;
        try
        {
            written = Encoding.UTF8.GetBytes(sourceKey, key);
            Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(key[..written], hash);
            return Convert.ToHexStringLower(hash[..8]);
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
    // (2) RepoContextReuse.ContentHash - per delivered file version
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - the whole file body re-encoded into a
    /// throwaway array the size of the body itself.
    /// </summary>
    /// <param name="length">The file body length in characters.</param>
    [Benchmark(Description = "Content hash: GetBytes array of whole body (baseline)")]
    [Arguments(2048)]
    [Arguments(65536)]
    public string ContentHash_Baseline(int length) => ContentHashBaseline(_bodies[length]);

    /// <summary>
    /// Shipped: transcode the body into a pooled buffer (the stack budget covers
    /// only a very short body) and hash from that buffer.
    /// </summary>
    /// <param name="length">The file body length in characters.</param>
    [Benchmark(Description = "Content hash: pooled transcode (optimized)")]
    [Arguments(2048)]
    [Arguments(65536)]
    public string ContentHash_Optimized(int length) => ContentHashOptimized(_bodies[length]);

    private static string ContentHashBaseline(string content)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(content), hash);
        return Convert.ToHexStringLower(hash);
    }

    private static string ContentHashOptimized(string content)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(content.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackReuseBytes
            ? stackalloc byte[StackReuseBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        var written = 0;
        try
        {
            written = Encoding.UTF8.GetBytes(content, buffer);
            Span<byte> hash = stackalloc byte[32];
            SHA256.HashData(buffer[..written], hash);
            return Convert.ToHexStringLower(hash);
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
    // (3) RepoContextReuse.Receipt / PossessionToken - per delivered unit
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - a five-part interpolated staging string per
    /// receipt, re-encoded into a throwaway array, plus a one-character string
    /// from <c>char.ToString()</c> per possession token.
    /// </summary>
    /// <param name="unitCount">Units delivered in one bundle.</param>
    [Benchmark(Description = "Reuse tokens: interpolated staging + GetBytes (baseline)")]
    [Arguments(8)]
    [Arguments(64)]
    public string ReuseTokens_Baseline(int unitCount)
    {
        var units = _units[unitCount];
        var last = string.Empty;
        for (var i = 0; i < units.Length; i++)
        {
            var unit = units[i];
            last = ReceiptBaseline(RepoId, unit.Path, unit.Hash, unit.Kind, unit.UnitKey)
                + PossessionTokenBaseline(unit.Path, unit.Hash);
        }

        return last;
    }

    /// <summary>
    /// Shipped: write the receipt's five parts straight to UTF-8 in one stack
    /// buffer, and compose the possession token with <c>string.Create</c>.
    /// </summary>
    /// <param name="unitCount">Units delivered in one bundle.</param>
    [Benchmark(Description = "Reuse tokens: direct UTF-8 write (optimized)")]
    [Arguments(8)]
    [Arguments(64)]
    public string ReuseTokens_Optimized(int unitCount)
    {
        var units = _units[unitCount];
        var last = string.Empty;
        for (var i = 0; i < units.Length; i++)
        {
            var unit = units[i];
            last = ReceiptOptimized(RepoId, unit.Path, unit.Hash, unit.Kind, unit.UnitKey)
                + PossessionTokenOptimized(unit.Path, unit.Hash);
        }

        return last;
    }

    private static string ReceiptBaseline(
        string repoId, string path, string contentHash, string kind, string unitKey)
    {
        var input = $"{repoId}{PossessionSeparator}{path}{PossessionSeparator}{contentHash}{PossessionSeparator}{kind}{PossessionSeparator}{unitKey}";
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(input), hash);
        return Convert.ToHexStringLower(hash);
    }

    private static string ReceiptOptimized(
        string repoId, string path, string contentHash, string kind, string unitKey)
    {
        var maxBytes =
            Encoding.UTF8.GetMaxByteCount(repoId.Length) +
            Encoding.UTF8.GetMaxByteCount(path.Length) +
            Encoding.UTF8.GetMaxByteCount(contentHash.Length) +
            Encoding.UTF8.GetMaxByteCount(kind.Length) +
            Encoding.UTF8.GetMaxByteCount(unitKey.Length) +
            SeparatorCount;
        byte[]? rented = null;
        var buffer = maxBytes <= StackReuseBytes
            ? stackalloc byte[StackReuseBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        var written = 0;
        try
        {
            written = Encoding.UTF8.GetBytes(repoId, buffer);
            buffer[written++] = SeparatorByte;
            written += Encoding.UTF8.GetBytes(path, buffer[written..]);
            buffer[written++] = SeparatorByte;
            written += Encoding.UTF8.GetBytes(contentHash, buffer[written..]);
            buffer[written++] = SeparatorByte;
            written += Encoding.UTF8.GetBytes(kind, buffer[written..]);
            buffer[written++] = SeparatorByte;
            written += Encoding.UTF8.GetBytes(unitKey, buffer[written..]);

            Span<byte> hash = stackalloc byte[32];
            SHA256.HashData(buffer[..written], hash);
            return Convert.ToHexStringLower(hash);
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

    private static string PossessionTokenBaseline(string path, string contentHash) =>
        string.Concat(path, PossessionSeparator.ToString(), contentHash);

    private static string PossessionTokenOptimized(string path, string contentHash) => string.Create(
        path.Length + 1 + contentHash.Length,
        (path, contentHash),
        static (destination, state) =>
        {
            state.path.CopyTo(destination);
            destination[state.path.Length] = PossessionSeparator;
            state.contentHash.CopyTo(destination[(state.path.Length + 1)..]);
        });

    // ------------------------------------------------------------------
    // (4) CSharpSymbolExtractor.Build - per extracted declaration digest
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - every declaration materialised a second time
    /// as a string (<c>node.ToString()</c>) and a third time as its UTF-8
    /// encoding, once per symbol and again for each enclosing declaration.
    /// </summary>
    /// <param name="symbolCount">Declarations extracted from one file.</param>
    [Benchmark(Description = "Declaration digest: ToString + GetBytes (baseline)")]
    [Arguments(16)]
    [Arguments(128)]
    public string DeclarationDigest_Baseline(int symbolCount)
    {
        var spans = _declarationSpans[symbolCount];
        var last = string.Empty;
        for (var i = 0; i < spans.Length; i++)
        {
            var declaration = _declarationSource.Substring(spans[i].Start, spans[i].Length);
            last = DigestBaseline(Encoding.UTF8.GetBytes(declaration));
        }

        return last;
    }

    /// <summary>
    /// Shipped: digest the extractor's own slice of the file text in place,
    /// transcoding through a stack buffer and renting only for a large enclosing
    /// declaration.
    /// </summary>
    /// <param name="symbolCount">Declarations extracted from one file.</param>
    [Benchmark(Description = "Declaration digest: slice in place (optimized)")]
    [Arguments(16)]
    [Arguments(128)]
    public string DeclarationDigest_Optimized(int symbolCount)
    {
        var spans = _declarationSpans[symbolCount];
        var last = string.Empty;
        for (var i = 0; i < spans.Length; i++)
        {
            last = DigestOptimized(_declarationSource.AsSpan(spans[i].Start, spans[i].Length));
        }

        return last;
    }

    private static string DigestBaseline(ReadOnlySpan<byte> content)
    {
        Span<byte> hash = stackalloc byte[16];
        XxHash128.Hash(content, hash);
        return XxHash128Prefix + Convert.ToHexStringLower(hash);
    }

    private static string DigestOptimized(ReadOnlySpan<char> text)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(text.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackDigestBytes
            ? stackalloc byte[StackDigestBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        var written = 0;
        try
        {
            written = Encoding.UTF8.GetBytes(text, buffer);
            return DigestBaseline(buffer[..written]);
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
}
