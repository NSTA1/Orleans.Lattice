using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three SHA-256 identity-digest allocation trims so their
/// per-call byte deltas are measurable in the clear with no Orleans cluster in
/// the loop. It is the SHA-256 sibling of
/// <see cref="HashingAllocationBenchmarks"/>, which covers the non-cryptographic
/// (<c>XxHash</c>) view-maintenance hashes, and follows the same convention:
/// each production shape is reproduced here rather than called, because all
/// three sit behind <see langword="internal"/> or <see langword="private"/>
/// members, and the reproduced digest is byte-identical to production, so the
/// <c>Allocated</c> delta is precisely the heap the change removes.
/// <para>
/// The three pairs mirror the production edits verbatim:
/// (1) <c>MembershipCacheKey.DigestMetadata</c> - the credential-cache key's
/// metadata digest, computed on every cache probe for a credential that carries
/// a metadata bag. The baseline stages the canonical form as a
/// <see cref="StringBuilder"/>, materialises it with <c>ToString</c>, and
/// re-encodes it with <c>Encoding.UTF8.GetBytes</c>; the shipped lane writes the
/// canonical form straight to UTF-8 and hashes from that buffer;
/// (2) <c>CookieCredentialStore.Digest</c> - the explorer's cookie revocation
/// identity, computed on every cookie-authenticated read. Same shape, one input
/// string rather than a bag;
/// (3) <c>BackupContentHash.ToHexLowerAndReset</c> - the throwaway 32-byte
/// digest array that the parameterless
/// <see cref="IncrementalHash.GetHashAndReset()"/> allocates for every hashed
/// backup artifact, read once by the hex formatter and then dropped.
/// </para>
/// <para>
/// Pairs (1) and (2) are threshold-gated on a constant stack budget, so each
/// carries an above-threshold parameter that drives the pooled path as well as
/// the stack path - the larger argument in each case. Both lanes of every pair
/// build the identical inputs, so the sole per-lane difference is the heap under
/// test. <see cref="Setup"/> asserts that each pair's two lanes agree on the
/// exact digest string at every parameter, so a lane that stopped computing the
/// same thing fails the run rather than reporting a cheaper number.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=identitydigestalloc</c> (or
/// <c>--suite identitydigestalloc</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class IdentityDigestAllocationBenchmarks
{
    /// <summary>The U+001F field separator, ASCII and so one UTF-8 byte.</summary>
    private const byte UnitSeparator = 0x1f;

    /// <summary>
    /// Mirrors the private <c>MembershipCacheKey.MaxLengthPrefixBytes</c>: an
    /// <see cref="int"/> is at most 11 decimal digits, and each of a pair's two
    /// length prefixes contributes those digits plus one separator byte.
    /// </summary>
    private const int MaxLengthPrefixBytes = (11 + 1) * 2;

    /// <summary>Mirrors the private <c>MembershipCacheKey.StackCanonicalBytes</c>.</summary>
    private const int StackCanonicalBytes = 256;

    /// <summary>Mirrors the private <c>CookieCredentialStore.StackCookieBytes</c>.</summary>
    private const int StackCookieBytes = 512;

    /// <summary>Metadata bags keyed by the entry count the lanes are parameterized on.</summary>
    private readonly Dictionary<int, Dictionary<string, string>> _metadataBags = [];

    /// <summary>Encrypted-cookie payloads keyed by the char length the lanes are parameterized on.</summary>
    private readonly Dictionary<int, string> _cookiePayloads = [];

    /// <summary>The backup artifact payload every artifact lane hashes.</summary>
    private byte[] _artifactChunk = null!;

    /// <summary>
    /// Builds the per-parameter inputs and asserts that both lanes of all three
    /// pairs produce the identical digest, so the measured lanes are known to be
    /// computing the same function before any timing is reported.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        foreach (var entryCount in new[] { 2, 24 })
        {
            var bag = new Dictionary<string, string>(entryCount);
            for (var i = 0; i < entryCount; i++)
            {
                bag["claim-" + i.ToString("D3", CultureInfo.InvariantCulture)] =
                    "value-" + i.ToString("D6", CultureInfo.InvariantCulture);
            }

            _metadataBags[entryCount] = bag;
        }

        foreach (var length in new[] { 256, 1536 })
        {
            _cookiePayloads[length] = string.Create(
                length,
                length,
                static (span, _) =>
                {
                    for (var i = 0; i < span.Length; i++)
                    {
                        span[i] = (char)('A' + (i % 26));
                    }
                });
        }

        _artifactChunk = new byte[4096];
        for (var i = 0; i < _artifactChunk.Length; i++)
        {
            _artifactChunk[i] = (byte)i;
        }

        foreach (var entryCount in new[] { 2, 24 })
        {
            AssertEqual(
                MetadataDigest_Baseline(entryCount),
                MetadataDigest_Optimized(entryCount),
                $"metadata digest, {entryCount} entries");
        }

        foreach (var length in new[] { 256, 1536 })
        {
            AssertEqual(
                CookieDigest_Baseline(length),
                CookieDigest_Optimized(length),
                $"cookie digest, {length} chars");
        }

        foreach (var artifactCount in new[] { 1, 64 })
        {
            AssertEqual(
                ArtifactDigest_Baseline(artifactCount),
                ArtifactDigest_Optimized(artifactCount),
                $"artifact digest, {artifactCount} artifacts");
        }
    }

    private static void AssertEqual(string baseline, string optimized, string what)
    {
        if (!string.Equals(baseline, optimized, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Lane equivalence failed for {what}: baseline '{baseline}' != optimized '{optimized}'.");
        }
    }

    // ------------------------------------------------------------------
    // (1) MembershipCacheKey.DigestMetadata - credential cache-probe digest
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - a <see cref="StringBuilder"/> canonical form,
    /// materialised with <c>ToString</c>, re-encoded with
    /// <c>Encoding.UTF8.GetBytes</c>, and hashed by the allocating
    /// <c>SHA256.HashData</c> overload. Four intermediates per probe that nothing
    /// outlives.
    /// </summary>
    /// <param name="entryCount">Metadata entries in the credential's bag.</param>
    [Benchmark(Description = "Metadata digest: StringBuilder + GetBytes (baseline)")]
    [Arguments(2)]
    [Arguments(24)]
    public string MetadataDigest_Baseline(int entryCount)
    {
        var pairs = SortedPairs(entryCount);

        var canonical = new StringBuilder();
        foreach (var pair in pairs)
        {
            canonical.Append(pair.Key.Length).Append('\u001f').Append(pair.Key)
                .Append(pair.Value.Length).Append('\u001f').Append(pair.Value);
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical.ToString())));
    }

    /// <summary>
    /// Optimized: the shipped shape - the canonical form written straight to
    /// UTF-8 in a stack (or pooled, above the constant budget) buffer and hashed
    /// into a stack digest span. The byte stream is unchanged.
    /// </summary>
    /// <param name="entryCount">Metadata entries in the credential's bag.</param>
    [Benchmark(Description = "Metadata digest: direct UTF-8 + stack digest (optimized)")]
    [Arguments(2)]
    [Arguments(24)]
    public string MetadataDigest_Optimized(int entryCount)
    {
        var pairs = SortedPairs(entryCount);

        var maxBytes = 0;
        foreach (var pair in pairs)
        {
            maxBytes += Encoding.UTF8.GetMaxByteCount(pair.Key.Length + pair.Value.Length)
                + MaxLengthPrefixBytes;
        }

        byte[]? rented = null;
        var canonical = maxBytes <= StackCanonicalBytes
            ? stackalloc byte[StackCanonicalBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        try
        {
            var written = 0;
            foreach (var pair in pairs)
            {
                written += WriteLengthPrefix(canonical[written..], pair.Key.Length);
                written += Encoding.UTF8.GetBytes(pair.Key, canonical[written..]);
                written += WriteLengthPrefix(canonical[written..], pair.Value.Length);
                written += Encoding.UTF8.GetBytes(pair.Value, canonical[written..]);
            }

            Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(canonical[..written], digest);
            return Convert.ToHexString(digest);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented, clearArray: true);
            }
        }
    }

    /// <summary>
    /// The ordering prologue both metadata lanes share, so the sole per-lane
    /// difference is the canonical encode rather than the sort.
    /// </summary>
    private List<KeyValuePair<string, string>> SortedPairs(int entryCount)
    {
        var pairs = new List<KeyValuePair<string, string>>(_metadataBags[entryCount]);
        pairs.Sort(static (left, right) =>
        {
            var byKey = string.CompareOrdinal(left.Key, right.Key);
            return byKey != 0 ? byKey : string.CompareOrdinal(left.Value, right.Value);
        });

        return pairs;
    }

    /// <summary>
    /// Mirrors the private <c>MembershipCacheKey.WriteLengthPrefix</c>: the
    /// decimal length followed by the U+001F separator, both ASCII.
    /// </summary>
    private static int WriteLengthPrefix(Span<byte> destination, int length)
    {
        length.TryFormat(destination, out var written);
        destination[written] = UnitSeparator;
        return written + 1;
    }

    // ------------------------------------------------------------------
    // (2) CookieCredentialStore.Digest - cookie revocation identity
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - <c>Encoding.UTF8.GetBytes(string)</c> feeding
    /// the allocating <c>SHA256.HashData</c> overload, so every
    /// cookie-authenticated read allocates the encode array and the digest array.
    /// </summary>
    /// <param name="payloadLength">Encrypted-cookie payload length in characters.</param>
    [Benchmark(Description = "Cookie digest: GetBytes + HashData byte[] (baseline)")]
    [Arguments(256)]
    [Arguments(1536)]
    public string CookieDigest_Baseline(int payloadLength) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(_cookiePayloads[payloadLength])));

    /// <summary>
    /// Optimized: the shipped shape - a stack buffer below the constant budget
    /// and a cleared pooled rental above it, hashed into a stack digest span.
    /// </summary>
    /// <param name="payloadLength">Encrypted-cookie payload length in characters.</param>
    [Benchmark(Description = "Cookie digest: stack/pooled + stack digest (optimized)")]
    [Arguments(256)]
    [Arguments(1536)]
    public string CookieDigest_Optimized(int payloadLength)
    {
        var cookieValue = _cookiePayloads[payloadLength];
        var maxBytes = Encoding.UTF8.GetMaxByteCount(cookieValue.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackCookieBytes
            ? stackalloc byte[StackCookieBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        try
        {
            var written = Encoding.UTF8.GetBytes(cookieValue, buffer);
            Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(buffer[..written], digest);
            return Convert.ToHexString(digest);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented, clearArray: true);
            }
        }
    }

    // ------------------------------------------------------------------
    // (3) BackupContentHash.ToHexLowerAndReset - per-artifact digest array
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior shape - the parameterless
    /// <see cref="IncrementalHash.GetHashAndReset()"/>, whose fresh 32-byte array
    /// is read once by the hex formatter and dropped, paid once per hashed
    /// artifact.
    /// </summary>
    /// <param name="artifactCount">Artifacts hashed in one capture or restore pass.</param>
    [Benchmark(Description = "Artifact digest: GetHashAndReset byte[] (baseline)")]
    [Arguments(1)]
    [Arguments(64)]
    public string ArtifactDigest_Baseline(int artifactCount)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var last = string.Empty;
        for (var i = 0; i < artifactCount; i++)
        {
            hasher.AppendData(_artifactChunk);
            last = Convert.ToHexStringLower(hasher.GetHashAndReset());
        }

        return last;
    }

    /// <summary>
    /// Optimized: the shipped shape - the digest filled into a stack span and
    /// formatted from it, so the per-artifact array is never allocated.
    /// </summary>
    /// <param name="artifactCount">Artifacts hashed in one capture or restore pass.</param>
    [Benchmark(Description = "Artifact digest: stack digest span (optimized)")]
    [Arguments(1)]
    [Arguments(64)]
    public string ArtifactDigest_Optimized(int artifactCount)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var last = string.Empty;
        for (var i = 0; i < artifactCount; i++)
        {
            hasher.AppendData(_artifactChunk);
            last = ToHexLowerAndReset(hasher);
        }

        return last;
    }

    /// <summary>
    /// Verbatim copy of the shipped <c>BackupContentHash.ToHexLowerAndReset</c>
    /// helper. The digest span is stack-allocated inside this method, exactly as
    /// it is in the shipped shape, so the lane measures one fresh frame per
    /// artifact rather than a <c>localloc</c> executed inside a loop body.
    /// </summary>
    /// <param name="hasher">The accumulating hasher.</param>
    /// <returns>The 64-character lowercase hexadecimal digest.</returns>
    private static string ToHexLowerAndReset(IncrementalHash hasher)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        hasher.GetHashAndReset(digest);
        return Convert.ToHexStringLower(digest);
    }
}
