using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Caching.AzureBlob;

/// <summary>
/// Maps an <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>
/// key to a stable, collision-resistant, storage-legal blob name. Cache keys are
/// arbitrary caller-supplied strings that can contain characters an Azure blob
/// name forbids and can exceed the blob-name length limit, so each key is hashed
/// to a fixed 64-character lowercase-hex SHA-256 digest and the configured
/// <see cref="LatticeAzureBlobCacheOptions.KeyPrefix"/> is prepended verbatim.
/// </summary>
internal static class BlobCacheKeyMap
{
    // Keys up to this UTF-8 byte length are hashed on the stack; longer keys
    // (rare) rent from the shared pool. Neither path allocates a byte[] on the
    // hot path.
    private const int StackHashThresholdBytes = 512;

    // UTF-8 emits at most three bytes per char (a surrogate pair costs four
    // bytes for two chars, so the per-char bound holds), so a key no longer than
    // this cannot overflow the stack buffer whatever it contains. Below it the
    // buffer choice is settled by the key's length alone and the separate
    // GetByteCount pass - a full scan of the key, on the hot path of every cache
    // read and write - is skipped outright.
    private const int StackHashThresholdChars = StackHashThresholdBytes / 3;

    /// <summary>
    /// Returns the blob name that backs <paramref name="key"/> under
    /// <paramref name="keyPrefix"/>. The prefix is concatenated verbatim (include
    /// a trailing slash for a virtual directory); the key contributes a 64-char
    /// lowercase-hex SHA-256 digest.
    /// <para>
    /// A key of at most <see cref="StackHashThresholdChars"/> chars is transcoded
    /// exactly once: its worst case already fits the stack buffer, so the count
    /// pass that used to pick the buffer is not needed and the encoder's written
    /// count bounds the hash instead. A longer key keeps the two-pass shape, so
    /// every key lands in the same buffer it did before and the digest is
    /// byte-identical either way.
    /// </para>
    /// </summary>
    /// <param name="keyPrefix">The configured key prefix (may be empty).</param>
    /// <param name="key">The cache key. Must not be <see langword="null"/>.</param>
    /// <returns>The blob name.</returns>
    public static string ToBlobName(string keyPrefix, string key)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);
        ArgumentNullException.ThrowIfNull(key);

        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];

        // -1 marks "short enough that the count is not worth taking"; it selects
        // the stack buffer exactly as a real count below the threshold would.
        var byteCount = key.Length <= StackHashThresholdChars
            ? -1
            : Encoding.UTF8.GetByteCount(key);

        byte[]? rented = null;
        try
        {
            Span<byte> buffer = byteCount <= StackHashThresholdBytes
                ? stackalloc byte[StackHashThresholdBytes]
                : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

            var written = Encoding.UTF8.GetBytes(key, buffer);
            SHA256.HashData(buffer[..written], digest);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        var hex = Convert.ToHexStringLower(digest);
        return keyPrefix.Length == 0 ? hex : string.Concat(keyPrefix, hex);
    }
}
