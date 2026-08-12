using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The byte encoding and content addressing for a stored embedding vector. A
/// vector is persisted as the immutable payload bytes of a
/// <see cref="VectorPayloadRecord"/> under a content-addressed key, so the
/// encoding must be deterministic (identical components always yield identical
/// bytes and therefore an identical content address) and fully reversible.
/// <para>
/// Components are written little-endian, four bytes each, in order. The content
/// address is the lower-case hex SHA-256 of those bytes, which is what makes the
/// payload self-deduplicating: two embeddings with byte-identical components map
/// to the same key, and a re-embed that produces the same bytes is a no-op write.
/// </para>
/// </summary>
internal static class VectorCodec
{
    /// <summary>The size in bytes of a single encoded component.</summary>
    internal const int ComponentSize = sizeof(float);

    /// <summary>
    /// Encodes <paramref name="vector"/> into its deterministic little-endian byte
    /// form (four bytes per component, in order).
    /// </summary>
    /// <param name="vector">The vector components to encode.</param>
    /// <returns>The encoded payload bytes.</returns>
    internal static byte[] Encode(ReadOnlyMemory<float> vector)
    {
        var span = vector.Span;
        var bytes = new byte[span.Length * ComponentSize];
        for (var i = 0; i < span.Length; i++)
        {
            BinaryPrimitives.WriteSingleLittleEndian(
                bytes.AsSpan(i * ComponentSize, ComponentSize), span[i]);
        }

        return bytes;
    }

    /// <summary>
    /// Decodes payload bytes produced by <see cref="Encode(ReadOnlyMemory{float})"/>
    /// back into their component array.
    /// </summary>
    /// <param name="payload">The encoded payload bytes. Its length must be a
    /// multiple of <see cref="ComponentSize"/>.</param>
    /// <returns>The decoded vector components.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="payload"/> is null.</exception>
    /// <exception cref="ArgumentException">The payload length is not a whole number
    /// of components.</exception>
    internal static float[] Decode(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length % ComponentSize != 0)
        {
            throw new ArgumentException(
                $"An encoded vector payload must be a multiple of {ComponentSize} bytes.", nameof(payload));
        }

        var count = payload.Length / ComponentSize;
        var vector = new float[count];
        for (var i = 0; i < count; i++)
        {
            vector[i] = BinaryPrimitives.ReadSingleLittleEndian(
                payload.AsSpan(i * ComponentSize, ComponentSize));
        }

        return vector;
    }

    /// <summary>
    /// Computes the content address of <paramref name="payload"/>: the lower-case
    /// hex SHA-256 of the bytes. This is the stable key a payload is stored under,
    /// so byte-identical payloads deduplicate to one key.
    /// </summary>
    /// <param name="payload">The encoded payload bytes.</param>
    /// <returns>A 64-character lower-case hex content address.</returns>
    internal static string ContentAddress(ReadOnlySpan<byte> payload)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(payload, hash);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Derives the stable, per-repository source identifier for a canonical record
    /// key: the first 16 hex characters of the SHA-256 of the key. This keys a
    /// source's content-addressed vector-presence range so a re-embed of the same
    /// source can find and retire its prior embedding without a growing membership
    /// value.
    /// </summary>
    /// <param name="sourceKey">The canonical record key the vector derives from.</param>
    /// <returns>A 16-character lower-case hex source identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sourceKey"/> is null.</exception>
    internal static string SourceId(string sourceKey)
    {
        ArgumentNullException.ThrowIfNull(sourceKey);
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(sourceKey), hash);
        return Convert.ToHexStringLower(hash[..8]);
    }
}
