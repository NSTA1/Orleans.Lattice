using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice;

/// <summary>
/// Unambiguous framing for the strings a deterministic fingerprint feeds through an
/// <see cref="IncrementalHash"/>: each value is prefixed with its encoded length,
/// so no two distinct sequences of values can hash the same bytes.
/// </summary>
internal static class IncrementalHashFraming
{
    private const int StackBufferBytes = 512;

    /// <summary>
    /// Appends a 4-byte little-endian length prefix followed by the UTF-8 bytes
    /// of <paramref name="value"/> to <paramref name="hash"/>, encoding through a
    /// stack buffer for short strings and renting from the array pool only for
    /// the rare long value.
    /// </summary>
    /// <param name="hash">The hash being accumulated.</param>
    /// <param name="value">The string to append.</param>
    /// <param name="lenPrefix">A caller-owned scratch span of at least four bytes for the prefix.</param>
    internal static void AppendLengthPrefixed(IncrementalHash hash, string value, Span<byte> lenPrefix)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = maxBytes > StackBufferBytes ? ArrayPool<byte>.Shared.Rent(maxBytes) : null;
        Span<byte> buffer = rented ?? stackalloc byte[StackBufferBytes];
        var written = Encoding.UTF8.GetBytes(value, buffer);
        BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, written);
        hash.AppendData(lenPrefix);
        hash.AppendData(buffer[..written]);
        if (rented is not null)
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
