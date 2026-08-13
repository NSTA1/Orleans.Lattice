using System.Buffers.Binary;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The on-the-wire framing constants and low-level frame primitives for a
/// repository-context snapshot stream. A snapshot is a self-describing,
/// provider-agnostic byte stream:
/// <list type="number">
///   <item><description>an 8-byte ASCII <see cref="Magic"/> marker,</description></item>
///   <item><description>a little-endian <see cref="int"/> format version, then</description></item>
///   <item><description>zero or more length-prefixed record frames (a little-endian
///     <see cref="int"/> byte length followed by that many Orleans-serialized
///     <see cref="RepoContextSnapshotRecord"/> bytes), terminated by end of stream.</description></item>
/// </list>
/// The record payloads reuse the Orleans <c>[Alias]</c> wire format, so the
/// stream is stable across durability profiles and host versions; the header lets
/// a reader reject a foreign or future-incompatible stream before it decodes a
/// single record.
/// </summary>
internal static class RepoContextSnapshotFormat
{
    /// <summary>The 8-byte ASCII stream marker: <c>OLRCSNP1</c>.</summary>
    internal static ReadOnlySpan<byte> Magic => "OLRCSNP1"u8;

    /// <summary>The format version this build writes.</summary>
    internal const int CurrentVersion = 1;

    /// <summary>The oldest format version this build can read.</summary>
    internal const int MinReadableVersion = 1;

    /// <summary>Writes the stream header (magic marker followed by the format version).</summary>
    /// <param name="destination">The stream to write to. Must not be <see langword="null"/>.</param>
    /// <param name="version">The format version to stamp.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    internal static async ValueTask WriteHeaderAsync(
        Stream destination,
        int version,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);

        var header = new byte[Magic.Length + sizeof(int)];
        Magic.CopyTo(header);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(Magic.Length), version);
        await destination.WriteAsync(header, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads and validates the stream header, returning the stamped format
    /// version. Throws <see cref="InvalidDataException"/> when the marker is
    /// absent or the version is outside the readable range.
    /// </summary>
    /// <param name="source">The stream to read from. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The stamped format version.</returns>
    internal static async ValueTask<int> ReadHeaderAsync(
        Stream source,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);

        var header = new byte[Magic.Length + sizeof(int)];
        await ReadExactlyAsync(source, header, cancellationToken).ConfigureAwait(false);

        if (!header.AsSpan(0, Magic.Length).SequenceEqual(Magic))
        {
            throw new InvalidDataException(
                "The stream is not a repository-context snapshot (bad magic marker).");
        }

        var version = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(Magic.Length));
        if (version < MinReadableVersion || version > CurrentVersion)
        {
            throw new InvalidDataException(
                $"Unsupported repository-context snapshot format version {version}; " +
                $"this build reads versions {MinReadableVersion} through {CurrentVersion}.");
        }

        return version;
    }

    /// <summary>Writes a single length-prefixed record frame.</summary>
    /// <param name="destination">The stream to write to. Must not be <see langword="null"/>.</param>
    /// <param name="payload">The serialized record payload. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    internal static async ValueTask WriteFrameAsync(
        Stream destination,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(payload);

        var length = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, payload.Length);
        await destination.WriteAsync(length, cancellationToken).ConfigureAwait(false);
        await destination.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the next length-prefixed record frame, or <see langword="null"/> when
    /// the stream has reached a clean end (no more frames). Throws
    /// <see cref="InvalidDataException"/> on a truncated frame.
    /// </summary>
    /// <param name="source">The stream to read from. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The serialized record payload, or <see langword="null"/> at end of stream.</returns>
    internal static async ValueTask<byte[]?> ReadFrameAsync(
        Stream source,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);

        var lengthBuffer = new byte[sizeof(int)];
        var read = await ReadUpToAsync(source, lengthBuffer, cancellationToken).ConfigureAwait(false);
        if (read == 0)
        {
            return null;
        }

        if (read < lengthBuffer.Length)
        {
            throw new InvalidDataException("Truncated repository-context snapshot frame header.");
        }

        var length = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
        if (length < 0)
        {
            throw new InvalidDataException(
                $"Negative repository-context snapshot frame length ({length}).");
        }

        var payload = new byte[length];
        await ReadExactlyAsync(source, payload, cancellationToken).ConfigureAwait(false);
        return payload;
    }

    private static async ValueTask ReadExactlyAsync(
        Stream source,
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        var read = await ReadUpToAsync(source, buffer, cancellationToken).ConfigureAwait(false);
        if (read < buffer.Length)
        {
            throw new InvalidDataException("Unexpected end of repository-context snapshot stream.");
        }
    }

    private static async ValueTask<int> ReadUpToAsync(
        Stream source,
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        var total = 0;
        while (total < buffer.Length)
        {
            var read = await source
                .ReadAsync(buffer.AsMemory(total), cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
            {
                break;
            }

            total += read;
        }

        return total;
    }
}
