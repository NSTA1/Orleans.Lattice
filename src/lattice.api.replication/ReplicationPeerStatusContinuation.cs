using System.Buffers.Text;
using System.Globalization;
using System.Text;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Encodes and decodes the opaque continuation token of
/// <see cref="ILatticeReplicationStatus.GetPeerStatusAsync"/>. A token names the
/// key of the last row the caller was handed - its effective tree id (the id
/// the row was reported under), peer and direction - so resuming re-reads
/// strictly after it.
/// </summary>
/// <remarks>
/// A token is a client-supplied value, so it is treated as an assertion to
/// validate, never as authority: it only positions the scan, and every row after
/// it is still authorized before it is reported. Only a row the caller was
/// already shown is ever encoded, so a token cannot disclose a tree the caller
/// may not see. Malformed input is rejected with <see cref="ArgumentException"/>,
/// including a token of an earlier format version.
/// </remarks>
internal static class ReplicationPeerStatusContinuation
{
    /// <summary>The longest token accepted, bounding the work a hostile token can cause.</summary>
    public const int MaxTokenLength = 4096;

    // Version 2 dropped the tenant-rendering flag the version 1 cursor carried (#4000).
    private const string VersionPrefix = "2.";

    /// <summary>Encodes <paramref name="cursor"/> as an opaque token.</summary>
    /// <param name="cursor">The key of the last row returned.</param>
    /// <returns>The token.</returns>
    public static string Encode(in ReplicationPeerStatusCursor cursor)
    {
        var payload = string.Create(
            CultureInfo.InvariantCulture,
            $"{cursor.Tree.Length}:{cursor.Tree}{cursor.Peer.Length}:{cursor.Peer}{(int)cursor.Direction}");
        return VersionPrefix + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload));
    }

    /// <summary>
    /// Decodes <paramref name="token"/>, or returns <see langword="null"/> for a
    /// <see langword="null"/> or empty token (the first page).
    /// </summary>
    /// <param name="token">The token to decode.</param>
    /// <returns>The cursor, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="token"/> is malformed.</exception>
    public static ReplicationPeerStatusCursor? Decode(string? token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return null;
        }

        if (token.Length > MaxTokenLength
            || !token.StartsWith(VersionPrefix, StringComparison.Ordinal))
        {
            throw Malformed();
        }

        string payload;
        try
        {
            payload = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(token.AsSpan(VersionPrefix.Length)));
        }
        catch (FormatException)
        {
            throw Malformed();
        }

        var position = 0;
        if (!TryReadField(payload, ref position, out var tree)
            || tree.Length == 0
            || !TryReadField(payload, ref position, out var peer)
            || peer.Length == 0
            || payload.Length - position != 1)
        {
            throw Malformed();
        }

        var direction = payload[position] switch
        {
            '0' => ReplicationContactDirection.Outbound,
            '1' => ReplicationContactDirection.Inbound,
            _ => throw Malformed(),
        };
        return new ReplicationPeerStatusCursor(tree, peer, direction);
    }

    private static bool TryReadField(string payload, ref int position, out string value)
    {
        value = string.Empty;
        var colon = payload.IndexOf(':', position);
        if (colon <= position
            || !int.TryParse(
                payload.AsSpan(position, colon - position),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var length)
            || length > payload.Length - colon - 1)
        {
            return false;
        }

        value = payload.Substring(colon + 1, length);
        position = colon + 1 + length;
        return true;
    }

    private static ArgumentException Malformed() =>
        new("The replication peer-status continuation token is not valid.", "query");
}
