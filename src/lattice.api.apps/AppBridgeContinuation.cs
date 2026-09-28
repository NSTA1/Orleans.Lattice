namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The app bridge's scan continuation: the first key of the next page, behind a version marker. It carries
/// app data the caller may already read and nothing else - never a tree id, a cursor or any server state - so
/// it is stateless and cannot be replayed against anything the caller could not already address.
/// </summary>
internal static class AppBridgeContinuation
{
    /// <summary>The version marker every continuation starts with.</summary>
    public const string Marker = "k1:";

    /// <summary>Encodes the first key of the next page.</summary>
    /// <param name="nextKey">The first key of the next page.</param>
    /// <returns>The continuation.</returns>
    public static string Encode(string nextKey) => string.Concat(Marker, nextKey);

    /// <summary>
    /// Decodes a continuation for a scan of <paramref name="prefix"/>. A continuation that is malformed, too
    /// long, or names a key outside the prefix is refused, so it can never widen the scan.
    /// </summary>
    /// <param name="continuation">The continuation.</param>
    /// <param name="prefix">The scan's prefix.</param>
    /// <param name="startKey">The first key of the page on success.</param>
    /// <returns>Whether the continuation is valid for the prefix.</returns>
    public static bool TryDecode(string continuation, string prefix, out string startKey)
    {
        startKey = string.Empty;
        if (continuation.Length <= Marker.Length
            || continuation.Length > AppBridgeLimits.MaxContinuationLength
            || !continuation.StartsWith(Marker, StringComparison.Ordinal))
        {
            return false;
        }

        var key = continuation.AsSpan(Marker.Length);
        if (key.Length > AppBridgeLimits.MaxKeyLength || !key.StartsWith(prefix, StringComparison.Ordinal))
        {
            return false;
        }

        startKey = continuation[Marker.Length..];
        return true;
    }
}
