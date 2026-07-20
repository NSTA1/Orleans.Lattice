namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Validates a Microsoft Graph <c>@odata.nextLink</c> continuation token before it
/// is replayed as an absolute request URL. The continuation token flows in from the
/// wire (an admin-supplied <c>ContinuationToken</c>) and is otherwise handed straight
/// to the Graph SDK's <c>WithUrl</c>, so a tampered value could coerce the app-only
/// Graph client into issuing a blind outbound GET to an attacker-chosen host
/// (a server-side request forgery / confused-deputy). A genuine Graph next-link is
/// always an absolute HTTPS URL on the same host as the configured Graph endpoint, so
/// this validator rejects anything else and lets the caller degrade to an empty page
/// without ever issuing the request.
/// </summary>
internal static class GraphContinuationToken
{
    /// <summary>
    /// Determines whether <paramref name="token"/> is a legitimate Microsoft Graph
    /// pagination cursor that is safe to replay against <paramref name="graphBaseUrl"/>.
    /// </summary>
    /// <param name="token">The continuation token supplied by the caller.</param>
    /// <param name="graphBaseUrl">
    /// The configured Graph endpoint base URL (e.g. <c>https://graph.microsoft.com/v1.0</c>),
    /// used to derive the expected host. A <c>null</c> value fails every token closed.
    /// </param>
    /// <returns>
    /// <c>true</c> only when the token parses as an absolute <c>https</c> URI whose host
    /// matches the Graph base URL's host (case-insensitive); otherwise <c>false</c>.
    /// </returns>
    public static bool IsValid(string? token, Uri? graphBaseUrl)
    {
        if (string.IsNullOrEmpty(token))
        {
            return false;
        }

        if (graphBaseUrl is null || !graphBaseUrl.IsAbsoluteUri || string.IsNullOrEmpty(graphBaseUrl.Host))
        {
            return false;
        }

        if (!Uri.TryCreate(token, UriKind.Absolute, out var tokenUri))
        {
            return false;
        }

        if (!string.Equals(tokenUri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return string.Equals(tokenUri.Host, graphBaseUrl.Host, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Parses a Graph base URL string into a <see cref="Uri"/>, or returns <c>null</c>
    /// when it is missing or unparseable so callers fail closed.
    /// </summary>
    /// <param name="baseUrl">The Graph client's configured base URL.</param>
    /// <returns>The parsed absolute URI, or <c>null</c>.</returns>
    public static Uri? ParseGraphBaseUrl(string? baseUrl)
    {
        if (string.IsNullOrEmpty(baseUrl))
        {
            return null;
        }

        return Uri.TryCreate(baseUrl, UriKind.Absolute, out var uri) ? uri : null;
    }
}
