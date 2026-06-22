namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Validates a user-entered state-API endpoint before it is persisted or used to
/// build a connection.
/// </summary>
public static class EndpointValidation
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="endpoint"/> is a
    /// well-formed absolute <c>http</c>/<c>https</c> URL. On failure,
    /// <paramref name="error"/> carries a user-facing message.
    /// </summary>
    public static bool TryValidate(string? endpoint, out string? error)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            error = "Enter the state-API endpoint, for example https://host:443.";
            return false;
        }

        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            error = "Enter an absolute http:// or https:// URL.";
            return false;
        }

        error = null;
        return true;
    }
}
