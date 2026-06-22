namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Enforces the explorer's transport-security rules over a configured endpoint
/// and its <see cref="ExplorerTransportMode"/>:
/// <list type="bullet">
/// <item><description>a non-loopback endpoint must use TLS (<c>https</c>);</description></item>
/// <item><description>anonymous access to a non-loopback endpoint is rejected by default;</description></item>
/// <item><description>the interim anonymous / plaintext path is gated behind the explicit
/// <see cref="ExplorerTransportMode.InsecureLoopbackDev"/> opt-in, which is itself
/// allowed only for loopback endpoints.</description></item>
/// </list>
/// </summary>
public static class TransportSecurityPolicy
{
    /// <summary>
    /// Validates the structural transport rules for an endpoint and mode, without
    /// regard to whether a credential is present. Used when an endpoint is
    /// entered or applied: it accepts a secure non-loopback endpoint that the user
    /// will sign in to, but rejects plaintext non-loopback endpoints and an
    /// insecure-loopback-dev opt-in pointed at a non-loopback host.
    /// </summary>
    /// <param name="endpoint">The endpoint URL.</param>
    /// <param name="mode">The configured transport mode.</param>
    /// <param name="error">A user-facing message when validation fails.</param>
    /// <returns><see langword="true"/> when the endpoint and mode are acceptable.</returns>
    public static bool TryValidateEndpoint(string? endpoint, ExplorerTransportMode mode, out string? error)
    {
        if (!EndpointValidation.TryValidate(endpoint, out error))
        {
            return false;
        }

        var uri = new Uri(endpoint!, UriKind.Absolute);

        if (mode == ExplorerTransportMode.InsecureLoopbackDev)
        {
            if (!uri.IsLoopback)
            {
                error = "Insecure loopback-dev mode is only allowed for a loopback endpoint, "
                    + "for example http://localhost:5199. Use a secure https:// endpoint for remote hosts.";
                return false;
            }

            error = null;
            return true;
        }

        // Secure mode: TLS is mandatory for any non-loopback host.
        if (!uri.IsLoopback && !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            error = "A non-loopback endpoint requires TLS. Use an https:// address, or switch to "
                + "insecure loopback-dev mode for a local development endpoint.";
            return false;
        }

        error = null;
        return true;
    }

    /// <summary>
    /// Validates the full connection policy, including whether a credential is
    /// present: in <see cref="ExplorerTransportMode.Secure"/> mode an anonymous
    /// connection to a non-loopback endpoint is rejected (the user must sign in).
    /// </summary>
    /// <param name="endpoint">The endpoint URL.</param>
    /// <param name="mode">The configured transport mode.</param>
    /// <param name="hasCredential">Whether the user is currently authenticated.</param>
    /// <param name="error">A user-facing message when validation fails.</param>
    /// <returns><see langword="true"/> when the connection is permitted.</returns>
    public static bool TryValidateConnection(string? endpoint, ExplorerTransportMode mode, bool hasCredential, out string? error)
    {
        if (!TryValidateEndpoint(endpoint, mode, out error))
        {
            return false;
        }

        var uri = new Uri(endpoint!, UriKind.Absolute);
        if (mode == ExplorerTransportMode.Secure && !uri.IsLoopback && !hasCredential)
        {
            error = "Sign in to connect to a remote endpoint. Anonymous access is only available "
                + "in insecure loopback-dev mode against a local endpoint.";
            return false;
        }

        error = null;
        return true;
    }
}
