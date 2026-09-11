using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The container's <c>--healthcheck</c> self-probe: a short-lived HTTP GET against
/// the host's own <c>/health/silo</c> endpoint on localhost, invoked by the image's
/// exec-form Docker <c>HEALTHCHECK</c>. The runtime image is chiseled and
/// shell-less - it ships no <c>curl</c>, <c>wget</c> or <c>nc</c> - so the probe is
/// the host binary re-invoked against itself, exactly as the ONNX embedder image
/// does. A <c>CMD</c> referencing a probe tool the image does not contain would be
/// permanently unhealthy, which is why this exists.
/// </summary>
/// <remarks>
/// The endpoint is three-valued (see <see cref="RepoContextSiloHealthCheck"/>) and
/// signals its verdict in the response body's leading word rather than the status
/// code alone, because both Degraded (starting) and Unhealthy map to HTTP 503. The
/// probe therefore classifies on the body: it prints a single stable token -
/// <c>HEALTHY</c>, <c>STARTING</c> or <c>UNHEALTHY</c> - so a human reading
/// <c>docker inspect</c> health output sees the distinction, and it returns a
/// process exit code Docker can act on (0 healthy, 1 otherwise). Docker's own
/// health model is two-valued (0/1); "starting" is realised by the compose
/// service's <c>start_period</c> holding early non-zero exits out of the retry
/// tally, not by this exit code.
/// </remarks>
internal static class RepoContextHealthProbe
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(8);

    /// <summary>The three-way verdict the self-probe reports.</summary>
    internal enum Verdict
    {
        /// <summary>The silo answered a trivial grain call.</summary>
        Healthy,

        /// <summary>The silo has not answered yet but is still joining; not a fault.</summary>
        Starting,

        /// <summary>The silo failed a grain call after readiness, or is unreachable.</summary>
        Unhealthy,
    }

    /// <summary>
    /// Probes <c>/health/silo</c> on the given port, prints the classified token,
    /// and returns the process exit code (0 for healthy, 1 otherwise).
    /// </summary>
    /// <param name="port">The port the host listens on.</param>
    public static async Task<int> RunAsync(int port)
    {
        Verdict verdict;
        try
        {
            using var client = new HttpClient { Timeout = RequestTimeout };
            using var response = await client
                .GetAsync(new Uri(
                    string.Create(CultureInfo.InvariantCulture, $"http://localhost:{port}/health/silo")))
                .ConfigureAwait(false);
            var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            verdict = Classify(body);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            // The host is not accepting connections at all: the process is down, the
            // listener never came up, or the request timed out against a wedged host.
            // That is exactly the dead-silo case this probe exists to catch, so it is
            // Unhealthy, not merely "unknown".
            verdict = Verdict.Unhealthy;
        }

        Console.Out.WriteLine(verdict switch
        {
            Verdict.Healthy => "HEALTHY",
            Verdict.Starting => "STARTING",
            _ => "UNHEALTHY",
        });

        return verdict == Verdict.Healthy ? 0 : 1;
    }

    /// <summary>
    /// Classifies the endpoint's plain-text body by its leading status word. The
    /// body is <c>&lt;Status&gt;: &lt;reason&gt;</c>; a Degraded status is the
    /// silo-still-starting case and maps to <see cref="Verdict.Starting"/>, while
    /// anything else - Unhealthy, an unrecognised body, or an empty one - is treated
    /// as Unhealthy so an ambiguous answer never reads as healthy.
    /// </summary>
    /// <param name="body">The response body text.</param>
    internal static Verdict Classify(string? body)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return Verdict.Unhealthy;
        }

        var trimmed = body.TrimStart();
        if (trimmed.StartsWith("Healthy", StringComparison.OrdinalIgnoreCase))
        {
            return Verdict.Healthy;
        }

        if (trimmed.StartsWith("Degraded", StringComparison.OrdinalIgnoreCase))
        {
            return Verdict.Starting;
        }

        return Verdict.Unhealthy;
    }

    /// <summary>
    /// Resolves the listener port from <c>LATTICE_MCP_PORT</c>, falling back to the
    /// documented default when it is unset or unparseable.
    /// </summary>
    /// <param name="rawPort">The raw environment value, or <c>null</c>.</param>
    internal static int ResolvePort(string? rawPort) =>
        int.TryParse(rawPort, NumberStyles.Integer, CultureInfo.InvariantCulture, out var port) && port > 0
            ? port
            : RepoContextHostConfiguration.DefaultMcpPort;
}
