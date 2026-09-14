using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The response writer for the <c>/health/ready</c> endpoint. It emits the aggregate
/// verdict on the first line and then one <c>&lt;name&gt;: &lt;Status&gt;: &lt;description&gt;</c>
/// line per component, so a 503 says <b>which</b> component of the readiness
/// conjunction is not ready rather than only <b>that</b> one is not.
/// </summary>
/// <remarks>
/// <para>
/// <b>The diagnosis already existed; it was discarded at the HTTP boundary.</b>
/// <c>/health/ready</c> is the conjunction of
/// <see cref="RepoContextReadinessHealthCheck"/> (lifecycle) and
/// <see cref="RepoContextRetrievalReadinessHealthCheck"/> (vector plane), and each
/// returns a carefully worded description - the retrieval one was narrowed by issue
/// #2362 specifically so it would not overclaim. The endpoint was mapped with a bare
/// tag predicate and no response writer, so the framework default wrote the aggregate
/// status word and nothing else. Every one of those descriptions was computed on every
/// probe and then dropped.
/// </para>
/// <para>
/// The cost is attributability, which is the property acceptance evidence is scored
/// on. A captured body of <c>Unhealthy</c> cannot distinguish a still-replaying
/// lifecycle from a degraded vector plane; the two have entirely different remedies
/// and only one of them is a retrieval finding. Worse, the reading looks complete:
/// the body is non-empty, so a consumer that guards on "did it say anything" is
/// satisfied by a string that says nothing. That is the defect class issue #2962
/// records, one layer further out - the detector is correct, its message is correct,
/// and the transport drops it.
/// </para>
/// <para>
/// <b>Deliberately additive.</b> The first line is still exactly the aggregate status
/// word, so a consumer that compared the whole body against <c>Healthy</c> keeps
/// working, and the status-code mapping is left at the framework default so the code
/// an orchestrator (and the acceptance predicate) reads is unchanged. This endpoint
/// gains detail; it does not move.
/// </para>
/// <para>
/// <b>Entries are ordered by name.</b> <see cref="HealthReport.Entries"/> is a
/// dictionary and its enumeration order is not contractual, so an unordered body
/// could differ between two runs of an unchanged deployment. Run-to-run comparability
/// is the basis on which this rig scores anything, so the order is pinned rather than
/// left to the framework.
/// </para>
/// </remarks>
public static class RepoContextReadinessHealthResponse
{
    /// <summary>
    /// Writes the aggregate verdict followed by one line per readiness component,
    /// ordered by component name.
    /// </summary>
    /// <param name="context">The HTTP context.</param>
    /// <param name="report">The health report over the readiness-tagged entries.</param>
    /// <exception cref="ArgumentNullException"><paramref name="context"/> or <paramref name="report"/> is null.</exception>
    public static Task Write(HttpContext context, HealthReport report)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(report);

        var body = new StringBuilder();
        body.Append(report.Status.ToString());

        foreach (var name in report.Entries.Keys.OrderBy(key => key, StringComparer.Ordinal))
        {
            var entry = report.Entries[name];
            var description = Flatten(entry.Description) ?? entry.Status.ToString();

            body.Append('\n')
                .Append(name)
                .Append(": ")
                .Append(entry.Status.ToString())
                .Append(": ")
                .Append(description);

            // The exception message, when one is present, is the only part of a
            // thrown fault that survives into the report. Omitting it would leave a
            // component that failed by throwing indistinguishable on the wire from
            // one that returned Unhealthy deliberately.
            var failure = Flatten(entry.Exception?.Message);
            if (failure is not null && !string.Equals(failure, description, StringComparison.Ordinal))
            {
                body.Append(" (").Append(failure).Append(')');
            }
        }

        context.Response.ContentType = "text/plain; charset=utf-8";
        return context.Response.WriteAsync(body.ToString(), context.RequestAborted);
    }

    /// <summary>
    /// Collapses interior whitespace so one component always occupies exactly one
    /// line. A description carrying a newline would otherwise split into what reads
    /// as an extra component and silently corrupt a line-oriented parse of the body.
    /// </summary>
    private static string? Flatten(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;

        var parts = value.Split(
            (char[]?)null,
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        return parts.Length == 0 ? null : string.Join(' ', parts);
    }
}

/// <summary>
/// Builds the <c>/health/ready</c> endpoint options - the tag predicate and the
/// plain-text response writer - in one place so the host wiring and its tests map the
/// endpoint identically, and a test cannot pass against a mapping that has drifted
/// from the one the host serves.
/// </summary>
public static class RepoContextReadinessHealthEndpoint
{
    /// <summary>
    /// Creates the health-check options for the readiness endpoint.
    /// </summary>
    /// <remarks>
    /// <see cref="HealthCheckOptions.ResultStatusCodes"/> is deliberately left at the
    /// framework default (Healthy and Degraded 200, Unhealthy 503). The status code is
    /// what an orchestrator routes on and what the acceptance predicate records, so it
    /// is held fixed here on purpose: this change adds detail to the body and must not
    /// move the verdict. <c>RepoContextReadinessHealthEndpointTests</c> pins the
    /// mapping so a later edit cannot change it quietly.
    /// </remarks>
    /// <param name="readinessTag">The tag identifying the readiness-tagged health checks.</param>
    /// <returns>The options used to map the readiness endpoint.</returns>
    /// <exception cref="ArgumentException"><paramref name="readinessTag"/> is null or empty.</exception>
    public static HealthCheckOptions CreateOptions(string readinessTag)
    {
        ArgumentException.ThrowIfNullOrEmpty(readinessTag);

        return new HealthCheckOptions
        {
            Predicate = registration => registration.Tags.Contains(readinessTag),
            ResponseWriter = RepoContextReadinessHealthResponse.Write,
        };
    }
}
