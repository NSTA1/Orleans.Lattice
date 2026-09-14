using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The shared plain-text response writer for this host's component-listing health
/// endpoints. It emits the aggregate verdict on the first line and then one
/// <c>&lt;name&gt;: &lt;Status&gt;: &lt;description&gt;</c> line per component, so a
/// non-200 says <b>which</b> component reached the verdict rather than only
/// <b>that</b> one was reached.
/// </summary>
/// <remarks>
/// <para>
/// <b>The diagnosis already existed; it was discarded at the HTTP boundary.</b> Every
/// health check on this host returns a carefully worded description - the retrieval
/// one was narrowed by issue #2362 specifically so it would not overclaim, and the
/// backup one renders which tree is protected, how many entries the last full capture
/// described, when the newest sink backup was taken, and the last failure text. An
/// endpoint mapped with a bare tag predicate and no response writer falls back to the
/// framework default, which writes the aggregate status word and nothing else, so all
/// of that is computed on every probe and then dropped one layer below where it was
/// written.
/// </para>
/// <para>
/// The cost is attributability, which is the property acceptance evidence is scored
/// on. Worse, the reading looks complete: the body is non-empty, so a consumer that
/// guards on "did it say anything" is satisfied by a string that says nothing, and a
/// consumer that prints the body renders a wrong string identically to a right one.
/// There is no arity error, no type error and no crash, so the defect survives contact
/// with every consumer that touches it. Issue #2962 recorded this on
/// <c>/health/ready</c> and issue #2980 recorded the same defect on
/// <c>/health/backup</c>, where documentation had additionally been written describing
/// the body the endpoint never sent.
/// </para>
/// <para>
/// <b>Deliberately additive.</b> The first line is still exactly the aggregate status
/// word, so a consumer that compared the whole body against <c>Healthy</c> keeps
/// working, and callers leave <see cref="HealthCheckOptions.ResultStatusCodes"/> at
/// the framework default so the code an orchestrator (and the acceptance predicate)
/// reads is unchanged. An endpoint using this writer gains detail; it does not move.
/// </para>
/// <para>
/// <b>Entries are ordered by name.</b> <see cref="HealthReport.Entries"/> is a
/// dictionary and its enumeration order is not contractual, so an unordered body could
/// differ between two runs of an unchanged deployment. Run-to-run comparability is the
/// basis on which this rig scores anything, so the order is pinned rather than left to
/// the framework. It is not load-bearing on an endpoint that currently reports a single
/// component, and is applied there anyway so that adding a second one cannot introduce
/// the non-determinism quietly.
/// </para>
/// </remarks>
public static class RepoContextComponentHealthResponse
{
    /// <summary>
    /// Writes the aggregate verdict followed by one line per reported component,
    /// ordered by component name.
    /// </summary>
    /// <param name="context">The HTTP context.</param>
    /// <param name="report">The health report over the endpoint's selected entries.</param>
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
