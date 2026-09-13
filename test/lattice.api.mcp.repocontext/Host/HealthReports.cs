using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Builds <see cref="HealthReport"/> instances for the health-publication fixtures.
/// </summary>
/// <remarks>
/// Reports are assembled from real <see cref="HealthReportEntry"/> values rather than
/// mocked, so a fixture that asserts what the meter carries is asserting against the
/// same shape the health-check service hands the publisher at runtime.
/// </remarks>
internal static class HealthReports
{
    /// <summary>An entry carrying a verdict and an explicit bounded fault cause.</summary>
    /// <param name="status">The verdict.</param>
    /// <param name="cause">The cause to carry in <see cref="HealthReportEntry.Data"/>.</param>
    internal static HealthReportEntry Entry(HealthStatus status, RepoContextSiloProbeFaultCause cause)
        => new(
            status,
            description: null,
            duration: TimeSpan.Zero,
            exception: null,
            data: new Dictionary<string, object>(StringComparer.Ordinal)
            {
                [RepoContextSiloHealthCheck.CauseDataKey] = cause,
            });

    /// <summary>An entry carrying a verdict and no classification at all.</summary>
    /// <param name="status">The verdict.</param>
    internal static HealthReportEntry Unclassified(HealthStatus status)
        => new(status, description: null, duration: TimeSpan.Zero, exception: null, data: null);

    /// <summary>An entry built from a real health-check result, data included.</summary>
    /// <param name="result">The result the check produced.</param>
    internal static HealthReportEntry From(HealthCheckResult result)
        => new(result.Status, result.Description, TimeSpan.Zero, result.Exception, result.Data);

    /// <summary>Wraps named entries into a report.</summary>
    /// <param name="entries">The per-component entries.</param>
    internal static HealthReport Report(params (string Component, HealthReportEntry Entry)[] entries)
        => new(
            entries.ToDictionary(e => e.Component, e => e.Entry, StringComparer.Ordinal),
            TimeSpan.Zero);
}
