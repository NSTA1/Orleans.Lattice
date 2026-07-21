using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Validates <see cref="LatticeApiMcpTelemetryOptions"/> at host start: requires
/// an absolute backend address, a defined auth mode with the credential material
/// its mode needs, strictly positive request-timeout and range guardrails, a
/// defined metric-access mode, and - in deny-all mode - a non-empty allow-list
/// with no null-or-empty entries.
/// </summary>
internal sealed class LatticeApiMcpTelemetryOptionsValidator
    : IValidateOptions<LatticeApiMcpTelemetryOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeApiMcpTelemetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.BackendAddress is null)
        {
            failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.BackendAddress)} must be supplied.");
        }
        else if (!options.BackendAddress.IsAbsoluteUri)
        {
            failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.BackendAddress)} must be an absolute URI.");
        }

        if (!Enum.IsDefined(options.AuthMode))
        {
            failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.AuthMode)} must be a defined LatticeTelemetryBackendAuthMode value.");
        }
        else
        {
            ValidateCredential(options, failures);
        }

        if (options.RequestTimeout <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.RequestTimeout)} must be strictly positive.");
        }

        if (options.MaxRange <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.MaxRange)} must be strictly positive.");
        }

        if (options.MaxStep <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.MaxStep)} must be strictly positive.");
        }

        if (!Enum.IsDefined(options.MetricAccess))
        {
            failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.MetricAccess)} must be a defined LatticeTelemetryMetricAccessMode value.");
        }
        else if (options.MetricAccess == LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed)
        {
            if (options.AllowedMetrics.Count == 0)
            {
                failures.Add(
                    $"{nameof(LatticeApiMcpTelemetryOptions.AllowedMetrics)} must list at least one metric when "
                    + $"{nameof(LatticeApiMcpTelemetryOptions.MetricAccess)} is "
                    + $"{nameof(LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed)}; a deny-all posture with an empty allow-list exposes nothing.");
            }
            else if (options.AllowedMetrics.Any(string.IsNullOrWhiteSpace))
            {
                failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.AllowedMetrics)} must not contain a null, empty, or whitespace entry.");
            }
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }

    private static void ValidateCredential(LatticeApiMcpTelemetryOptions options, List<string> failures)
    {
        switch (options.AuthMode)
        {
            case LatticeTelemetryBackendAuthMode.None:
                break;
            case LatticeTelemetryBackendAuthMode.Bearer:
                if (string.IsNullOrEmpty(options.Credential?.BearerToken))
                {
                    failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.Credential)}.{nameof(LatticeTelemetryBackendCredential.BearerToken)} must be supplied when {nameof(LatticeApiMcpTelemetryOptions.AuthMode)} is {nameof(LatticeTelemetryBackendAuthMode.Bearer)}.");
                }

                break;
            case LatticeTelemetryBackendAuthMode.Basic:
                if (string.IsNullOrEmpty(options.Credential?.BasicUsername))
                {
                    failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.Credential)}.{nameof(LatticeTelemetryBackendCredential.BasicUsername)} must be supplied when {nameof(LatticeApiMcpTelemetryOptions.AuthMode)} is {nameof(LatticeTelemetryBackendAuthMode.Basic)}.");
                }

                break;
            case LatticeTelemetryBackendAuthMode.MutualTls:
                if (options.Credential?.ClientCertificate is null)
                {
                    failures.Add($"{nameof(LatticeApiMcpTelemetryOptions.Credential)}.{nameof(LatticeTelemetryBackendCredential.ClientCertificate)} must be supplied when {nameof(LatticeApiMcpTelemetryOptions.AuthMode)} is {nameof(LatticeTelemetryBackendAuthMode.MutualTls)}.");
                }

                break;
            case LatticeTelemetryBackendAuthMode.DynamicBearer:
                // No static credential material: the bearer token is acquired at
                // request time from a registered ITelemetryBackendTokenProvider,
                // which is a DI service rather than an options value, so its
                // presence is enforced by the proxy at first use, not here.
                break;
        }
    }
}
