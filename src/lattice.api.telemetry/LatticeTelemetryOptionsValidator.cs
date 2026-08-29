using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Validates <see cref="LatticeTelemetryOptions"/> at host start: requires
/// an absolute backend address, a defined auth mode with the credential material
/// its mode needs, strictly positive request-timeout and range guardrails, a
/// defined metric-access mode, and - in deny-all mode - a non-empty allow-list
/// with no null-or-empty entries.
/// </summary>
public sealed class LatticeTelemetryOptionsValidator
    : IValidateOptions<LatticeTelemetryOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeTelemetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.BackendAddress is null)
        {
            failures.Add($"{nameof(LatticeTelemetryOptions.BackendAddress)} must be supplied.");
        }
        else if (!options.BackendAddress.IsAbsoluteUri)
        {
            failures.Add($"{nameof(LatticeTelemetryOptions.BackendAddress)} must be an absolute URI.");
        }

        if (!Enum.IsDefined(options.AuthMode))
        {
            failures.Add($"{nameof(LatticeTelemetryOptions.AuthMode)} must be a defined LatticeTelemetryBackendAuthMode value.");
        }
        else
        {
            ValidateCredential(options, failures);
        }

        if (options.RequestTimeout <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeTelemetryOptions.RequestTimeout)} must be strictly positive.");
        }

        if (options.MaxRange <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeTelemetryOptions.MaxRange)} must be strictly positive.");
        }

        if (options.MaxStep <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeTelemetryOptions.MaxStep)} must be strictly positive.");
        }

        if (!Enum.IsDefined(options.MetricAccess))
        {
            failures.Add($"{nameof(LatticeTelemetryOptions.MetricAccess)} must be a defined LatticeTelemetryMetricAccessMode value.");
        }
        else if (options.MetricAccess == LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed)
        {
            if (options.AllowedMetrics.Count == 0)
            {
                failures.Add(
                    $"{nameof(LatticeTelemetryOptions.AllowedMetrics)} must list at least one metric when "
                    + $"{nameof(LatticeTelemetryOptions.MetricAccess)} is "
                    + $"{nameof(LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed)}; a deny-all posture with an empty allow-list exposes nothing.");
            }
            else if (options.AllowedMetrics.Any(string.IsNullOrWhiteSpace))
            {
                failures.Add($"{nameof(LatticeTelemetryOptions.AllowedMetrics)} must not contain a null, empty, or whitespace entry.");
            }
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }

    private static void ValidateCredential(LatticeTelemetryOptions options, List<string> failures)
    {
        switch (options.AuthMode)
        {
            case LatticeTelemetryBackendAuthMode.None:
                break;
            case LatticeTelemetryBackendAuthMode.Bearer:
                if (string.IsNullOrEmpty(options.Credential?.BearerToken))
                {
                    failures.Add($"{nameof(LatticeTelemetryOptions.Credential)}.{nameof(LatticeTelemetryBackendCredential.BearerToken)} must be supplied when {nameof(LatticeTelemetryOptions.AuthMode)} is {nameof(LatticeTelemetryBackendAuthMode.Bearer)}.");
                }

                break;
            case LatticeTelemetryBackendAuthMode.Basic:
                if (string.IsNullOrEmpty(options.Credential?.BasicUsername))
                {
                    failures.Add($"{nameof(LatticeTelemetryOptions.Credential)}.{nameof(LatticeTelemetryBackendCredential.BasicUsername)} must be supplied when {nameof(LatticeTelemetryOptions.AuthMode)} is {nameof(LatticeTelemetryBackendAuthMode.Basic)}.");
                }

                break;
            case LatticeTelemetryBackendAuthMode.MutualTls:
                if (options.Credential?.ClientCertificate is null)
                {
                    failures.Add($"{nameof(LatticeTelemetryOptions.Credential)}.{nameof(LatticeTelemetryBackendCredential.ClientCertificate)} must be supplied when {nameof(LatticeTelemetryOptions.AuthMode)} is {nameof(LatticeTelemetryBackendAuthMode.MutualTls)}.");
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
