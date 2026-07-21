using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure;

/// <summary>
/// Validates <see cref="AzureTelemetryBackendTokenOptions"/> at host start:
/// requires an Azure credential, a non-empty scope, and a non-negative refresh
/// skew.
/// </summary>
internal sealed class AzureTelemetryBackendTokenOptionsValidator
    : IValidateOptions<AzureTelemetryBackendTokenOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, AzureTelemetryBackendTokenOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.Credential is null)
        {
            failures.Add(
                $"{nameof(AzureTelemetryBackendTokenOptions.Credential)} must be supplied "
                + "(for example new DefaultAzureCredential()).");
        }

        if (string.IsNullOrWhiteSpace(options.Scope))
        {
            failures.Add($"{nameof(AzureTelemetryBackendTokenOptions.Scope)} must be a non-empty scope.");
        }

        if (options.RefreshSkew < TimeSpan.Zero)
        {
            failures.Add($"{nameof(AzureTelemetryBackendTokenOptions.RefreshSkew)} must not be negative.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
