using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Validates <see cref="LatticeApiMcpTelemetryOptions"/> at host start. The rules
/// - an absolute backend address, a defined auth mode with the credential
/// material its mode needs, strictly positive request-timeout and range
/// guardrails, a defined metric-access mode, and, in deny-all mode, a non-empty
/// allow-list with no null-or-empty entries - belong to the neutral
/// <see cref="LatticeTelemetryOptions"/> surface, so this binding validator
/// delegates to <see cref="LatticeTelemetryOptionsValidator"/> rather than
/// restating them.
/// </summary>
internal sealed class LatticeApiMcpTelemetryOptionsValidator
    : IValidateOptions<LatticeApiMcpTelemetryOptions>
{
    private static readonly LatticeTelemetryOptionsValidator Inner = new();

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeApiMcpTelemetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return Inner.Validate(name, options);
    }
}
