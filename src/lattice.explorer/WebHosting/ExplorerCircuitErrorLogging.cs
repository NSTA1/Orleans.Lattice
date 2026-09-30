using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// In Development, makes sure every unhandled circuit exception reaches the log with its
/// stack trace. Blazor logs a circuit that it terminates at <see cref="LogLevel.Error"/>
/// under <see cref="CircuitCategory"/>, carrying the exception; a host whose filters
/// silence that category (for example <c>"Microsoft": "None"</c>) would otherwise show a
/// dead console with no trace of why (issue #4011).
/// </summary>
/// <remarks>
/// The rule only lowers that one category's floor to <see cref="LogLevel.Error"/>; a host
/// that already names the category at <see cref="LogLevel.Error"/> or below keeps its own
/// rule. Outside Development nothing is added, so production logging stays the host's
/// decision. Nothing new is logged: the record is the framework's own, which carries the
/// exception and the circuit id and no user input.
/// </remarks>
/// <param name="environment">The host's environment, or <see langword="null"/> when there is none.</param>
internal sealed class ExplorerCircuitErrorLogging(IHostEnvironment? environment = null) : IConfigureOptions<LoggerFilterOptions>
{
    /// <summary>The category prefix Blazor Server logs circuit faults under.</summary>
    public const string CircuitCategory = "Microsoft.AspNetCore.Components.Server.Circuits";

    /// <inheritdoc />
    public void Configure(LoggerFilterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (environment?.IsDevelopment() != true)
        {
            return;
        }

        var alreadyShown = options.Rules.Any(rule =>
            rule.ProviderName is null
            && rule.Filter is null
            && string.Equals(rule.CategoryName, CircuitCategory, StringComparison.OrdinalIgnoreCase)
            && rule.LogLevel is { } level
            && level <= LogLevel.Error);
        if (!alreadyShown)
        {
            options.Rules.Add(new LoggerFilterRule(providerName: null, CircuitCategory, LogLevel.Error, filter: null));
        }
    }
}
