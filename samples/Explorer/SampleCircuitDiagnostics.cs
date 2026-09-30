using Microsoft.AspNetCore.Components.Server;
using Microsoft.Extensions.Logging.Console;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// Makes a console circuit fault diagnosable (issue #4011). The sample keeps its console
/// quiet - it clears every logging provider - so a circuit the framework terminated used
/// to leave nothing behind but the browser's "unhandled exception on the current circuit".
/// The console region now writes exactly one kind of record to the terminal: Blazor's own
/// Error record of an unhandled circuit exception, with its stack trace. In Development
/// the browser is also sent the detail (<see cref="CircuitOptions.DetailedErrors"/>).
/// </summary>
/// <remarks>
/// The record is the framework's, carrying the exception and the circuit id; the sample
/// adds no message of its own and logs no user input.
/// </remarks>
internal static class SampleCircuitDiagnostics
{
    /// <summary>The category prefix Blazor Server logs circuit faults under.</summary>
    public const string CircuitCategory = "Microsoft.AspNetCore.Components.Server.Circuits";

    /// <summary>Adds the terminal log of circuit faults, and nothing else, to a region's logging.</summary>
    /// <param name="logging">The console region's logging, with every provider already cleared.</param>
    public static void ConfigureLogging(ILoggingBuilder logging)
    {
        ArgumentNullException.ThrowIfNull(logging);
        logging.AddSimpleConsole(console => console.ColorBehavior = LoggerColorBehavior.Disabled);
        logging.SetMinimumLevel(LogLevel.None);
        logging.AddFilter(CircuitCategory, LogLevel.Error);
    }

    /// <summary>In Development, sends a circuit fault's detail to the browser as well.</summary>
    /// <param name="services">The console region's services.</param>
    /// <param name="environment">The console region's environment.</param>
    public static void ConfigureCircuits(IServiceCollection services, IHostEnvironment environment)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(environment);
        if (environment.IsDevelopment())
        {
            services.Configure<CircuitOptions>(circuit => circuit.DetailedErrors = true);
        }
    }
}
