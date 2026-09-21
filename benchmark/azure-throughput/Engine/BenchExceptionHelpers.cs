using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Exception classification and formatting helpers shared by the ingest
/// engine and by the silo-side TCP listener that feeds it.
/// </summary>
/// <remarks>
/// These were private statics on <c>TcpIngestService</c> before the engine
/// was extracted. Both halves of that split still need them - the engine
/// classifies flush failures, and the silo's connection handler and startup
/// reshard retry classify placement-directory races - so they live here and
/// are imported with <c>using static</c> at both call sites, which keeps
/// every existing call site byte-identical.
/// </remarks>
internal static class BenchExceptionHelpers
{
    internal static bool IsShutdownRejection(Exception ex)
    {
        // The library surfaces shutdown-refused failures (from the
        // saga coordinator AND from direct SetAsync / SetManyAsync
        // calls that race the writer drain) as the typed public
        // LatticeShuttingDownException. The bench treats this as
        // ShutdownDiscarded so the residual at-shutdown failures
        // attribute to discarded=N rather than failed=N on FINAL,
        // mirroring the existing residual-channel-abandon contract.
        // This gate runs only when ApplicationStopping is requested,
        // so it cannot mask a steady-state error.
        if (ex is LatticeShuttingDownException)
        {
            return true;
        }

        // Match the two messages Orleans emits when an activation cannot
        // be created because the silo is draining. Type-name match keeps
        // the check resilient to Orleans internalising the type.
        var typeName = ex.GetType().FullName ?? string.Empty;
        if (!typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal))
        {
            return false;
        }
        var msg = ex.Message ?? string.Empty;
        return msg.Contains("Unable to create local activation", StringComparison.Ordinal)
            || msg.Contains("silo is blocking application messages", StringComparison.Ordinal)
            || msg.Contains("to invalid activation", StringComparison.Ordinal);
    }

    // Type-name match for any Orleans message-rejection exception. Used
    // by the startup reshard retry: a brand-new silo's first call to a
    // never-activated grain races the client directory cache and lands
    // here, but the directory recovers within a few hundred ms and a
    // retry succeeds. Keeping this separate from IsShutdownRejection
    // makes the retry safe to use before the silo is anywhere near
    // shutdown.
    internal static bool IsOrleansMessageRejection(Exception ex)
    {
        var typeName = ex.GetType().FullName ?? string.Empty;
        return typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal);
    }

    internal static string Truncate(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return string.Empty;
        return s.Length <= max ? s : s.Substring(0, max) + "...";
    }
}
