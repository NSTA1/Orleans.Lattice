using System.Diagnostics;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Formats a one-line description of the host a bench process landed on, so a
/// cohort whose latency shifts as a whole can be attributed to its placement.
/// Repeats of one Layer 3 cell can differ by a level shift (p50 and p95 slow
/// together, storage latency flat), which points at the container host rather
/// than the code; the CPU model and a short single-thread probe make that
/// visible per replica.
/// </summary>
public static class BenchHostIdentity
{
    private const int ProbeIterations = 20_000_000;

    /// <summary>
    /// Returns <c>[role] host name=.. cpus=.. cpuModel=".." cpuProbeMs=..</c>.
    /// Never throws: an unreadable field is reported as <c>unknown</c>.
    /// </summary>
    /// <param name="role">The log prefix, for example <c>silo</c> or <c>producer</c>.</param>
    public static string Describe(string role)
    {
        ArgumentNullException.ThrowIfNull(role);
        string replica = Environment.GetEnvironmentVariable("CONTAINER_APP_REPLICA_NAME") ?? "unknown";
        return $"[{role}] host name={Environment.MachineName} replica={replica} cpus={Environment.ProcessorCount} cpuModel=\"{ReadCpuModel()}\" cpuProbeMs={RunCpuProbe():0.0}";
    }

    /// <summary>Reads the first <c>model name</c> entry of <c>/proc/cpuinfo</c>.</summary>
    internal static string ReadCpuModel()
    {
        try
        {
            if (!File.Exists("/proc/cpuinfo"))
            {
                return "unknown";
            }

            foreach (string line in File.ReadLines("/proc/cpuinfo"))
            {
                if (line.StartsWith("model name", StringComparison.Ordinal))
                {
                    int colon = line.IndexOf(':');
                    return colon < 0 ? "unknown" : line[(colon + 1)..].Trim().Replace('"', '\'');
                }
            }
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }

        return "unknown";
    }

    /// <summary>
    /// Times a fixed single-thread integer loop. Comparable only between
    /// processes running this same build; it ranks hosts, it is not a benchmark.
    /// </summary>
    internal static double RunCpuProbe()
    {
        Stopwatch sw = Stopwatch.StartNew();
        ulong x = 0x9E3779B97F4A7C15UL;
        for (int i = 0; i < ProbeIterations; i++)
        {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
        }

        sw.Stop();
        GC.KeepAlive(x);
        return sw.Elapsed.TotalMilliseconds;
    }
}
