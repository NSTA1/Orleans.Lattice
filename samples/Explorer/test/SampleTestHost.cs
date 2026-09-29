using System.Net;
using System.Net.Sockets;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// Starts the whole sample in this process on free loopback ports, with its
/// console configuration in a temporary file, so a test never collides with a
/// sample someone is running on the default ports.
/// </summary>
internal static class SampleTestHost
{
    /// <summary>Seven distinct free loopback ports, one for each port the sample binds.</summary>
    public static SamplePorts FreePorts()
    {
        var listeners = new List<TcpListener>(7);
        try
        {
            for (var i = 0; i < 7; i++)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                listeners.Add(listener);
            }

            var ports = listeners.Select(listener => ((IPEndPoint)listener.LocalEndpoint).Port).ToArray();
            return new SamplePorts(ports[0], ports[1], ports[2], ports[3], ports[4], ports[5], ports[6]);
        }
        finally
        {
            foreach (var listener in listeners)
            {
                listener.Stop();
            }
        }
    }

    /// <summary>Options for a test run: free ports, a temporary console configuration and a fast writer.</summary>
    /// <param name="minimal">Whether to run the single-region sample.</param>
    public static ExplorerSampleOptions Options(bool minimal) => new()
    {
        Minimal = minimal,
        Ports = FreePorts(),
        ExplorerConfigPath = Path.Combine(Path.GetTempPath(), $"explorer-sample-test-{Guid.NewGuid():N}.json"),
        WriterInterval = TimeSpan.FromMilliseconds(250),
    };

    /// <summary>Builds and starts the sample.</summary>
    /// <param name="minimal">Whether to run the single-region sample.</param>
    public static async Task<ExplorerSample> StartAsync(bool minimal)
    {
        var sample = ExplorerSample.Create(Options(minimal));
        try
        {
            await sample.StartAsync();
            return sample;
        }
        catch
        {
            await sample.DisposeAsync();
            throw;
        }
    }

    /// <summary>Fetches the console's server-rendered home page, as the automatically signed-in administrator.</summary>
    /// <param name="sample">The started sample.</param>
    public static async Task<string> GetHomeAsync(ExplorerSample sample)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromMinutes(1) };
        return await client.GetStringAsync(sample.Console.Url);
    }

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or <paramref name="budget"/> elapses: replication is
    /// eventually consistent, so its effects are awaited, never assumed after a fixed delay.
    /// </summary>
    /// <param name="condition">The condition.</param>
    /// <param name="budget">The longest wait.</param>
    /// <returns>Whether the condition held.</returns>
    public static async Task<bool> EventuallyAsync(Func<Task<bool>> condition, TimeSpan budget)
    {
        using var deadline = new CancellationTokenSource(budget);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(200));
        try
        {
            do
            {
                if (await condition())
                {
                    return true;
                }
            }
            while (await poll.WaitForNextTickAsync(deadline.Token));
        }
        catch (OperationCanceledException)
        {
            // The budget elapsed.
        }

        return false;
    }
}
