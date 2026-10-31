using System.Net;
using System.Net.Sockets;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>Where the repository and the estate's working files are.</summary>
internal static class JourneyPaths
{
    private static readonly Lazy<string> Root = new(FindRoot);

    /// <summary>The repository root: the directory holding <c>Orleans.Lattice.slnx</c>.</summary>
    public static string RepositoryRoot => Root.Value;

    /// <summary>This project's directory, <c>test/lattice.explorer.e2e</c>.</summary>
    public static string ProjectDirectory => Path.Combine(RepositoryRoot, "test", "lattice.explorer.e2e");

    /// <summary>
    /// Where an estate's process logs, screenshots and traces are written:
    /// <c>test/lattice.explorer.e2e/artifacts/{estate}/</c>, the path the CI lane uploads.
    /// </summary>
    /// <param name="estate">The estate's run-unique name.</param>
    public static string Artifacts(string estate)
    {
        var directory = Path.Combine(ProjectDirectory, "artifacts", estate);
        Directory.CreateDirectory(directory);
        return directory;
    }

    private static string FindRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Orleans.Lattice.slnx")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException(
            $"The repository root (holding Orleans.Lattice.slnx) is not above '{AppContext.BaseDirectory}'.");
    }
}

/// <summary>Free loopback ports for the estate's processes.</summary>
internal static class LoopbackPorts
{
    private static readonly HashSet<int> Handed = [];
    private static readonly Lock Gate = new();

    /// <summary>
    /// A loopback TCP port nothing is listening on, never the same one twice in a test run,
    /// so two estates in one process cannot be handed a port a stopped process still owns.
    /// </summary>
    public static int Next()
    {
        lock (Gate)
        {
            while (true)
            {
                using var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                listener.Stop();
                if (Handed.Add(port))
                {
                    return port;
                }
            }
        }
    }

    /// <summary>Whether something accepts TCP connections on <paramref name="port"/> of <paramref name="host"/>.</summary>
    /// <param name="host">The host.</param>
    /// <param name="port">The port.</param>
    /// <param name="timeout">How long to try to connect.</param>
    public static async Task<bool> IsOpenAsync(string host, int port, TimeSpan timeout)
    {
        using var client = new TcpClient();
        using var cancellation = new CancellationTokenSource(timeout);
        try
        {
            await client.ConnectAsync(host, port, cancellation.Token);
            return true;
        }
        catch (Exception ex) when (ex is SocketException or OperationCanceledException)
        {
            return false;
        }
    }
}
