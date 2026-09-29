using System.Diagnostics;
using Orleans.Lattice.Samples.Explorer;

// Orleans.Lattice.Explorer sample: one process that shows every area of the
// Explorer console with live data, with no cloud dependency.
//
// By default it runs a two-region estate: the east and west regions are two
// single-silo Orleans clusters in this process, each serving every control
// plane the Explorer has an area for on its own gRPC endpoint, replicating with
// each other over loopback gRPC, and sharing one in-memory backup sink. East
// also serves the console, which connects to east (or to west, with
// --explorer-region west). Tenancy is on, with two seeded tenants. --minimal
// keeps the single-region experience with no tenancy and no peer.
//
// The console signs in automatically as the bootstrap administrator, so every
// administrator-gated area lights up. Telemetry is the one area that stays
// hidden: it reads a Prometheus-compatible metrics backend, which this sample
// does not run. README.md walks each area.

if (!ExplorerSampleOptions.TryParse(args, Environment.GetEnvironmentVariable, out var options, out var error))
{
    Console.WriteLine(error);
    return 2;
}

var clock = Stopwatch.StartNew();
Console.WriteLine(options.Minimal ? "Starting one region..." : "Starting the east and west regions...");

await using var sample = ExplorerSample.Create(options);
await sample.StartAsync();
SampleBanner.Write(Console.Out, sample, clock.Elapsed);

sample.PeerLink.Changed += paused => Console.WriteLine(paused
    ? "Peer link PAUSED: replication between east and west is refused. Watch Replication go Lagging, then Stalled."
    : "Peer link RESUMED: the regions catch up.");

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};

if (sample.West is not null)
{
    _ = Task.Run(() => ReadPeerToggles(sample.PeerLink, stop.Token));
}

try
{
    await Task.Delay(Timeout.Infinite, stop.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Stopping...");
}

return 0;

// P (or a line starting with p, when input is redirected) pauses or resumes the
// link between the regions.
static void ReadPeerToggles(PeerLink link, CancellationToken stop)
{
    while (!stop.IsCancellationRequested)
    {
        if (Console.IsInputRedirected)
        {
            var line = Console.In.ReadLine();
            if (line is null)
            {
                return;
            }

            if (line.Trim().StartsWith('p') || line.Trim().StartsWith('P'))
            {
                link.Toggle();
            }
        }
        else if (Console.ReadKey(intercept: true).Key == ConsoleKey.P)
        {
            link.Toggle();
        }
    }
}
