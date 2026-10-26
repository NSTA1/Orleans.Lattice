using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans;
using Orleans.Lattice;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the FINAL line's <c>active=</c> window to "first accepted batch to
/// last settled flush". On the single-VM rig the ingest channel only
/// completes when the harness stops the silo, after its post-producer
/// quiesce poll; ending the window at channel completion charged those idle
/// seconds to <c>active</c> and deflated <c>(active avg)</c> on every cohort.
/// </summary>
[TestFixture]
[NonParallelizable]
public class BenchIngestEngineActiveWindowTests
{
    private const int EntryCount = 64;

    private static readonly Regex ActiveToken = new(@"\[silo\] FINAL .* active=(?<active>[\d.]+)s", RegexOptions.CultureInvariant);

    private TextWriter originalOut = null!;
    private StringWriter capturedOut = null!;

    [SetUp]
    public void CaptureConsole()
    {
        originalOut = Console.Out;
        capturedOut = new StringWriter();
        Console.SetOut(TextWriter.Synchronized(capturedOut));
    }

    [TearDown]
    public void RestoreConsole()
    {
        Console.SetOut(originalOut);
        capturedOut.Dispose();
    }

    [Test]
    public async Task Active_window_ends_at_the_last_settled_flush_not_at_channel_completion()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var settings = new IngestSettings(
            "tree", 7000, EntryCount, TimeSpan.FromMinutes(5), TimeSpan.FromHours(1), 4, 0,
            BenchWorkloadMode.SetMany, 64, 0, 8, 30, 8, 1, "tcp");
        var engine = new BenchIngestEngine(
            Substitute.For<IGrainFactory>(), settings, Substitute.For<IHostApplicationLifetime>(), new NoOpBenchSaturationGate(), NullLogger.Instance);

        var channel = Channel.CreateUnbounded<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < EntryCount; i++)
        {
            await channel.Writer.WriteAsync(new KeyValuePair<string, byte[]>($"k{i:D3}", [(byte)i]));
        }

        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var drain = engine.DrainAsync(lattice, channel.Reader, timeout.Token);

        // The full batch dispatches and settles at once; the channel stays
        // open (the producer has stopped, the harness has not yet stopped
        // the silo) for well over a second before it completes.
        await Task.Delay(TimeSpan.FromSeconds(2));
        channel.Writer.Complete();
        await drain;

        var match = ActiveToken.Match(capturedOut.ToString());
        Assert.That(match.Success, Is.True, "the engine must emit its FINAL line with active=");
        var active = double.Parse(match.Groups["active"].Value, CultureInfo.InvariantCulture);
        Assert.That(active, Is.LessThan(1.0),
            "idle time after the last flush settled must not be charged to the active window");
    }
}
