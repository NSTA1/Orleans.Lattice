using System.Collections.Concurrent;
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
/// Pins how <see cref="BenchIngestEngine"/> accounts the per-entry point modes.
/// Every point entry is its own <c>ILattice</c> call with its own outcome, so a
/// failed call must cost that entry only. Booking the whole flush unit as
/// failed when one call in it threw turned a single Azure Tables timeout into
/// up to 4,096 failed keys and understated the Layer 3 point-write cells.
/// </summary>
[TestFixture]
[NonParallelizable]
public class BenchIngestEnginePointFlushTests
{
    private const int EntryCount = 400;
    private const int FlushConcurrency = 8;

    private static readonly Regex FinalLine = new(
        @"\[silo\] FINAL ops=(?<ops>[\d,]+) failed=(?<failed>[\d,]+)",
        RegexOptions.CultureInvariant);

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

    [TestCase(BenchWorkloadMode.SetPoint)]
    [TestCase(BenchWorkloadMode.SetPointMv)]
    public async Task Point_write_failure_books_only_the_failed_keys(BenchWorkloadMode mode)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci => ci.ArgAt<string>(0) is var key && (key == Key(7) || key == Key(300))
                ? Task.FromException(new InvalidOperationException("Operation could not be completed within the specified time"))
                : Task.CompletedTask);

        await DrainAsync(lattice, mode);

        Assert.That(ParseFinal(), Is.EqualTo((EntryCount - 2, 2)),
            "two failed point writes must cost two keys, not the whole flush unit");
    }

    [Test]
    public async Task Point_read_failure_books_only_the_failed_key()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => ci.ArgAt<string>(0) == Key(42)
                ? Task.FromException<byte[]?>(new InvalidOperationException("read failed for the test"))
                : Task.FromResult<byte[]?>(null));

        await DrainAsync(lattice, BenchWorkloadMode.GetPoint);

        Assert.That(ParseFinal(), Is.EqualTo((EntryCount - 1, 1)));
    }

    [Test]
    public async Task Point_write_saturation_retry_reoffers_only_the_refused_key()
    {
        var writes = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var refused = 0;

        var lattice = Substitute.For<ILattice>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var key = ci.ArgAt<string>(0);
                if (key == Key(123) && Interlocked.Exchange(ref refused, 1) == 0)
                {
                    return Task.FromException(new LatticeSaturatedException("refused for the test", "tree"));
                }

                writes.AddOrUpdate(key, 1, static (_, n) => n + 1);
                return Task.CompletedTask;
            });

        await DrainAsync(lattice, BenchWorkloadMode.SetPoint);

        var rewritten = writes.Where(kv => kv.Value != 1).Select(kv => kv.Key).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(writes, Has.Count.EqualTo(EntryCount));
            Assert.That(rewritten, Is.Empty,
                "a saturation retry must re-offer only the refused key, never rewrite keys that already landed");
            Assert.That(ParseFinal(), Is.EqualTo((EntryCount, 0)));
        });
    }

    private static string Key(int i) => $"k{i:D5}";

    private static async Task DrainAsync(ILattice lattice, BenchWorkloadMode mode)
    {
        var settings = new IngestSettings(
            "tree", 7000, EntryCount, TimeSpan.FromMinutes(5), TimeSpan.FromHours(1), FlushConcurrency, 0,
            mode, 64, 0, 8, 30, 8, 1, "orleans-client");
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var engine = new BenchIngestEngine(
            Substitute.For<IGrainFactory>(), settings, lifetime, new NoOpBenchSaturationGate(), NullLogger.Instance);

        var channel = Channel.CreateUnbounded<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < EntryCount; i++)
        {
            await channel.Writer.WriteAsync(new KeyValuePair<string, byte[]>(Key(i), new byte[] { (byte)(i & 0xFF) }));
        }

        channel.Writer.Complete();

        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await engine.DrainAsync(lattice, channel.Reader, timeout.Token);
    }

    private (int Ops, int Failed) ParseFinal()
    {
        var match = FinalLine.Match(capturedOut.ToString());
        Assert.That(match.Success, Is.True, "the engine must emit its FINAL line");
        return (
            int.Parse(match.Groups["ops"].Value.Replace(",", string.Empty, StringComparison.Ordinal), System.Globalization.CultureInfo.InvariantCulture),
            int.Parse(match.Groups["failed"].Value.Replace(",", string.Empty, StringComparison.Ordinal), System.Globalization.CultureInfo.InvariantCulture));
    }
}
