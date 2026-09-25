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
/// Pins how <see cref="BenchIngestEngine"/> schedules and accounts the atomic
/// workload modes (#3581). A producer batch in those modes is many small sagas,
/// and the engine must treat each saga as its own flush unit: dispatched
/// concurrently under the flush gate, retried on its own, and booked as written
/// or failed on its own. Treating the whole batch as one unit ran its sagas as a
/// single sequential chain, so a 2,000-entry batch at 0.6 s per saga took ten
/// minutes to report anything and the Layer 3 cohorts read ops=0.
/// </summary>
[TestFixture]
[NonParallelizable]
public class BenchIngestEngineAtomicFlushTests
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

    [TestCase(EntryCount, TestName = "SetManyAtomic2_full_batch_sagas_run_concurrently_up_to_the_flush_gate")]
    [TestCase(4096, TestName = "SetManyAtomic2_residual_batch_sagas_run_concurrently_up_to_the_flush_gate")]
    public async Task SetManyAtomic2_sagas_run_concurrently_up_to_the_flush_gate(int batchSize)
    {
        var concurrent = 0;
        var maxConcurrent = 0;
        // Every saga parks until FlushConcurrency of them are in flight at
        // once. The fail-safe timer only exists so a sequential engine fails
        // this test in seconds rather than hanging it.
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var failSafe = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var failSafeRegistration = failSafe.Token.Register(() => barrier.TrySetResult());

        var lattice = Substitute.For<ILattice>();
        lattice.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                var now = Interlocked.Increment(ref concurrent);
                UpdateMax(ref maxConcurrent, now);
                if (now >= FlushConcurrency)
                {
                    barrier.TrySetResult();
                }

                try
                {
                    await barrier.Task.ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.Decrement(ref concurrent);
                }
            });

        await DrainAsync(lattice, BenchWorkloadMode.SetManyAtomic2, batchSize);

        Assert.Multiple(() =>
        {
            Assert.That(maxConcurrent, Is.EqualTo(FlushConcurrency),
                "sagas from one producer batch must share the flush gate rather than run as one sequential chain");
            Assert.That(ParseFinal(), Is.EqualTo((EntryCount, 0)));
        });
    }

    [Test]
    public async Task SetManyAtomic2_saturation_retry_recommits_only_the_refused_saga()
    {
        var commits = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var calls = 0;

        var lattice = Substitute.For<ILattice>();
        lattice.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                // The third saga dispatched is refused once for saturation;
                // every other call commits.
                if (Interlocked.Increment(ref calls) == 3)
                {
                    return Task.FromException(new LatticeSaturatedException("refused for the test", "tree"));
                }

                foreach (var entry in ci.Arg<List<KeyValuePair<string, byte[]>>>())
                {
                    commits.AddOrUpdate(entry.Key, 1, static (_, n) => n + 1);
                }

                return Task.CompletedTask;
            });

        await DrainAsync(lattice, BenchWorkloadMode.SetManyAtomic2, EntryCount);

        var recommitted = commits.Where(kv => kv.Value != 1).Select(kv => kv.Key).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(commits, Has.Count.EqualTo(EntryCount));
            Assert.That(recommitted, Is.Empty,
                "a saturation retry must re-offer only the refused saga, never re-commit sagas that already landed under fresh operation ids");
            Assert.That(ParseFinal(), Is.EqualTo((EntryCount, 0)));
        });
    }

    [Test]
    public async Task SetManyAtomic2_failed_saga_books_only_its_own_keys_as_failed()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci => ci.Arg<List<KeyValuePair<string, byte[]>>>().Any(e => e.Key == Key(100))
                ? Task.FromException(new InvalidOperationException("Atomic write saga failed and was rolled back."))
                : Task.CompletedTask);

        await DrainAsync(lattice, BenchWorkloadMode.SetManyAtomic2, EntryCount);

        Assert.That(ParseFinal(), Is.EqualTo((EntryCount - 2, 2)),
            "one rolled-back 2-key saga must cost its 2 keys, not the whole producer batch");
    }

    [Test]
    public async Task SetMany_keeps_the_producer_batch_as_one_flush_unit()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await DrainAsync(lattice, BenchWorkloadMode.SetMany, EntryCount);

        await lattice.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(b => b.Count == EntryCount),
            Arg.Any<CancellationToken>());
        Assert.That(ParseFinal(), Is.EqualTo((EntryCount, 0)));
    }

    private static string Key(int i) => $"k{i:D5}";

    private static void UpdateMax(ref int max, int candidate)
    {
        var seen = Volatile.Read(ref max);
        while (candidate > seen)
        {
            var prior = Interlocked.CompareExchange(ref max, candidate, seen);
            if (prior == seen)
            {
                return;
            }

            seen = prior;
        }
    }

    private static async Task DrainAsync(ILattice lattice, BenchWorkloadMode mode, int batchSize)
    {
        var settings = new IngestSettings(
            "tree", 7000, batchSize, TimeSpan.FromMinutes(5), TimeSpan.FromHours(1), FlushConcurrency, 0,
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
