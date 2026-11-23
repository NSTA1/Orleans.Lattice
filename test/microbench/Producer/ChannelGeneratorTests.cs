using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.AzureThroughput.Producer;

namespace Orleans.Lattice.Benchmark.Microbench.Tests.Producer;

[TestFixture]
public class ChannelGeneratorTests
{
    [Test]
    public async Task RunAsync_read_key_space_beyond_fleet_reads_across_the_seeded_tree()
    {
        const int Fleet = 1000;
        const int KeySpace = 1_000_000;
        var generator = new ChannelGenerator(Fleet, parallelism: 4, readKeySpace: KeySpace);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var entries = new List<KeyValuePair<string, byte[]>>();
        var drain = Task.Run(async () =>
        {
            await foreach (var entry in generator.Reader.ReadAllAsync(timeout.Token)) entries.Add(entry);
        });
        await generator.RunAsync(tickHz: 1, duration: 1, readOnly: true, timeout.Token);
        await drain;

        Assert.That(entries, Has.Count.EqualTo(Fleet), "The offered load is still the fleet per tick.");
        Assert.That(entries.Select(e => e.Key).Distinct().Count(), Is.EqualTo(Fleet));
        var fleetKeys = Enumerable.Range(0, Fleet).Select(i => new Guid(i, unchecked((short)0xffee), 0x00c0, 0xef, 0xbe, 0xad, 0xde, 0xbe, 0xba, 0xfe, 0xca).ToString("N")).ToHashSet();
        Assert.That(entries.Count(e => fleetKeys.Contains(e.Key)), Is.LessThan(Fleet / 10), "Reads range beyond the fleet's own keys.");
        var indices = entries.Select(e => Guid.ParseExact(e.Key, "N").ToByteArray()).Select(b => BitConverter.ToInt32(b, 0)).ToArray();
        Assert.That(indices, Is.All.InRange(0, KeySpace - 1), "Every read targets a seeded index.");
        Assert.That(indices.Max() - indices.Min(), Is.GreaterThan(KeySpace / 2), "Reads span the seeded tree.");
    }

    [Test]
    public void StridedReadIndex_visits_every_index_once_per_period()
    {
        const int KeySpace = 10_007;
        var seen = new HashSet<int>();
        for (long s = 0; s < KeySpace; s++) seen.Add(ChannelGenerator.StridedReadIndex(s + (3L << 40), KeySpace));
        Assert.That(seen, Has.Count.EqualTo(KeySpace));
    }

    [Test]
    public async Task RunAsync_read_key_space_not_above_fleet_keeps_fleet_keys()
    {
        var generator = new ChannelGenerator(100, parallelism: 2, readKeySpace: 100);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var entries = new List<KeyValuePair<string, byte[]>>();
        var drain = Task.Run(async () =>
        {
            await foreach (var entry in generator.Reader.ReadAllAsync(timeout.Token)) entries.Add(entry);
        });
        await generator.RunAsync(tickHz: 1, duration: 1, readOnly: true, timeout.Token);
        await drain;
        var expected = Enumerable.Range(0, 100).Select(i => new Guid(i, unchecked((short)0xffee), 0x00c0, 0xef, 0xbe, 0xad, 0xde, 0xbe, 0xba, 0xfe, 0xca).ToString("N"));
        Assert.That(entries.Select(e => e.Key), Is.EquivalentTo(expected));
    }

    [TestCase(1, 10003, true)]
    [TestCase(4, 10003, true)]
    [TestCase(4, 10003, false)]
    [TestCase(8, 3, true)]
    public async Task RunAsync_disjoint_slices_preserve_keys_payload_and_offered_load(int parallelism, int count, bool readOnly)
    {
        var generator = new ChannelGenerator(count, parallelism);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var entries = new List<KeyValuePair<string, byte[]>>();
        var drain = Task.Run(async () =>
        {
            await foreach (var entry in generator.Reader.ReadAllAsync(timeout.Token)) entries.Add(entry);
        });
        await generator.RunAsync(tickHz: 1, duration: 1, readOnly, timeout.Token);
        await drain;

        Assert.That(entries, Has.Count.EqualTo(count), "Each vehicle is emitted once per tick, not once per worker.");
        Assert.That(entries.Select(e => e.Key).Distinct().Count(), Is.EqualTo(count));
        var byKey = entries.ToDictionary(e => e.Key);
        for (var i = 0; i < count; i++)
        {
            var id = new Guid(i, unchecked((short)0xffee), 0x00c0, 0xef, 0xbe, 0xad, 0xde, 0xbe, 0xba, 0xfe, 0xca);
            var entry = byKey[id.ToString("N")];
            if (readOnly)
            {
                Assert.That(entry.Value, Is.SameAs(Array.Empty<byte>()));
            }
            else
            {
                var actual = JsonSerializer.Deserialize<VehicleTelemetryEvent>(entry.Value);
                var expected = new VehicleTelemetryEvent(id, actual.TimestampUtc, "A", "B",
                    (i % 100) * 0.5, 100, 60, 40, VehicleStatus.Driving, 50);
                Assert.That(entry.Value, Is.EqualTo(JsonSerializer.SerializeToUtf8Bytes(expected)));
                Assert.That(actual.TimestampUtc, Is.Not.EqualTo(default(DateTimeOffset)));
            }
        }
    }

    [Test]
    public async Task RunAsync_repeated_ticks_reuse_key_strings()
    {
        var generator = new ChannelGenerator(17, 4);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var entries = new List<KeyValuePair<string, byte[]>>();
        var drain = Task.Run(async () =>
        {
            await foreach (var entry in generator.Reader.ReadAllAsync(timeout.Token)) entries.Add(entry);
        });
        await generator.RunAsync(5, 1, true, timeout.Token);
        await drain;
        foreach (var group in entries.GroupBy(e => e.Key))
        {
            Assert.That(group.Count(), Is.GreaterThan(1));
            Assert.That(group.All(e => ReferenceEquals(e.Key, group.First().Key)), Is.True);
        }
    }

    [Test]
    public async Task RunAsync_blocked_channel_cancels_all_workers()
    {
        var generator = new ChannelGenerator(200000, 4);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var run = generator.RunAsync(1, 0, true, timeout.Token);
        try
        {
            await run.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Fail("An infinite generator must observe cancellation while its channel is full.");
        }
        catch (OperationCanceledException)
        {
            Assert.That(run.IsCompleted, Is.True);
        }
    }

    [Test]
    public void Snapshot_channel_wait_is_counted_even_when_also_late()
    {
        var progress = new GeneratorProgress();
        var before = Stopwatch.GetTimestamp();
        progress.BeginTick(Stopwatch.GetTimestamp() - Stopwatch.Frequency, 1);
        progress.BeginWait();
        var sleepStart = Stopwatch.GetTimestamp();
        Thread.Sleep(20);
        var sleepEnd = Stopwatch.GetTimestamp();
        var state = progress.Snapshot();
        var after = Stopwatch.GetTimestamp();
        progress.EndWait();
        progress.Complete();
        Assert.That(state.Blocked, Is.GreaterThanOrEqualTo(sleepEnd - sleepStart));
        Assert.That(state.Blocked, Is.LessThanOrEqualTo(after - before));
        Assert.That(state.Slip, Is.GreaterThanOrEqualTo(Stopwatch.Frequency));
    }

    [Test]
    public void Snapshot_late_without_channel_wait_does_not_report_backpressure()
    {
        var progress = new GeneratorProgress();
        progress.BeginTick(Stopwatch.GetTimestamp() - Stopwatch.Frequency, 1);
        progress.Sent(1);
        var state = progress.Snapshot();
        Assert.That(state.Slip, Is.GreaterThanOrEqualTo(Stopwatch.Frequency));
        Assert.That(state.Blocked, Is.Zero);
    }

    [Test]
    [NonParallelizable]
    public async Task RunAsync_slow_consumer_reports_both_high_wait_fraction_and_slip()
    {
        var generator = new ChannelGenerator(200000, 1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        var previous = Console.Out;
        Console.SetOut(TextWriter.Synchronized(output));
        try
        {
            var run = generator.RunAsync(5, 1, true, timeout.Token);
            await generator.Reader.WaitToReadAsync(timeout.Token);
            await Task.Delay(1500, timeout.Token);
            await foreach (var entry in generator.Reader.ReadAllAsync(timeout.Token)) { }
            await run;
            var done = Regex.Match(output.ToString(), @"\[producer\] DONE .*genBlockedFrac=([\d.]+) slipMaxMs=([\d.]+)");
            Assert.That(done.Success, Is.True);
            Assert.That(double.Parse(done.Groups[1].Value, CultureInfo.InvariantCulture), Is.GreaterThanOrEqualTo(0.2));
            Assert.That(double.Parse(done.Groups[2].Value, CultureInfo.InvariantCulture), Is.GreaterThan(1000));
        }
        finally
        {
            await timeout.CancelAsync();
            Console.SetOut(previous);
        }
    }
}
