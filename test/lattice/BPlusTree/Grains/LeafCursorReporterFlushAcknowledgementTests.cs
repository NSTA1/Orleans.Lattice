using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3643: <see cref="LeafCursorReporter.FlushDurableMaterialiserFrontierAsync"/>
/// swallows a faulted shard write so no deactivation is ever blocked by the pin
/// store, which means returning normally does NOT say the merge landed. The
/// flush therefore reports acknowledgement explicitly, and these tests hold it
/// to the one meaning a caller can bank on: <see langword="true"/> only when
/// every routed shard write for the batch completed.
/// </summary>
/// <remarks>
/// The leaf banks its #3599 coverage-lag trigger and the #3643 deactivation
/// elision record from this answer. Reporting a swallowed fault as
/// acknowledgement would let the leaf stand down its republish and elide its
/// last healing barrier with the durable pin still frozen low.
/// </remarks>
[TestFixture]
[Category("Unit")]
public sealed class LeafCursorReporterFlushAcknowledgementTests
{
    private const string Tree = "tree-3643-ack";

    /// <summary>Sharding only engages above one shard, so a partial fault is only expressible here.</summary>
    private const int PinShards = 4;

    /// <summary>
    /// Clears the process-wide durable-pin pressure state (issue #2014), so a
    /// write measured by another fixture cannot shed a write here.
    /// </summary>
    [SetUp]
    public void ResetPinPressure() => WalMaterialiserPinPressure.ResetForTests();

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1, WalMaterialiserPinShards = PinShards };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// A real reporter over a pin grain per shard key. <paramref name="faults"/>
    /// decides, per grain key, whether that shard's <c>ReportManyAsync</c> faults.
    /// </summary>
    private static (LeafCursorReporter Reporter, ConcurrentDictionary<string, int> WritesByKey) Create(
        Func<string, bool> faults)
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        var writes = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(callInfo =>
        {
            // Positional: GetGrain<T>(string primaryKey, string? grainClassNamePrefix = null)
            // takes two string parameters, so a by-type lookup is ambiguous and throws.
            var key = callInfo.ArgAt<string>(0);
            var pin = Substitute.For<IWalMaterialiserPinGrain>();
            pin.ReportManyAsync(Arg.Any<IReadOnlyList<MaterialiserPinReport>>()).Returns(_ =>
            {
                writes.AddOrUpdate(key, 1, static (_, n) => n + 1);
                return faults(key)
                    ? Task.FromException(new InvalidOperationException("pin store transient fault"))
                    : Task.CompletedTask;
            });
            return pin;
        });

        return (new LeafCursorReporter(registry, factory, Monitor()), writes);
    }

    /// <summary>Enough consumers that the batch routes to more than one pin shard.</summary>
    private static IReadOnlyList<MaterialiserPinReport> SpreadBatch()
    {
        var reports = new List<MaterialiserPinReport>(16);
        for (var i = 0; i < 16; i++)
        {
            reports.Add(new MaterialiserPinReport(
                $"_lattice_materialiser_{Tree}_leaf-{i}",
                new HybridLogicalClock { WallClockTicks = 100 + i },
                i));
        }

        return reports;
    }

    [Test]
    public async Task Flush_is_acknowledged_when_every_shard_write_lands()
    {
        var (reporter, writes) = Create(_ => false);

        var acknowledged = await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, SpreadBatch(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(writes.Count, Is.GreaterThan(1),
                "control: the batch must span several shards, or a partial fault is not exercised.");
            Assert.That(acknowledged, Is.True);
        });
    }

    [Test]
    public async Task Flush_is_not_acknowledged_when_one_shard_write_faults()
    {
        string? faulted = null;
        var (reporter, writes) = Create(key => Interlocked.CompareExchange(ref faulted, key, null) is null
            || string.Equals(faulted, key, StringComparison.Ordinal));

        // Must not throw: the swallow-and-log that keeps deactivation unblocked
        // is preserved, which is exactly why the answer has to be explicit.
        var acknowledged = await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, SpreadBatch(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(writes.Count, Is.GreaterThan(1),
                "control: the other shards were written, so only ONE shard faulted.");
            Assert.That(acknowledged, Is.False,
                "one faulted shard means part of the batch never landed, so the batch is not acknowledged.");
        });
    }

    [Test]
    public async Task Flush_is_not_acknowledged_when_every_shard_write_faults()
    {
        var (reporter, _) = Create(_ => true);

        var acknowledged = await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, SpreadBatch(), CancellationToken.None);

        Assert.That(acknowledged, Is.False);
    }

    [Test]
    public async Task Flush_without_durable_backing_is_trivially_acknowledged()
    {
        var reporter = new LeafCursorReporter(Substitute.For<IWalCursorRegistry>());

        var acknowledged = await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, SpreadBatch(), CancellationToken.None);

        Assert.That(acknowledged, Is.True,
            "with no pin store there is nothing that could fail to land.");
    }

    [Test]
    public async Task Flush_of_an_empty_batch_is_trivially_acknowledged()
    {
        var (reporter, writes) = Create(_ => true);

        var acknowledged = await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, [], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(acknowledged, Is.True);
            Assert.That(writes, Is.Empty, "an empty batch issues no shard write.");
        });
    }

    [Test]
    public void Flush_rejects_a_null_batch()
    {
        var (reporter, _) = Create(_ => false);

        Assert.ThrowsAsync<ArgumentNullException>(() => reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, null!, CancellationToken.None));
    }
}
