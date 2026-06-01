using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Unit tests for the advisory WAL byte-pressure policy in
/// <see cref="LatticeWalGc"/>. The policy samples post-trim retained
/// bytes when <see cref="LatticeOptions.WalMaxRetainedBytes"/> is set and
/// reports whether the tree is still over the ceiling without ever trimming
/// past the safe frontier.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcBytePressureTests
{
    private const string Tree = "bp-tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(IWalStorageProvider provider)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        return sc.BuildServiceProvider();
    }

    private static WalEntry Entry(long offset, string key, byte[] value, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = ts,
        },
    };

    private static async Task SeedAsync(IWalStorageProvider provider, int shard, params WalEntry[] entries) =>
        await provider.AppendBatchAsync(Tree, shard, entries, CancellationToken.None);

    [Test]
    public async Task RunOnceAsync_reports_no_byte_pressure_when_policy_disabled()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0, Entry(0, "a", [1, 2, 3], Hlc(10)));

        var sut = new LatticeWalGc(
            Services(provider),
            new InMemoryWalCursorRegistry(),
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = null }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.ByteCeiling, Is.Null);
            Assert.That(report.RetainedBytesBefore, Is.Null);
            Assert.That(report.RetainedBytesAfter, Is.Null);
            Assert.That(report.BytePressureTriggered, Is.False);
            Assert.That(report.BytePressureOverThreshold, Is.False);
        });
    }

    [Test]
    public async Task RunOnceAsync_reports_over_threshold_when_lagging_consumer_pins_log()
    {
        var provider = new InMemoryWalStorageProvider();
        // No consumer cursor and no TTL -> nothing is trim-eligible.
        await SeedAsync(provider, 0,
            Entry(0, "a", new byte[100], Hlc(10)),
            Entry(1, "b", new byte[100], Hlc(20)));

        var sut = new LatticeWalGc(
            Services(provider),
            new InMemoryWalCursorRegistry(),
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 50 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
            Assert.That(report.ByteCeiling, Is.EqualTo(50));
            Assert.That(report.RetainedBytesBefore, Is.GreaterThan(50));
            Assert.That(report.RetainedBytesAfter, Is.GreaterThan(50));
            // Over the ceiling at entry -> the policy scheduled a trim, but a
            // lagging consumer (no cursor) pinned every byte, so it stays over.
            Assert.That(report.BytePressureTriggered, Is.True);
            Assert.That(report.BytePressureOverThreshold, Is.True);
        });
    }

    [Test]
    public async Task RunOnceAsync_reports_within_threshold_after_safe_trim()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            Entry(0, "a", new byte[10], Hlc(10)),
            Entry(1, "b", new byte[10], Hlc(20)),
            Entry(2, "c", new byte[10], Hlc(30)));

        var registry = new InMemoryWalCursorRegistry();
        // Advancing the cursor past every entry lets the GC trim them all.
        await registry.ReportCursorAsync(Tree, "peer", Hlc(30));

        var sut = new LatticeWalGc(
            Services(provider),
            registry,
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3));
            Assert.That(report.ByteCeiling, Is.EqualTo(1));
            Assert.That(report.RetainedBytesBefore, Is.GreaterThan(1));
            Assert.That(report.RetainedBytesAfter, Is.EqualTo(0));
            // Over the ceiling at entry, and the caught-up consumer let the GC
            // safely reclaim every byte: triggered, but no longer over.
            Assert.That(report.BytePressureTriggered, Is.True);
            Assert.That(report.BytePressureOverThreshold, Is.False);
        });
    }

    [Test]
    public async Task RunOnceAsync_sums_retained_bytes_across_partitions()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0, Entry(0, "a", new byte[40], Hlc(10)));
        await SeedAsync(provider, 1, Entry(0, "b", new byte[40], Hlc(15)));

        var sut = new LatticeWalGc(
            Services(provider),
            new InMemoryWalCursorRegistry(),
            Monitor(new LatticeOptions { WalPartitions = 2, WalMaxRetainedBytes = 50 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.RetainedBytesBefore, Is.GreaterThan(50));
        Assert.That(report.RetainedBytesAfter, Is.GreaterThan(50));
        Assert.That(report.BytePressureTriggered, Is.True);
        Assert.That(report.BytePressureOverThreshold, Is.True);
    }

    [Test]
    public async Task RunOnceAsync_zero_ceiling_disables_policy()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0, Entry(0, "a", new byte[100], Hlc(10)));

        var sut = new LatticeWalGc(
            Services(provider),
            new InMemoryWalCursorRegistry(),
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 0 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.ByteCeiling, Is.Null);
            Assert.That(report.RetainedBytesBefore, Is.Null);
            Assert.That(report.RetainedBytesAfter, Is.Null);
            Assert.That(report.BytePressureTriggered, Is.False);
            Assert.That(report.BytePressureOverThreshold, Is.False);
        });
    }
}
