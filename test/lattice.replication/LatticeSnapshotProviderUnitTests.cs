using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="LatticeSnapshotProvider"/>'s
/// causal-stable cut-point selection. The frontier source is the
/// <see cref="ILatticeReplicationCursorRegistry"/>'s causal-stable
/// meet when at least one consumer has reported a vector; otherwise
/// the producer's per-tree
/// <see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/> is
/// used as a strict-superset fallback.
/// </summary>
[TestFixture]
public class LatticeSnapshotProviderUnitTests
{
    private const string Tree = "snap-tree";

    private static (LatticeSnapshotProvider Provider, IGrainFactory Factory, ILatticeReplicationCursorRegistry Cursors, ILattice Lattice, IReplicationHighWaterMarkGrain Hwm) Create()
    {
        var factory = Substitute.For<IGrainFactory>();
        var cursors = Substitute.For<ILatticeReplicationCursorRegistry>();
        var lattice = Substitute.For<ILattice>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();

        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);

        // Default: empty entry stream so frontier-only tests don't need
        // to wire EntriesAsync each time.
        lattice.EntriesAsync(
            Arg.Any<string?>(),
            Arg.Any<string?>(),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>()).Returns(EmptyEntries());

        return (new LatticeSnapshotProvider(factory, cursors, TestOptions()), factory, cursors, lattice, hwm);
    }

    /// <summary>
    /// Returns an <see cref="IOptionsMonitor{TOptions}"/> wired to a
    /// fresh <see cref="LatticeReplicationOptions"/> for the snapshot
    /// provider's options read.
    /// </summary>
    internal static IOptionsMonitor<LatticeReplicationOptions> TestOptions()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-test",
        };

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> EmptyEntries()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesOf(params (string Key, byte[] Value)[] entries)
    {
        await Task.CompletedTask;
        foreach (var (key, value) in entries)
        {
            yield return new KeyValuePair<string, byte[]>(key, value);
        }
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        var cursors = Substitute.For<ILatticeReplicationCursorRegistry>();
        Assert.That(
            () => new LatticeSnapshotProvider(null!, cursors, TestOptions()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_cursor_registry_is_null()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => new LatticeSnapshotProvider(factory, null!, TestOptions()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_options_is_null()
    {
        var factory = Substitute.For<IGrainFactory>();
        var cursors = Substitute.For<ILatticeReplicationCursorRegistry>();
        Assert.That(
            () => new LatticeSnapshotProvider(factory, cursors, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ExportAsync_uses_causal_stable_frontier_when_registry_returns_one()
    {
        var (provider, _, cursors, _, hwm) = Create();
        var causalStable = new VersionVector();
        causalStable.Tick("site-a");
        causalStable.Tick("site-b");
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(causalStable));

        var snapshot = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);

        Assert.That(snapshot.CausalStableFrontier, Is.SameAs(causalStable));
        await hwm.DidNotReceive().GetVectorAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExportAsync_falls_back_to_local_vector_when_registry_returns_null()
    {
        var (provider, _, cursors, _, hwm) = Create();
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(null));
        var localVc = new VersionVector();
        localVc.Tick("site-a");
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(localVc));

        var snapshot = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);

        Assert.That(snapshot.CausalStableFrontier, Is.SameAs(localVc));
        await hwm.Received(1).GetVectorAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExportAsync_uses_supplied_tree_name_for_frontier_lookup()
    {
        var (provider, _, cursors, _, _) = Create();
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));

        var snapshot = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);

        await cursors.Received(1).GetCausalStableAsync(Tree, Arg.Any<CancellationToken>());
        Assert.That(snapshot.TreeName, Is.EqualTo(Tree));
    }

    [Test]
    public async Task ExportAsync_returns_stream_with_supplied_as_of_hlc_and_tree_name()
    {
        var (provider, _, cursors, _, _) = Create();
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));
        var asOf = Hlc(123);

        var snapshot = await provider.ExportAsync(Tree, asOf);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TreeName, Is.EqualTo(Tree));
            Assert.That(snapshot.AsOfHlc, Is.EqualTo(asOf));
        });
    }

    [Test]
    public async Task Entries_skips_keys_with_timestamp_strictly_greater_than_as_of_hlc()
    {
        var (provider, _, cursors, lattice, _) = Create();
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));
        lattice.EntriesAsync(
            Arg.Any<string?>(),
            Arg.Any<string?>(),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>()).Returns(EntriesOf(
                ("a", new byte[] { 1 }),
                ("b", new byte[] { 2 })));
        lattice.GetWithVersionAsync("a", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = new byte[] { 1 }, Version = Hlc(50) }));
        lattice.GetWithVersionAsync("b", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = new byte[] { 2 }, Version = Hlc(150) }));

        var snapshot = await provider.ExportAsync(Tree, Hlc(100));
        var collected = new List<SnapshotEntry>();
        await foreach (var e in snapshot.Entries)
        {
            collected.Add(e);
        }

        Assert.That(collected, Has.Count.EqualTo(1));
        Assert.That(collected[0].Key, Is.EqualTo("a"));
    }

    [Test]
    public async Task Entries_yields_every_live_entry_when_as_of_hlc_is_zero()
    {
        var (provider, _, cursors, lattice, _) = Create();
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));
        lattice.EntriesAsync(
            Arg.Any<string?>(),
            Arg.Any<string?>(),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>()).Returns(EntriesOf(
                ("a", new byte[] { 1 }),
                ("b", new byte[] { 2 })));
        lattice.GetWithVersionAsync("a", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = new byte[] { 1 }, Version = Hlc(1) }));
        lattice.GetWithVersionAsync("b", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = new byte[] { 2 }, Version = Hlc(int.MaxValue) }));

        var snapshot = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);
        var collected = new List<SnapshotEntry>();
        await foreach (var e in snapshot.Entries)
        {
            collected.Add(e);
        }

        Assert.That(collected.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Entries_skips_keys_tombstoned_between_enumeration_and_version_read()
    {
        var (provider, _, cursors, lattice, _) = Create();
        cursors.GetCausalStableAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));
        lattice.EntriesAsync(
            Arg.Any<string?>(),
            Arg.Any<string?>(),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>()).Returns(EntriesOf(
                ("ghost", new byte[] { 9 })));
        lattice.GetWithVersionAsync("ghost", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero }));

        var snapshot = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);
        var collected = new List<SnapshotEntry>();
        await foreach (var e in snapshot.Entries)
        {
            collected.Add(e);
        }

        Assert.That(collected, Is.Empty);
    }
}
