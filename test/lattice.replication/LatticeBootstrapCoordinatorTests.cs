using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of the receiver-side snapshot/bootstrap state
/// machine in <see cref="LatticeBootstrapCoordinator"/>.
/// </summary>
[TestFixture]
public class LatticeBootstrapCoordinatorTests
{
    private const string Tree = "boot-tree";
    private const string SourceCluster = "site-a";

    private static (
        LatticeBootstrapCoordinator Coordinator,
        IGrainFactory Factory,
        ISnapshotProvider Provider,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm) Create()
    {
        var factory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<ISnapshotProvider>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Arg.Any<string>()).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        return (new LatticeBootstrapCoordinator(factory, provider), factory, provider, apply, hwm);
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static async IAsyncEnumerable<SnapshotEntry> Stream(params SnapshotEntry[] entries)
    {
        await Task.CompletedTask;
        foreach (var e in entries)
        {
            yield return e;
        }
    }

    private static SnapshotStream MakeStream(
        HybridLogicalClock asOf,
        VersionVector frontier,
        IAsyncEnumerable<SnapshotEntry>? entries = null,
        string treeName = Tree) =>
        new(treeName, asOf, frontier, entries ?? Stream());

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        var provider = Substitute.For<ISnapshotProvider>();
        Assert.That(
            () => new LatticeBootstrapCoordinator(null!, provider),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_snapshot_provider_is_null()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => new LatticeBootstrapCoordinator(factory, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetState_throws_when_tree_name_is_null()
    {
        var (coord, _, _, _, _) = Create();
        Assert.That(
            () => coord.GetState(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetState_throws_when_tree_name_is_empty()
    {
        var (coord, _, _, _, _) = Create();
        Assert.That(
            () => coord.GetState(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetState_returns_idle_for_unknown_tree()
    {
        var (coord, _, _, _, _) = Create();
        Assert.That(coord.GetState("never-touched"), Is.EqualTo(LatticeBootstrapState.Idle));
    }

    [Test]
    public void BootstrapAsync_throws_when_tree_name_is_null()
    {
        var (coord, _, _, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(null!, SourceCluster),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_tree_name_is_empty()
    {
        var (coord, _, _, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(string.Empty, SourceCluster),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_source_cluster_id_is_null()
    {
        var (coord, _, _, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(Tree, null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_source_cluster_id_is_empty()
    {
        var (coord, _, _, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(Tree, string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_cancelled_up_front()
    {
        var (coord, _, _, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await coord.BootstrapAsync(Tree, SourceCluster, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task BootstrapAsync_drives_state_machine_through_terminal_LiveIncremental()
    {
        var (coord, _, provider, apply, hwm) = Create();
        var asOf = Hlc(123);
        var frontier = new VersionVector();
        frontier.Tick(SourceCluster);
        var entry = new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(50) };
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(asOf, frontier, Stream(entry))));

        await coord.BootstrapAsync(Tree, SourceCluster);

        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        await provider.Received(1).ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
        await apply.Received(1).ApplySetAsync(
            "k",
            Arg.Any<byte[]>(),
            Hlc(50),
            SourceCluster,
            null,
            0);
        await hwm.Received(1).PinSnapshotAsync(asOf, frontier, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BootstrapAsync_applies_every_emitted_entry_in_order()
    {
        var (coord, _, provider, apply, hwm) = Create();
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "b", Value = new byte[] { 2 }, Timestamp = Hlc(2) },
            new SnapshotEntry { Key = "c", Value = new byte[] { 3 }, Timestamp = Hlc(3) },
        };
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entries))));

        await coord.BootstrapAsync(Tree, SourceCluster);

        Received.InOrder(() =>
        {
            apply.ApplySetAsync("a", Arg.Any<byte[]>(), Hlc(1), SourceCluster, null, 0);
            apply.ApplySetAsync("b", Arg.Any<byte[]>(), Hlc(2), SourceCluster, null, 0);
            apply.ApplySetAsync("c", Arg.Any<byte[]>(), Hlc(3), SourceCluster, null, 0);
            hwm.PinSnapshotAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task BootstrapAsync_with_empty_stream_pins_frontier_and_completes()
    {
        var (coord, _, provider, apply, hwm) = Create();
        var asOf = Hlc(7);
        var frontier = new VersionVector();
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(asOf, frontier)));

        await coord.BootstrapAsync(Tree, SourceCluster);

        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        await apply.DidNotReceive().ApplySetAsync(
            Arg.Any<string>(),
            Arg.Any<byte[]>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<long>());
        await hwm.Received(1).PinSnapshotAsync(asOf, frontier, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BootstrapAsync_skips_entries_with_null_value()
    {
        var (coord, _, provider, apply, _) = Create();
        var entries = new[]
        {
            new SnapshotEntry { Key = "live", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "ghost", Value = null!, Timestamp = Hlc(2) },
        };
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entries))));

        await coord.BootstrapAsync(Tree, SourceCluster);

        await apply.Received(1).ApplySetAsync(
            "live", Arg.Any<byte[]>(), Hlc(1), SourceCluster, null, 0);
        await apply.DidNotReceive().ApplySetAsync(
            "ghost", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<string>(), null, 0);
    }

    [Test]
    public void BootstrapAsync_transitions_to_failed_when_export_throws()
    {
        var (coord, _, provider, _, _) = Create();
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("export boom"));

        Assert.That(
            async () => await coord.BootstrapAsync(Tree, SourceCluster),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.Failed));
    }

    [Test]
    public void BootstrapAsync_transitions_to_failed_when_apply_throws()
    {
        var (coord, _, provider, apply, _) = Create();
        var entry = new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1) };
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(2), new VersionVector(), Stream(entry))));
        apply
            .ApplySetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
                Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>())
            .Throws(new InvalidOperationException("apply boom"));

        Assert.That(
            async () => await coord.BootstrapAsync(Tree, SourceCluster),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.Failed));
    }

    [Test]
    public void BootstrapAsync_transitions_to_failed_when_pin_throws()
    {
        var (coord, _, provider, _, hwm) = Create();
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(2), new VersionVector())));
        hwm
            .PinSnapshotAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("pin boom"));

        Assert.That(
            async () => await coord.BootstrapAsync(Tree, SourceCluster),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.Failed));
    }

    [Test]
    public async Task BootstrapAsync_can_be_restarted_after_failure()
    {
        var (coord, _, provider, _, _) = Create();
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(
                _ => throw new InvalidOperationException("first try"),
                _ => Task.FromResult(MakeStream(Hlc(1), new VersionVector())));

        Assert.That(
            async () => await coord.BootstrapAsync(Tree, SourceCluster),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.Failed));

        await coord.BootstrapAsync(Tree, SourceCluster);

        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
    }

    [Test]
    public async Task BootstrapAsync_concurrent_invocation_for_same_tree_throws_immediately()
    {
        var (coord, _, provider, _, _) = Create();
        var gate = new TaskCompletionSource<SnapshotStream>(TaskCreationOptions.RunContinuationsAsynchronously);
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(gate.Task);

        var first = coord.BootstrapAsync(Tree, SourceCluster);

        // Wait until the first call has acquired the gate and entered
        // the state machine.
        await Task.Yield();
        for (var i = 0; i < 100 && coord.GetState(Tree) != LatticeBootstrapState.RequestingSnapshot; i++)
        {
            await Task.Delay(5);
        }
        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));

        Assert.That(
            async () => await coord.BootstrapAsync(Tree, SourceCluster),
            Throws.InstanceOf<InvalidOperationException>());

        gate.SetResult(MakeStream(Hlc(1), new VersionVector()));
        await first;
        Assert.That(coord.GetState(Tree), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
    }

    [Test]
    public async Task BootstrapAsync_concurrent_invocations_for_different_trees_do_not_block()
    {
        var (coord, _, provider, _, _) = Create();
        provider.ExportAsync(Arg.Any<string>(), HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(MakeStream(Hlc(1), new VersionVector(), entries: null, treeName: ci.ArgAt<string>(0))));

        await Task.WhenAll(
            coord.BootstrapAsync("tree-a", SourceCluster),
            coord.BootstrapAsync("tree-b", SourceCluster));

        Assert.That(coord.GetState("tree-a"), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        Assert.That(coord.GetState("tree-b"), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
    }
}
