using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// The progress fields of the resize, snapshot and reshard status reads on
/// <see cref="LatticeTreeAdmin"/> (issue 3958): each is projected from its
/// coordinator's durable progress, reported only while the operation runs, and
/// left unknown (a <see langword="null"/> total) rather than invented when the
/// coordinator cannot say.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminProgressTests
{
    private const string Tree = "orders";

    private sealed class AllowGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new AllowGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver());

    private static (IGrainFactory Factory, ILattice Lattice, ILatticeRegistry Registry) Wire()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILattice>(Tree).Returns(lattice);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (factory, lattice, registry);
    }

    // ----- Resize -----

    private static ITreeResizeGrain ResizeReporting(IGrainFactory factory, ResizeProgress progress)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.GetProgressAsync().Returns(progress);
        factory.GetGrain<ITreeResizeGrain>(Tree).Returns(resize);
        return resize;
    }

    [TestCase(0, "Copy")]
    [TestCase(1, "Swap")]
    [TestCase(3, "RejectOldShards")]
    [TestCase(2, "RetireOldCopy")]
    public async Task GetResizeStatusAsync_projects_the_phase_and_units_of_a_running_resize(int corePhase, string expected)
    {
        var (factory, lattice, _) = Wire();
        lattice.IsResizeCompleteAsync().Returns(false);
        ResizeReporting(factory, new ResizeProgress(true, (ResizePhase)corePhase, 5, 7));

        var status = await Create(factory).GetResizeStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(Enum.Parse<TreeResizePhase>(expected)));
            Assert.That(status.CompletedUnits, Is.EqualTo(5));
            Assert.That(status.TotalUnits, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task GetResizeStatusAsync_reports_an_unknown_total_rather_than_zero()
    {
        var (factory, lattice, _) = Wire();
        lattice.IsResizeCompleteAsync().Returns(false);
        ResizeReporting(factory, new ResizeProgress(true, ResizePhase.Swap, 0, 0));

        var status = await Create(factory).GetResizeStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(TreeResizePhase.Swap));
            Assert.That(status.TotalUnits, Is.Null);
        });
    }

    [Test]
    public async Task GetResizeStatusAsync_reports_an_unwinding_undo_as_its_own_phase_without_units()
    {
        var (factory, lattice, _) = Wire();
        lattice.IsResizeCompleteAsync().Returns(false);
        lattice.IsResizeUndoPendingAsync().Returns(true);
        ResizeReporting(factory, new ResizeProgress(true, ResizePhase.Snapshot, 2, 7));

        var status = await Create(factory).GetResizeStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.UndoRequested, Is.True);
            Assert.That(status.Phase, Is.EqualTo(TreeResizePhase.Undo));
            Assert.That(status.CompletedUnits, Is.Zero);
            Assert.That(status.TotalUnits, Is.Null);
        });
    }

    [Test]
    public async Task GetResizeStatusAsync_reports_no_progress_once_the_resize_has_completed()
    {
        var (factory, lattice, _) = Wire();
        lattice.IsResizeCompleteAsync().Returns(true);
        ResizeReporting(factory, new ResizeProgress(true, ResizePhase.Cleanup, 6, 7));

        var status = await Create(factory).GetResizeStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.Null);
            Assert.That(status.CompletedUnits, Is.Zero);
            Assert.That(status.TotalUnits, Is.Null);
        });
    }

    [Test]
    public async Task ResizeTreeAsync_returns_the_progress_of_the_resize_it_started()
    {
        var (factory, lattice, _) = Wire();
        lattice.IsResizeCompleteAsync().Returns(false);
        ResizeReporting(factory, new ResizeProgress(true, ResizePhase.Snapshot, 0, 7));

        var status = await Create(factory).ResizeTreeAsync(Tree, 256, 64);

        Assert.That((status.Phase, status.CompletedUnits, status.TotalUnits), Is.EqualTo(((TreeResizePhase?)TreeResizePhase.Copy, 0, (int?)7)));
    }

    // ----- Snapshot -----

    private static void SnapshotReporting(IGrainFactory factory, SnapshotProgress progress)
    {
        var snapshot = Substitute.For<ITreeSnapshotGrain>();
        snapshot.GetProgressAsync().Returns(progress);
        factory.GetGrain<ITreeSnapshotGrain>(Tree).Returns(snapshot);
    }

    [TestCase(0, "LockSource")]
    [TestCase(3, "BeginForwarding")]
    [TestCase(1, "Copy")]
    [TestCase(2, "UnlockSource")]
    public async Task GetSnapshotStatusAsync_projects_the_phase_and_shards_of_a_running_snapshot(int corePhase, string expected)
    {
        var (factory, lattice, _) = Wire();
        lattice.IsSnapshotCompleteAsync().Returns(false);
        SnapshotReporting(factory, new SnapshotProgress(true, false, "op", (SnapshotPhase)corePhase, 3, 8));

        var status = await Create(factory).GetSnapshotStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(Enum.Parse<TreeSnapshotPhase>(expected)));
            Assert.That(status.CopiedShardCount, Is.EqualTo(3));
            Assert.That(status.ShardCount, Is.EqualTo(8));
        });
    }

    [Test]
    public async Task GetSnapshotStatusAsync_reports_no_progress_for_an_idle_source()
    {
        var (factory, lattice, _) = Wire();
        lattice.IsSnapshotCompleteAsync().Returns(true);
        SnapshotReporting(factory, new SnapshotProgress(false, true, "op", SnapshotPhase.Lock, 0, 0));

        var status = await Create(factory).GetSnapshotStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.Null);
            Assert.That(status.CopiedShardCount, Is.Zero);
            Assert.That(status.ShardCount, Is.Null);
        });
    }

    // ----- Reshard -----

    private static void ReshardReporting(IGrainFactory factory, ReshardProgress progress)
    {
        var reshard = Substitute.For<ITreeReshardGrain>();
        reshard.GetProgressAsync().Returns(progress);
        factory.GetGrain<ITreeReshardGrain>(Tree).Returns(reshard);
    }

    [Test]
    public async Task GetReshardStatusAsync_reports_the_target_and_start_on_a_standalone_read()
    {
        var (factory, lattice, registry) = Wire();
        lattice.IsReshardCompleteAsync().Returns(false);
        registry.GetShardMapAsync(Tree).Returns(new ShardMap { Slots = [0, 1, 2, 3, 4, 5, 0, 1], Version = 4 });
        ReshardReporting(factory, new ReshardProgress(true, 8, 2));

        var status = await Create(factory).GetReshardStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.RequestedShardCount, Is.Null, "a standalone read echoes no trigger");
            Assert.That(status.TargetShardCount, Is.EqualTo(8));
            Assert.That(status.StartPhysicalShardCount, Is.EqualTo(2));
            Assert.That(status.CurrentPhysicalShardCount, Is.EqualTo(6));
        });
    }

    [Test]
    public async Task GetReshardStatusAsync_reports_an_unrecorded_start_as_unknown()
    {
        var (factory, lattice, registry) = Wire();
        lattice.IsReshardCompleteAsync().Returns(false);
        registry.GetShardMapAsync(Tree).Returns((ShardMap?)null);
        ReshardReporting(factory, new ReshardProgress(true, 8, 0));

        var status = await Create(factory).GetReshardStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.TargetShardCount, Is.EqualTo(8));
            Assert.That(status.StartPhysicalShardCount, Is.Null);
        });
    }

    [Test]
    public async Task GetReshardStatusAsync_reports_no_target_once_the_reshard_has_completed()
    {
        var (factory, lattice, registry) = Wire();
        lattice.IsReshardCompleteAsync().Returns(true);
        registry.GetShardMapAsync(Tree).Returns((ShardMap?)null);
        ReshardReporting(factory, new ReshardProgress(true, 8, 2));

        var status = await Create(factory).GetReshardStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.TargetShardCount, Is.Null);
            Assert.That(status.StartPhysicalShardCount, Is.Null);
        });
    }
}
