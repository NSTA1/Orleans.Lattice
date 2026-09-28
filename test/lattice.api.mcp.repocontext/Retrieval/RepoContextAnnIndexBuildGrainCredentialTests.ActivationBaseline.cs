using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
{
    [Test]
    public async Task First_step_after_durable_restore_does_not_count_restored_vectors_as_advanced()
    {
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 32);
        rig.Start();
        Assert.That(await rig.PumpAsync(), Is.LessThan(MaxTicks));
        var before = rig.SliceReporter.Read();
        rig.Registry.Dispose();
        rig.Start();

        await rig.Grain.ProcessNextPhaseAsync();

        var after = rig.SliceReporter.Read();
        Assert.Multiple(() =>
        {
            Assert.That(PlaneProgress(rig).RestoredFromDurableState, Is.True);
            Assert.That(PlaneProgress(rig).VectorsIndexed, Is.EqualTo(32));
            Assert.That(after.Advanced - before.Advanced, Is.Zero);
            Assert.That(after.Idle - before.Idle, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task First_step_that_only_moves_phase_is_churned()
    {
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 32);
        rig.Start();
        await rig.Grain.EnsureBuildingAsync(Space);
        for (var tick = 0; tick < 10 && PlaneProgress(rig).VectorsIndexed < 32; tick++)
            await rig.Grain.ProcessNextPhaseAsync();
        Assert.That(PlaneProgress(rig).Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        var before = rig.SliceReporter.Read();
        rig.Reactivate();

        await rig.Grain.ProcessNextPhaseAsync();

        var after = rig.SliceReporter.Read();
        Assert.Multiple(() =>
        {
            Assert.That(PlaneProgress(rig).Phase, Is.EqualTo(VectorIndexBuildPhase.Training));
            Assert.That(PlaneProgress(rig).VectorsIndexed, Is.EqualTo(32));
            Assert.That(after.Advanced - before.Advanced, Is.Zero);
            Assert.That(after.Churned - before.Churned, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Append_checkpoint_timeout_is_an_ingesting_fault()
    {
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();
        await rig.Grain.EnsureBuildingAsync(Space);
        await rig.Grain.ProcessNextPhaseAsync();
        Assert.That(PlaneProgress(rig).Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        var store = rig.Backing.Store(RepoId, Space);
        store.WriteFaultFactory = () => new TimeoutException("checkpoint timeout");
        store.FaultWriteKey = VectorIndexStorageKeys.Manifest(
            RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space));
        store.FaultWrites = true;
        var before = rig.SliceReporter.Read().FaultedByPhase.Ingesting;

        Assert.ThrowsAsync<TimeoutException>(() => rig.Grain.ProcessNextPhaseAsync());

        Assert.That(store.RefusedWrites, Is.EqualTo(1), "The append checkpoint's manifest commit must be reached.");
        Assert.That(PlaneProgress(rig).VectorsIndexed, Is.GreaterThan(0));
        Assert.That(PlaneProgress(rig).Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        Assert.That(rig.SliceReporter.Read().FaultedByPhase.Ingesting - before, Is.EqualTo(1));
    }

    [Test]
    public async Task Reactivating_over_a_frozen_nonempty_index_never_counts_advanced()
    {
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 32);
        rig.Start();
        Assert.That(await rig.PumpAsync(), Is.LessThan(MaxTicks));
        var before = rig.SliceReporter.Read();
        for (var i = 0; i < 3; i++)
        {
            rig.Reactivate();
            await rig.Grain.ProcessNextPhaseAsync();
        }

        var after = rig.SliceReporter.Read();
        Assert.Multiple(() =>
        {
            Assert.That(PlaneProgress(rig).VectorsIndexed, Is.EqualTo(32));
            Assert.That(after.Total - before.Total, Is.EqualTo(3));
            Assert.That(after.Advanced - before.Advanced, Is.Zero);
            Assert.That(after.Idle - before.Idle, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task First_step_over_a_nonempty_index_that_banks_vectors_is_advanced()
    {
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 256);
        rig.Start();
        await rig.Grain.EnsureBuildingAsync(Space);
        await rig.Grain.ProcessNextPhaseAsync();
        await rig.Grain.ProcessNextPhaseAsync();
        var progress = PlaneProgress(rig);
        Assert.That(progress.VectorsIndexed, Is.GreaterThan(0));
        Assert.That(progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        var before = rig.SliceReporter.Read().Advanced;

        rig.Reactivate();
        await rig.Grain.ProcessNextPhaseAsync();

        Assert.That(PlaneProgress(rig).VectorsIndexed, Is.GreaterThan(progress.VectorsIndexed));
        Assert.That(rig.SliceReporter.Read().Advanced - before, Is.EqualTo(1));
    }
}
