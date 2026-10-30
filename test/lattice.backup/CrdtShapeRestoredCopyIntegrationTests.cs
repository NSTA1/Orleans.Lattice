using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression test for #5300: a shadow-cutover restore cuts a logical tree over to a
/// derived physical copy (<c>{tree}-bkprestore-{op}</c>), and the copy's leaves
/// resolved the OR-Map shape by that physical id rather than through the logical
/// tree the shape is registered under, so every OR-Map write after the restore threw
/// <see cref="LatticeCrdtShapeNotRegisteredException"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrdtShapeRestoredCopyIntegrationTests
{
    private const string Source = "csr-source";
    private const string Target = "csr-target";
    private const string Key = "row";

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync(static builder => builder.AddSiloBuilderConfigurator<OrMapShapeConfigurator>());
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Or_map_write_after_a_shadow_cutover_restore_folds_with_the_logical_tree_shape()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await Map(source).SetAsync("a", "r1", Counter("r1", 1));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("csr", BackupScopeSelector.WholeTree(Source)));

        var target = _fixture.GrainFactory.GetGrain<ILattice>(Target);
        await Map(target).SetAsync("live", "r1", Counter("r1", 9));
        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, Target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(result.ShadowPhysicalTreeId, Is.Not.EqualTo(Target), "precondition: the tree now resolves to the restored copy");

        await Map(target).SetAsync("b", "r1", Counter("r1", 2));

        var map = await Map(target).GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(map.ContainsKey("a"), Is.True, "the restored row survives");
            Assert.That(map.ContainsKey("b"), Is.True, "an OR-Map write on the restored copy folds with the tree's shape");
        });
    }

    private static OrMapAccessor<string, PnCounter> Map(ILattice lattice) => lattice.OrMap<string, PnCounter>(Key);

    private static PnCounter Counter(string replica, long amount)
    {
        var counter = new PnCounter();
        counter.Increment(replica, amount);
        return counter;
    }

    private sealed class OrMapShapeConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddOrMapShape<string, PnCounter>(Source);
            siloBuilder.AddOrMapShape<string, PnCounter>(Target);
        }
    }
}
