using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end coverage for the <see cref="ILatticeCoordinatedRestoreEngine"/>
/// seams the restore service exposes to the coordinated-restore saga. These are
/// the two-phase primitives a cross-cluster restore drives - probe admission,
/// build the shadow, then commit or abandon it - and they are otherwise only
/// reached through the replication add-on's saga, so they are driven directly
/// here against a live single-silo cluster.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupCoordinatedRestoreEngineTests
{
    private const string Source = "orders";

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private ILatticeCoordinatedRestoreEngine Engine =>
        (ILatticeCoordinatedRestoreEngine)_fixture.Restore;

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);

    private async Task<string> SeedAndCaptureAsync(string label, params (string Key, string Value)[] entries)
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        foreach (var (key, value) in entries)
        {
            await source.SetAsync(key, Bytes(value));
        }

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest(label, BackupScopeSelector.WholeTree(Source)));
        return backup.BackupId;
    }

    // ---- ProbeAdmissionAsync --------------------------------------------

    [Test]
    public async Task ProbeAdmissionAsync_reports_the_chain_size_and_topology()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("probe", ("k1", "v1"), ("k2", "v2"));

        var report = await Engine.ProbeAdmissionAsync(
            new LatticeRestoreRequest(backupId, "orders-probe"));

        Assert.Multiple(() =>
        {
            Assert.That(report.BackupId, Is.EqualTo(backupId));
            Assert.That(report.TargetTreeId, Is.EqualTo("orders-probe"));
            Assert.That(report.ManifestChain, Is.EqualTo(new[] { backupId }));
            Assert.That(report.ShardCount, Is.GreaterThan(0));
            Assert.That(report.TotalByteLength, Is.GreaterThan(0),
                "a non-empty capture must report a non-zero materialisation cost");
            Assert.That(report.TotalChunkCount, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task ProbeAdmissionAsync_defaults_the_target_tree_to_the_captured_scope()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("probe-default", ("k1", "v1"));

        var report = await Engine.ProbeAdmissionAsync(new LatticeRestoreRequest(backupId));

        Assert.That(report.TargetTreeId, Is.EqualTo(Source));
    }

    [Test]
    public async Task ProbeAdmissionAsync_sums_the_whole_incremental_chain()
    {
        await _fixture.InitializeAsync();
        var baseId = await SeedAndCaptureAsync("probe-base", ("k1", "v1"));

        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k2", Bytes("v2"));
        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "probe-inc", BackupScopeSelector.WholeTree(Source), baseId));

        var report = await Engine.ProbeAdmissionAsync(new LatticeRestoreRequest(increment.BackupId));

        Assert.That(report.ManifestChain, Is.EqualTo(new[] { baseId, increment.BackupId }),
            "the probe must report the base-first chain the shadow build would replay");
    }

    [Test]
    public async Task ProbeAdmissionAsync_rejects_an_unknown_backup_before_any_work()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await Engine.ProbeAdmissionAsync(new LatticeRestoreRequest("no-such-backup", "t")),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    [Test]
    public async Task ProbeAdmissionAsync_rejects_a_null_request()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await Engine.ProbeAdmissionAsync(null!),
            Throws.ArgumentNullException);
    }

    // ---- BuildShadowAsync ------------------------------------------------

    [Test]
    public async Task BuildShadowAsync_materialises_the_shadow_without_swapping_the_alias()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("build", ("k1", "v1"), ("k2", "v2"));

        const string target = "orders-build";
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await live.SetAsync("live-only", Bytes("still-here"));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));

        Assert.Multiple(() =>
        {
            Assert.That(shadow.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            Assert.That(shadow.ShadowPhysicalTreeId, Is.Not.Null.And.Not.Empty);
            Assert.That(shadow.EntriesApplied, Is.EqualTo(2));
            Assert.That(shadow.TargetTreeId, Is.EqualTo(target));
        });

        // The alias still resolves to the live tree: the build phase must be
        // invisible to readers until the commit phase swaps it.
        Assert.Multiple(() =>
        {
            Assert.That(Str(live.GetAsync("live-only").Result!), Is.EqualTo("still-here"));
            Assert.That(live.GetAsync("k1").Result, Is.Null,
                "the restored entries must not be visible before the cutover is committed");
        });
    }

    [Test]
    public async Task BuildShadowAsync_rejects_a_non_shadow_request()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("build-mode", ("k1", "v1"));

        Assert.That(
            async () => await Engine.BuildShadowAsync(
                new LatticeRestoreRequest(backupId, "t", scope: null, mode: LatticeRestoreMode.InPlace)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task BuildShadowAsync_rejects_a_null_request()
    {
        await _fixture.InitializeAsync();

        Assert.That(async () => await Engine.BuildShadowAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task BuildShadowAsync_rejects_an_unknown_backup()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await Engine.BuildShadowAsync(new LatticeRestoreRequest(
                "no-such-backup", "t", scope: null, mode: LatticeRestoreMode.ShadowCutover)),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    // ---- CommitShadowAsync -----------------------------------------------

    [Test]
    public async Task CommitShadowAsync_swaps_the_alias_onto_the_built_shadow()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("commit", ("k1", "v1"), ("k2", "v2"));

        const string target = "orders-commit";
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await live.SetAsync("live-only", Bytes("pre-cutover"));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));

        await Engine.CommitShadowAsync(shadow);

        var after = _fixture.GrainFactory.GetGrain<ILattice>(target);
        Assert.Multiple(() =>
        {
            Assert.That(Str(after.GetAsync("k1").Result!), Is.EqualTo("v1"));
            Assert.That(Str(after.GetAsync("k2").Result!), Is.EqualTo("v2"));
            Assert.That(after.GetAsync("live-only").Result, Is.Null,
                "after the cutover the alias must resolve to the restored shadow, not the old tree");
        });
    }

    [Test]
    public async Task CommitShadowAsync_leaves_the_previous_physical_tree_available_to_revert_to()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("commit-revert", ("k1", "v1"));

        const string target = "orders-commit-revert";
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await live.SetAsync("live-only", Bytes("pre-cutover"));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        await Engine.CommitShadowAsync(shadow);

        Assert.That(shadow.PreviousPhysicalTreeId, Is.Not.Null);

        await _fixture.Restore.RevertRestoreAsync(shadow);

        var reverted = _fixture.GrainFactory.GetGrain<ILattice>(target);
        Assert.That(Str(reverted.GetAsync("live-only").Result!), Is.EqualTo("pre-cutover"),
            "the commit must retain the previous physical tree so a revert restores it");
    }

    [Test]
    public async Task CommitShadowAsync_rejects_a_result_that_is_not_a_shadow_build()
    {
        await _fixture.InitializeAsync();
        var inPlace = new LatticeRestoreResult(
            "b", "t", LatticeRestoreMode.InPlace, "op", ["b"], 0);

        Assert.That(
            async () => await Engine.CommitShadowAsync(inPlace),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task CommitShadowAsync_rejects_a_shadow_result_with_no_physical_tree()
    {
        await _fixture.InitializeAsync();
        var missingShadow = new LatticeRestoreResult(
            "b", "t", LatticeRestoreMode.ShadowCutover, "op", ["b"], 0);

        Assert.That(
            async () => await Engine.CommitShadowAsync(missingShadow),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task CommitShadowAsync_rejects_a_null_result()
    {
        await _fixture.InitializeAsync();

        Assert.That(async () => await Engine.CommitShadowAsync(null!), Throws.ArgumentNullException);
    }

    // ---- DeleteShadowAsync -----------------------------------------------

    [Test]
    public async Task DeleteShadowAsync_garbage_collects_an_abandoned_shadow()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("abandon", ("k1", "v1"));

        const string target = "orders-abandon";
        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));

        var registry = _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        Assert.That(await registry.ExistsAsync(shadow.ShadowPhysicalTreeId!), Is.True,
            "the built shadow must be registered before it is abandoned");

        // The saga abandons the build rather than committing it.
        await Engine.DeleteShadowAsync(shadow.ShadowPhysicalTreeId!);

        Assert.That(await registry.ExistsAsync(shadow.ShadowPhysicalTreeId!), Is.False,
            "an abandoned shadow must be unregistered so it cannot leak");
    }

    [Test]
    public async Task DeleteShadowAsync_is_idempotent_for_a_shadow_that_never_existed()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await Engine.DeleteShadowAsync("never-built-shadow-tree"),
            Throws.Nothing,
            "a shadow that was never built, or was already collected, is a no-op");
    }

    [Test]
    public async Task DeleteShadowAsync_run_twice_stays_a_no_op()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("abandon-twice", ("k1", "v1"));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, "orders-abandon-twice", scope: null, mode: LatticeRestoreMode.ShadowCutover));

        await Engine.DeleteShadowAsync(shadow.ShadowPhysicalTreeId!);

        Assert.That(
            async () => await Engine.DeleteShadowAsync(shadow.ShadowPhysicalTreeId!),
            Throws.Nothing);
    }

    [TestCase(null)]
    [TestCase("")]
    public async Task DeleteShadowAsync_rejects_a_missing_shadow_id(string? shadowId)
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await Engine.DeleteShadowAsync(shadowId!),
            Throws.InstanceOf<ArgumentException>());
    }

    // ---- ResolveShadowTreeId ---------------------------------------------

    [Test]
    public async Task ResolveShadowTreeId_is_deterministic_for_the_same_request()
    {
        await _fixture.InitializeAsync();
        var request = new LatticeRestoreRequest(
            "backup-x", "orders-resolve", scope: null, mode: LatticeRestoreMode.ShadowCutover);

        Assert.That(
            Engine.ResolveShadowTreeId(request),
            Is.EqualTo(Engine.ResolveShadowTreeId(request)),
            "every peer in a coordinated restore must derive the same shadow tree id");
    }

    [Test]
    public async Task ResolveShadowTreeId_differs_per_target_tree()
    {
        await _fixture.InitializeAsync();

        var a = Engine.ResolveShadowTreeId(new LatticeRestoreRequest(
            "backup-x", "tree-a", scope: null, mode: LatticeRestoreMode.ShadowCutover));
        var b = Engine.ResolveShadowTreeId(new LatticeRestoreRequest(
            "backup-x", "tree-b", scope: null, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public async Task ResolveShadowTreeId_requires_an_explicit_target_tree_id()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            () => Engine.ResolveShadowTreeId(new LatticeRestoreRequest("backup-x")),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ResolveShadowTreeId_rejects_a_null_request()
    {
        await _fixture.InitializeAsync();

        Assert.That(() => Engine.ResolveShadowTreeId(null!), Throws.ArgumentNullException);
    }
}
