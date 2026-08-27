using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the restore-into-tree verbs on <see cref="LatticeTreeAdmin"/>
/// (<c>RestoreTree</c> / <c>RestoreTreeSet</c> / <c>RevertTreeRestore</c>), which
/// compose the public <see cref="ILatticeBackupRestoreService"/>. Each single-tree
/// verb authorizes the whole-tree <c>Restore</c> capability fail-closed and forces a
/// reversible shadow-cutover; the set verb delegates without a facade-level gate (the
/// engine authorizes each member). Driven purely with substitutes - no cluster, no
/// real backup engine.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminRestoreTests
{
    private const string Tree = "orders";
    private const string Backup = "bk-2024";
    private const string Op = "restore-1";

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(
        ILatticeBackupRestoreService? restore, bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            Substitute.For<IGrainFactory>(),
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver(),
            restore);

    private static LatticeRestoreResult Result(
        string targetTreeId = Tree,
        string backupId = Backup,
        string operationId = Op,
        string? shadow = "phys-new",
        string? previous = "phys-old")
        => new(
            backupId,
            targetTreeId,
            LatticeRestoreMode.ShadowCutover,
            operationId,
            ["base", backupId],
            entriesApplied: 5,
            shadowPhysicalTreeId: shadow,
            previousPhysicalTreeId: previous);

    // ----- RestoreTreeAsync -----

    [Test]
    public async Task RestoreTreeAsync_forces_shadow_cutover_and_maps_the_result()
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        service.RestoreAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(Result());
        var facade = Create(service);

        var result = await facade.RestoreTreeAsync(Tree, Backup, Op);

        await service.Received(1).RestoreAsync(
            Arg.Is<LatticeRestoreRequest>(r =>
                r.BackupId == Backup
                && r.TargetTreeId == Tree
                && r.Mode == LatticeRestoreMode.ShadowCutover
                && r.OperationId == Op),
            Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(result.BackupId, Is.EqualTo(Backup));
            Assert.That(result.TargetTreeId, Is.EqualTo(Tree));
            Assert.That(result.Mode, Is.EqualTo(TreeRestoreMode.ShadowCutover));
            Assert.That(result.OperationId, Is.EqualTo(Op));
            Assert.That(result.ManifestChain, Is.EqualTo(new[] { "base", Backup }));
            Assert.That(result.EntriesApplied, Is.EqualTo(5));
            Assert.That(result.ShadowPhysicalTreeId, Is.EqualTo("phys-new"));
            Assert.That(result.PreviousPhysicalTreeId, Is.EqualTo("phys-old"));
        });
    }

    [Test]
    public void RestoreTreeAsync_denied_by_gate_throws_and_does_not_call_service()
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        var facade = Create(service, allow: false);

        Assert.That(async () => await facade.RestoreTreeAsync(Tree, Backup, Op),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        service.DidNotReceive().RestoreAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void RestoreTreeAsync_reserved_tree_id_is_rejected()
    {
        var facade = Create(Substitute.For<ILatticeBackupRestoreService>());

        Assert.That(async () => await facade.RestoreTreeAsync(LatticeConstants.SystemTreePrefix + "t", Backup, Op),
            Throws.ArgumentException);
    }

    [Test]
    public void RestoreTreeAsync_null_or_empty_arguments_throw()
    {
        var facade = Create(Substitute.For<ILatticeBackupRestoreService>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.RestoreTreeAsync(null!, Backup), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.RestoreTreeAsync("", Backup), Throws.ArgumentException);
            Assert.That(async () => await facade.RestoreTreeAsync(Tree, null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.RestoreTreeAsync(Tree, ""), Throws.ArgumentException);
            Assert.That(async () => await facade.RestoreTreeAsync(Tree, Backup, ""), Throws.ArgumentException);
        });
    }

    [Test]
    public void RestoreTreeAsync_without_restore_engine_throws_InvalidOperation()
    {
        var facade = Create(restore: null);

        Assert.That(async () => await facade.RestoreTreeAsync(Tree, Backup, Op),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- RestoreTreeSetAsync -----

    [Test]
    public async Task RestoreTreeSetAsync_delegates_and_maps_each_member()
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        service.RestoreSetAsync("set-1", Arg.Any<CancellationToken>())
            .Returns(new[] { Result("a"), Result("b") });
        var facade = Create(service);

        var results = await facade.RestoreTreeSetAsync("set-1");

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(results[0].TargetTreeId, Is.EqualTo("a"));
            Assert.That(results[1].TargetTreeId, Is.EqualTo("b"));
        });
    }

    [Test]
    public async Task RestoreTreeSetAsync_applies_no_facade_level_gate()
    {
        // The set spans multiple member trees, so the facade applies no whole-tree
        // gate of its own; the engine authorizes each member. A denying gate must not
        // block the delegation.
        var service = Substitute.For<ILatticeBackupRestoreService>();
        service.RestoreSetAsync("set-1", Arg.Any<CancellationToken>())
            .Returns(new[] { Result("a") });
        var facade = Create(service, allow: false);

        var results = await facade.RestoreTreeSetAsync("set-1");

        Assert.That(results, Has.Count.EqualTo(1));
    }

    [Test]
    public void RestoreTreeSetAsync_null_or_empty_set_id_throws()
    {
        var facade = Create(Substitute.For<ILatticeBackupRestoreService>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.RestoreTreeSetAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.RestoreTreeSetAsync(""), Throws.ArgumentException);
        });
    }

    [Test]
    public void RestoreTreeSetAsync_without_restore_engine_throws_InvalidOperation()
    {
        var facade = Create(restore: null);

        Assert.That(async () => await facade.RestoreTreeSetAsync("set-1"),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- RevertTreeRestoreAsync -----

    [Test]
    public async Task RevertTreeRestoreAsync_reconstructs_the_result_and_delegates()
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        var facade = Create(service);
        var restore = new TreeRestoreResult
        {
            BackupId = Backup,
            TargetTreeId = Tree,
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = Op,
            ManifestChain = ["base", Backup],
            EntriesApplied = 5,
            ShadowPhysicalTreeId = "phys-new",
            PreviousPhysicalTreeId = "phys-old",
        };

        await facade.RevertTreeRestoreAsync(restore);

        await service.Received(1).RevertRestoreAsync(
            Arg.Is<LatticeRestoreResult>(r =>
                r.TargetTreeId == Tree
                && r.Mode == LatticeRestoreMode.ShadowCutover
                && r.PreviousPhysicalTreeId == "phys-old"
                && r.ShadowPhysicalTreeId == "phys-new"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void RevertTreeRestoreAsync_denied_by_gate_throws_and_does_not_call_service()
    {
        var service = Substitute.For<ILatticeBackupRestoreService>();
        var facade = Create(service, allow: false);

        Assert.That(async () => await facade.RevertTreeRestoreAsync(ToDto(Result())),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        service.DidNotReceive().RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void RevertTreeRestoreAsync_null_restore_throws()
    {
        var facade = Create(Substitute.For<ILatticeBackupRestoreService>());

        Assert.That(async () => await facade.RevertTreeRestoreAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void RevertTreeRestoreAsync_reserved_target_tree_id_is_rejected()
    {
        var facade = Create(Substitute.For<ILatticeBackupRestoreService>());

        Assert.That(async () => await facade.RevertTreeRestoreAsync(ToDto(Result(targetTreeId: LatticeConstants.SystemTreePrefix + "t"))),
            Throws.ArgumentException);
    }

    [Test]
    public void RevertTreeRestoreAsync_without_restore_engine_throws_InvalidOperation()
    {
        var facade = Create(restore: null);

        Assert.That(async () => await facade.RevertTreeRestoreAsync(ToDto(Result())),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- ProbeCapabilities: CanRestore -----

    [Test]
    public async Task ProbeCapabilities_reports_CanRestore_true_when_engine_present_and_allowed()
    {
        var facade = Create(Substitute.For<ILatticeBackupRestoreService>(), allow: true);

        var caps = await facade.ProbeCapabilitiesAsync(Tree);

        Assert.That(caps.CanRestore, Is.True);
    }

    [Test]
    public async Task ProbeCapabilities_reports_CanRestore_false_when_no_engine()
    {
        var facade = Create(restore: null, allow: true);

        var caps = await facade.ProbeCapabilitiesAsync(Tree);

        Assert.That(caps.CanRestore, Is.False);
    }

    [Test]
    public async Task ProbeCapabilities_reports_CanRestore_false_when_denied()
    {
        var facade = Create(Substitute.For<ILatticeBackupRestoreService>(), allow: false);

        var caps = await facade.ProbeCapabilitiesAsync(Tree);

        Assert.That(caps.CanRestore, Is.False);
    }

    private static TreeRestoreResult ToDto(LatticeRestoreResult r) => new()
    {
        BackupId = r.BackupId,
        TargetTreeId = r.TargetTreeId,
        Mode = TreeRestoreMode.ShadowCutover,
        OperationId = r.OperationId,
        ManifestChain = r.ManifestChain,
        EntriesApplied = r.EntriesApplied,
        ShadowPhysicalTreeId = r.ShadowPhysicalTreeId,
        PreviousPhysicalTreeId = r.PreviousPhysicalTreeId,
    };
}
