using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression coverage for the physical-tree ownership gate on the
/// shadow-cutover seams. A commit, a revert, and a shadow deletion each act on a
/// <b>physical</b> tree id that arrives on a caller-supplied result, while the
/// authorization above them only ever gates the <b>logical</b> target tree. These
/// tests hold the gate closed: a caller authorized for one tree must not be able
/// to move that tree's registry alias onto - or purge - a physical tree it does
/// not own, which would otherwise expose the other tree's data to every
/// subsequent read and write made under the authorized tree's own policy.
/// </summary>
public sealed partial class LatticeBackupCoordinatedRestoreEngineTests
{
    private const string Victim = "orders-victim";

    /// <summary>
    /// Seeds an unrelated tree the caller is not restoring into, standing in for
    /// another tenant's tree in the cross-tree redirection attempt.
    /// </summary>
    private async Task<string> SeedVictimTreeAsync(string treeId)
    {
        await _fixture.GrainFactory.GetGrain<ILattice>(treeId).SetAsync("secret", Bytes("victim-data"));
        return treeId;
    }

    private ILatticeRegistry Registry =>
        _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    // ---- RevertRestoreAsync ----------------------------------------------

    [Test]
    public async Task RevertRestoreAsync_refuses_a_previous_physical_tree_the_target_does_not_own()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("revert-foreign-previous", ("k1", "v1"));

        const string target = "orders-revert-foreign-previous";
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("live-only", Bytes("pre-cutover"));
        var victim = await SeedVictimTreeAsync($"{Victim}-previous");

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        await Engine.CommitShadowAsync(shadow);

        // The only edit: the retained physical tree is swapped for a tree the
        // caller was never authorized over. Authorization still passes, because
        // the target tree is unchanged.
        var forged = shadow with { PreviousPhysicalTreeId = victim };

        Assert.That(
            async () => await _fixture.Restore.RevertRestoreAsync(forged),
            Throws.InstanceOf<LatticeRestoreValidationException>(),
            "reverting must not redirect the target's alias onto a tree it does not own");

        var targetPhysical = await Registry.ResolveAsync(target);
        var victimPhysical = await Registry.ResolveAsync(victim);
        var leaked = await _fixture.GrainFactory.GetGrain<ILattice>(target).GetAsync("secret");

        Assert.Multiple(() =>
        {
            Assert.That(targetPhysical, Is.EqualTo(shadow.ShadowPhysicalTreeId),
                "the refused revert must leave the alias exactly where the commit put it");
            Assert.That(victimPhysical, Is.EqualTo(victim),
                "the unrelated tree must not have been drawn into the target's alias chain");

            // The decisive check: the target must not have become a window onto
            // the other tree's data.
            Assert.That(leaked, Is.Null, "the target must never serve the unrelated tree's entries");
        });
    }

    [Test]
    public async Task RevertRestoreAsync_refuses_a_shadow_physical_tree_the_target_does_not_own()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("revert-foreign-shadow", ("k1", "v1"));

        const string target = "orders-revert-foreign-shadow";
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("live-only", Bytes("pre-cutover"));
        var victim = await SeedVictimTreeAsync($"{Victim}-shadow");

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        await Engine.CommitShadowAsync(shadow);

        // The revert arms a retained redirect on every shard of the tree named
        // here, so an unvalidated id lets a caller reach another tree's shards.
        var forged = shadow with { ShadowPhysicalTreeId = victim };

        Assert.That(
            async () => await _fixture.Restore.RevertRestoreAsync(forged),
            Throws.InstanceOf<LatticeRestoreValidationException>());

        Assert.That(
            Str((await _fixture.GrainFactory.GetGrain<ILattice>(victim).GetAsync("secret"))!),
            Is.EqualTo("victim-data"),
            "the unrelated tree's shards must be untouched by the refused revert");
    }

    [Test]
    public async Task RevertRestoreAsync_still_accepts_the_result_the_engine_issued()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("revert-genuine", ("k1", "v1"));

        const string target = "orders-revert-genuine";
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("live-only", Bytes("pre-cutover"));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        await Engine.CommitShadowAsync(shadow);

        await _fixture.Restore.RevertRestoreAsync(shadow);

        Assert.That(
            Str((await _fixture.GrainFactory.GetGrain<ILattice>(target).GetAsync("live-only"))!),
            Is.EqualTo("pre-cutover"),
            "the ownership gate must not reject the engine's own unmodified result");
    }

    // ---- CommitShadowAsync -----------------------------------------------

    [Test]
    public async Task CommitShadowAsync_refuses_a_shadow_physical_tree_the_target_does_not_own()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("commit-foreign-shadow", ("k1", "v1"));

        const string target = "orders-commit-foreign-shadow";
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("live-only", Bytes("pre-cutover"));
        var victim = await SeedVictimTreeAsync($"{Victim}-commit");

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));

        // Shape checks (mode, non-null shadow id) all pass; only the ownership
        // of the named physical tree is wrong.
        var forged = shadow with { ShadowPhysicalTreeId = victim };

        Assert.That(
            async () => await Engine.CommitShadowAsync(forged),
            Throws.InstanceOf<LatticeRestoreValidationException>(),
            "committing must not point the target's alias at a tree built for someone else");

        var targetPhysical = await Registry.ResolveAsync(target);
        var leaked = await _fixture.GrainFactory.GetGrain<ILattice>(target).GetAsync("secret");

        Assert.Multiple(() =>
        {
            Assert.That(targetPhysical, Is.Not.EqualTo(victim));
            Assert.That(leaked, Is.Null, "the target must never serve the unrelated tree's entries");
        });
    }

    [Test]
    public async Task CommitShadowAsync_refuses_a_shadow_built_for_a_different_target_tree()
    {
        await _fixture.InitializeAsync();
        var backupId = await SeedAndCaptureAsync("commit-crossed-target", ("k1", "v1"));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, "orders-crossed-a", scope: null, mode: LatticeRestoreMode.ShadowCutover));

        // A shadow genuinely built for tree A, re-aimed at tree B. The shadow
        // carries A as its registry provenance, so B cannot adopt it.
        var forged = shadow with { TargetTreeId = "orders-crossed-b" };

        Assert.That(
            async () => await Engine.CommitShadowAsync(forged),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    // ---- DeleteShadowAsync -----------------------------------------------

    [Test]
    public async Task DeleteShadowAsync_refuses_a_registered_tree_that_is_not_a_restore_shadow()
    {
        await _fixture.InitializeAsync();
        var victim = await SeedVictimTreeAsync($"{Victim}-delete");

        Assert.That(await Registry.ExistsAsync(victim), Is.True,
            "the tree must be registered, so the idempotent never-built short-circuit does not apply");

        Assert.That(
            async () => await Engine.DeleteShadowAsync(victim),
            Throws.InstanceOf<LatticeRestoreValidationException>(),
            "the garbage-collection seam must only ever delete a tree the engine stamped as a shadow");

        var stillRegistered = await Registry.ExistsAsync(victim);
        var survivingValue = await _fixture.GrainFactory.GetGrain<ILattice>(victim).GetAsync("secret");

        Assert.Multiple(() =>
        {
            Assert.That(stillRegistered, Is.True, "the refused delete must leave the tree registered");
            Assert.That(survivingValue, Is.Not.Null.And.EqualTo(Bytes("victim-data")),
                "the refused delete must not have purged any shard");
        });
    }
}
