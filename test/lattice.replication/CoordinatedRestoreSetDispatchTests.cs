using System.Text;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Proves the backup-set (multi-tree) coordinated restore is reachable and group
/// atomic when driven through the <b>real</b> public set-restore entry point
/// (<see cref="ILatticeBackupRestoreService.RestoreSetAsync"/>), the <b>real</b>
/// restore saga dispatcher, the <b>real</b>
/// <see cref="Orleans.Lattice.Replication.Grains.CrossClusterSagaCoordinatorGrain"/>, and the <b>real</b>
/// per-saga participant grain hosting the <b>real</b>
/// <see cref="RestoreParticipant"/> - not a participant-direct call. This is the
/// end-to-end drive-through the participant-direct
/// <see cref="CoordinatedRestoreSetAtomicityTests"/> cannot cover: it asserts the
/// coordinator actually stamps the set id onto every control request, and that the
/// whole set flips together on commit or is compensated together on abort.
/// <para>
/// The set spans a member tree the membership seam reports replicated (so the
/// dispatcher promotes the whole set to one saga) and a local-only member tree
/// (which rides along in the same saga as a local participant). The single silo is
/// the coordinator and hosts every member, so the per-cluster member filter hosts
/// all of them. The loopback control channel records every request so the test can
/// assert the saga carried the set id on the wire.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoordinatedRestoreSetDispatchTests
{
    private const string ReplTree = "dispatch-set-orders@site";
    private const string LocalTree = "dispatch-set-audit@site";
    private const string PostCutKey = "k/zzz-post-cut";

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CoordinatedRestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Set_restore_driven_through_the_coordinator_commits_every_member_together()
    {
        var setId = await ArrangeAdvancedSetAsync(refuseCapacity: false);

        var results = await _fixture.RestoreService.RestoreSetAsync(setId);

        // The public entry returned this cluster's per-member results, one per hosted
        // member tree of the set.
        Assert.That(results, Has.Count.EqualTo(2), "the coordinated set restore reported both member trees");
        Assert.That(
            results.Select(static r => r.TargetTreeId),
            Is.EquivalentTo(new[] { ReplTree, LocalTree }),
            "both member trees were restored");

        // Every member flipped back to its cross-tree-consistent cut together: the
        // post-cut entry each member advanced past the cut is gone on both.
        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await repl.CountAsync(), Is.EqualTo(3), "replicated member reverted to its cut");
            Assert.That(await local.CountAsync(), Is.EqualTo(2), "local-only member reverted to its cut");
            Assert.That(await repl.GetAsync(PostCutKey), Is.Null, "replicated member dropped its post-cut entry");
            Assert.That(await local.GetAsync(PostCutKey), Is.Null, "local-only member dropped its post-cut entry");
        });

        // The saga was driven through the coordinator carrying the set id: every
        // prepare and commit request the coordinator dispatched over the control
        // channel stamped SetId, so the participant took the group-atomic set path.
        var channel = _fixture.ControlChannel!;
        Assert.Multiple(() =>
        {
            Assert.That(channel.Prepared, Is.Not.Empty, "the coordinator dispatched prepare");
            Assert.That(channel.Committed, Is.Not.Empty, "the coordinator dispatched commit");
            Assert.That(
                channel.Prepared.Select(static r => r.SetId),
                Has.All.EqualTo(setId),
                "every prepare request carried the set id");
            Assert.That(
                channel.Committed.Select(static r => r.SetId),
                Has.All.EqualTo(setId),
                "every commit request carried the set id");
            Assert.That(channel.Aborted, Is.Empty, "a committed set never dispatched an abort");
        });
    }

    [Test]
    public async Task Set_restore_driven_through_the_coordinator_compensates_every_member_on_abort()
    {
        // A refusing capacity probe makes the participant vote abort at prepare, so the
        // coordinated saga aborts and no member tree is ever flipped: the set is
        // compensated as one unit, never one committed while another aborts.
        var setId = await ArrangeAdvancedSetAsync(refuseCapacity: true);

        Assert.That(
            async () => await _fixture.RestoreService.RestoreSetAsync(setId),
            Throws.TypeOf<LatticeRestoreValidationException>(),
            "an aborted coordinated set restore surfaces as a validation failure");

        // Neither member flipped: both keep the post-cut entry they advanced past the
        // cut, exactly as before the restore was attempted.
        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await repl.CountAsync(), Is.EqualTo(4), "replicated member is untouched by the aborted set");
            Assert.That(await local.CountAsync(), Is.EqualTo(3), "local-only member is untouched by the aborted set");
            Assert.That(await repl.GetAsync(PostCutKey), Is.Not.Null, "replicated member kept its post-cut entry");
            Assert.That(await local.GetAsync(PostCutKey), Is.Not.Null, "local-only member kept its post-cut entry");
        });

        // The saga still ran through the coordinator carrying the set id (prepare was
        // dispatched), but nothing was committed: no member was left committed while
        // another aborted.
        var channel = _fixture.ControlChannel!;
        Assert.Multiple(() =>
        {
            Assert.That(channel.Prepared, Is.Not.Empty, "the coordinator dispatched prepare");
            Assert.That(
                channel.Prepared.Select(static r => r.SetId),
                Has.All.EqualTo(setId),
                "every prepare request carried the set id");
            Assert.That(channel.Committed, Is.Empty, "an aborted set never dispatched a commit");
        });
    }

    /// <summary>
    /// Deploys the dispatcher-driven cluster (with <see cref="ReplTree"/> reported
    /// replicated), seeds the two member trees, captures them as one cross-tree
    /// consistent set, advances both one entry past the cut, and returns the set id.
    /// </summary>
    private async Task<string> ArrangeAdvancedSetAsync(bool refuseCapacity)
    {
        await _fixture.InitializeAsync(
            driveThroughDispatcher: true,
            replicatedTrees: [ReplTree],
            refuseCapacity: refuseCapacity);

        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);

        await SeedAsync(repl, 3);
        await SeedAsync(local, 2);

        var set = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "nightly",
            [BackupScopeSelector.WholeTree(ReplTree), BackupScopeSelector.WholeTree(LocalTree)],
            crossTreeConsistent: true));

        await repl.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("repl-post"));
        await local.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("local-post"));
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await repl.CountAsync(), Is.EqualTo(4), "replicated member advanced past the cut");
            Assert.That(await local.CountAsync(), Is.EqualTo(3), "local-only member advanced past the cut");
        });

        var setId = set.SetManifest.SetId;
        Assert.That(setId, Is.Not.Null, "a two-tree set records durable membership and so carries a set id");
        return setId!;
    }

    private static async Task SeedAsync(ILattice tree, int count)
    {
        for (var i = 0; i < count; i++)
        {
            var key = $"k/{i:D3}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }
    }
}
