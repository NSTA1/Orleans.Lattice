using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Proves the backup-set (multi-tree) coordinated restore is <b>group atomic</b>:
/// every tree in the set flips together, or none does. The set spans a tree that
/// stands in for a replicated tree and a tree that stands in for a local-only tree
/// (both restore identically at the participant; the replicated/local distinction
/// only changes which clusters the coordinator dispatches to, which is asserted at
/// the dispatch layer, not here). The restore runs over the <b>real</b> restore
/// engine, the <b>real</b> backup set read seam
/// (<see cref="ILatticeBackupSetResolver"/>), and the <b>real</b> durable group
/// write-fence grain via the <b>real</b> <see cref="RestoreParticipant"/>.
/// <para>
/// A set is signalled to the participant by <see cref="SagaControlRequest.SetId"/>;
/// the participant expands it into the member trees via the read seam and fences,
/// builds, commits, or aborts all of them as one group. The two tests below pin the
/// two directions of the all-or-nothing property: commit flips every member, and
/// abort compensates every member (no tree is ever left committed while another
/// aborts).
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoordinatedRestoreSetAtomicityTests
{
    private const string ReplTree = "set-orders@site";
    private const string LocalTree = "set-audit@site";
    private const string SagaId = "restore-set-nightly";

    private const string PostCutKey = "k/zzz-post-cut";

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CoordinatedRestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Set_restore_commit_flips_every_member_tree_together()
    {
        var setId = await ArrangeAdvancedSetAsync();
        var participant = _fixture.SiloServices.GetRequiredService<RestoreParticipant>();
        var request = SetRequest(setId);

        var vote = await participant.PrepareAsync(request);
        Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit), "the whole set prepared");

        await participant.CommitAsync(request);

        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);
        Assert.Multiple(() =>
        {
            Assert.That(repl.CountAsync().Result, Is.EqualTo(3), "replicated member reverted to its cut");
            Assert.That(local.CountAsync().Result, Is.EqualTo(2), "local-only member reverted to its cut");
            Assert.That(repl.GetAsync(PostCutKey).Result, Is.Null, "replicated member dropped its post-cut entry");
            Assert.That(local.GetAsync(PostCutKey).Result, Is.Null, "local-only member dropped its post-cut entry");
        });

        // One shared fence covers the whole group; shipping stays globally gated for
        // every member together until the saga completes.
        var snapshot = await _fixture.Fence(SagaId).GetSnapshotAsync();
        Assert.That(snapshot.ShippingResumed, Is.False,
            "the group's shipping is globally gated until the set saga completes");
    }

    [Test]
    public async Task Set_restore_abort_compensates_every_member_tree_together()
    {
        var setId = await ArrangeAdvancedSetAsync();
        var participant = _fixture.SiloServices.GetRequiredService<RestoreParticipant>();
        var request = SetRequest(setId);

        // Prepare builds every member's shadow unfenced (no alias is swapped yet).
        var vote = await participant.PrepareAsync(request);
        Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit));

        // Abort before commit: the group is compensated as one unit - every member's
        // shadow is garbage collected and no member's alias is swapped, so both trees
        // stay exactly as they were. No tree is left committed while another aborts.
        await participant.AbortAsync(request);

        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);
        Assert.Multiple(() =>
        {
            Assert.That(repl.CountAsync().Result, Is.EqualTo(4), "replicated member is untouched by the aborted set");
            Assert.That(local.CountAsync().Result, Is.EqualTo(3), "local-only member is untouched by the aborted set");
            Assert.That(repl.GetAsync(PostCutKey).Result, Is.Not.Null, "replicated member kept its post-cut entry");
            Assert.That(local.GetAsync(PostCutKey).Result, Is.Not.Null, "local-only member kept its post-cut entry");
        });
    }

    /// <summary>
    /// Seeds two trees (three and two entries respectively), captures them as one
    /// cross-tree-consistent set, then advances both one entry past the cut, and
    /// returns the captured set id.
    /// </summary>
    private async Task<string> ArrangeAdvancedSetAsync()
    {
        await _fixture.InitializeAsync();

        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);

        await SeedAsync(repl, 3);
        await SeedAsync(local, 2);

        var set = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "nightly",
            [BackupScopeSelector.WholeTree(ReplTree), BackupScopeSelector.WholeTree(LocalTree)],
            crossTreeConsistent: true));

        // Both members advance one entry past the cut, so a naive restore of only one
        // member would leave the set inconsistent.
        await repl.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("repl-post"));
        await local.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("local-post"));
        Assert.Multiple(() =>
        {
            Assert.That(repl.CountAsync().Result, Is.EqualTo(4), "replicated member advanced past the cut");
            Assert.That(local.CountAsync().Result, Is.EqualTo(3), "local-only member advanced past the cut");
        });

        return set.SetManifest.SetId;
    }

    private static SagaControlRequest SetRequest(string setId) =>
        new()
        {
            SagaId = SagaId,
            // For a set restore the tree/manifest slots are unused; the SetId drives
            // the participant, which expands the member trees from the read seam.
            TargetTree = setId,
            ManifestId = setId,
            CoordinatorClusterId = CoordinatedRestoreClusterFixture.ClusterId,
            SetId = setId,
        };

    private static async Task SeedAsync(ILattice tree, int count)
    {
        for (var i = 0; i < count; i++)
        {
            var key = $"k/{i:D3}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }
    }
}
