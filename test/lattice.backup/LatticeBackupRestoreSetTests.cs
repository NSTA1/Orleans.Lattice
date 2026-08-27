using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end coverage for
/// <see cref="ILatticeBackupRestoreService.RestoreSetAsync"/>: the multi-tree
/// restore that expands a captured backup set into its member trees and restores
/// each one via shadow-cutover. On a single-cluster host the coordinated saga
/// dispatcher declines, so this drives the local per-member path.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupRestoreSetTests
{
    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);

    /// <summary>
    /// Waits until the catalog scan the set resolver reads through has caught up
    /// with the freshly-registered member manifests. The registration is awaited
    /// inside the capture, but the catalog is read back through an ordinary tree
    /// scan, so a just-written member can take a moment to become visible.
    /// </summary>
    private async Task AwaitSetMembersAsync(string setId, int expected)
    {
        var resolver = _fixture.SiloServices.GetRequiredService<ILatticeBackupSetResolver>();
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < 10_000)
        {
            var members = await resolver.ResolveMembersAsync(setId);
            if (members.Count >= expected)
            {
                return;
            }

            await Task.Delay(50);
        }

        Assert.Fail($"The catalog never listed {expected} member(s) for set '{setId}'.");
    }

    [Test]
    public async Task RestoreSetAsync_restores_every_member_tree_of_the_set()
    {
        await _fixture.InitializeAsync();
        const string treeA = "set-orders";
        const string treeB = "set-invoices";
        var a = _fixture.GrainFactory.GetGrain<ILattice>(treeA);
        var b = _fixture.GrainFactory.GetGrain<ILattice>(treeB);
        await a.SetAsync("k", Bytes("a-captured"));
        await b.SetAsync("k", Bytes("b-captured"));

        var set = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "restore-set",
            [BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB)]));

        // Drift both trees after the capture so a successful restore is visible.
        await a.SetAsync("k", Bytes("a-drifted"));
        await b.SetAsync("k", Bytes("b-drifted"));

        await AwaitSetMembersAsync(set.SetManifest.SetId, 2);
        var results = await _fixture.Restore.RestoreSetAsync(set.SetManifest.SetId);

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(
                results.Select(r => r.TargetTreeId),
                Is.EquivalentTo(new[] { treeA, treeB }));
            Assert.That(
                results.Select(r => r.Mode),
                Is.All.EqualTo(LatticeRestoreMode.ShadowCutover),
                "a set restore cuts every member over atomically per tree");
        });

        var restoredA = _fixture.GrainFactory.GetGrain<ILattice>(treeA);
        var restoredB = _fixture.GrainFactory.GetGrain<ILattice>(treeB);
        Assert.Multiple(() =>
        {
            Assert.That(Str(restoredA.GetAsync("k").Result!), Is.EqualTo("a-captured"));
            Assert.That(Str(restoredB.GetAsync("k").Result!), Is.EqualTo("b-captured"));
        });
    }

    [Test]
    public async Task RestoreSetAsync_returns_one_result_per_member_carrying_its_shadow_tree()
    {
        await _fixture.InitializeAsync();
        const string treeA = "set-shadow-a";
        const string treeB = "set-shadow-b";
        var a = _fixture.GrainFactory.GetGrain<ILattice>(treeA);
        var b = _fixture.GrainFactory.GetGrain<ILattice>(treeB);
        await a.SetAsync("k1", Bytes("v1"));
        await b.SetAsync("k1", Bytes("v1"));

        var set = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "shadow-set",
            [BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB)]));

        await AwaitSetMembersAsync(set.SetManifest.SetId, 2);
        var results = await _fixture.Restore.RestoreSetAsync(set.SetManifest.SetId);

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(
                results.Select(r => r.ShadowPhysicalTreeId),
                Is.All.Not.Null,
                "every member cuts over through its own shadow physical tree");
            Assert.That(
                results.Select(r => r.ShadowPhysicalTreeId).Distinct().Count(),
                Is.EqualTo(2),
                "member shadows must not collide");
            Assert.That(results.Select(r => r.EntriesApplied), Is.All.EqualTo(1));
        });
    }

    [Test]
    public async Task A_single_member_set_is_not_restorable_as_a_set()
    {
        await _fixture.InitializeAsync();
        const string tree = "set-solo";
        var solo = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await solo.SetAsync("k1", Bytes("v1"));

        var set = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "solo-set", [BackupScopeSelector.WholeTree(tree)]));

        // By design a single-member set is left unstamped - it is
        // indistinguishable from a plain backup and lists as one - so it resolves
        // to no set members and must be restored as an ordinary backup instead.
        Assert.That(
            async () => await _fixture.Restore.RestoreSetAsync(set.SetManifest.SetId),
            Throws.InstanceOf<ArgumentException>());

        var member = set.Members.Single();
        Assert.That(member.Manifest.SetId, Is.Null,
            "a one-member set must not stamp set membership onto its only manifest");

        var restored = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(member.BackupId, "set-solo-restored"));
        Assert.That(restored.EntriesApplied, Is.EqualTo(1));
    }

    [Test]
    public async Task RestoreSetAsync_rejects_an_unknown_set_id()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await _fixture.Restore.RestoreSetAsync("no-such-set"),
            Throws.InstanceOf<ArgumentException>(),
            "a set that resolves to no member trees is a caller error, not an empty success");
    }

    [TestCase(null)]
    [TestCase("")]
    public async Task RestoreSetAsync_rejects_a_missing_set_id(string? setId)
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await _fixture.Restore.RestoreSetAsync(setId!),
            Throws.InstanceOf<ArgumentException>());
    }
}
