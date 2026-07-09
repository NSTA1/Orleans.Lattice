using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;

namespace Orleans.Lattice.Backup.Tests.Chaos;

/// <summary>
/// Chaos coverage of the tag-index reconcile fired by a shadow-cutover restore.
/// A tag index is maintained inline in a sibling index tree, so a shadow-cutover
/// restore that reverts the subject tree's contents leaves membership rows for
/// keys absent from the restored point-in-time until a reconcile runs. The
/// restore fires a prompt reconcile; this suite drives that path under a large
/// working set, repeated restores to successively earlier point-in-times, and a
/// concurrent reader continuously issuing tag queries, and pins that membership
/// converges to exactly the restored subject with no orphaned rows surviving.
/// </summary>
/// <remarks>
/// Complements the deterministic single-restore reconcile regression that landed
/// with the fix. The concurrent reader ensures the reconcile is safe against
/// readers observing the index mid-sweep (no torn membership, no exceptions).
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class BackupRestoreReconcileChaosTests
{
    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<List<string>> RedKeysAsync(ILatticeTagIndex index)
    {
        var keys = new List<string>();
        await foreach (var key in index.WithAnyTags("red"))
        {
            keys.Add(key);
        }
        return keys;
    }

    // A single shadow-cutover restore over a large tagged working set, with a
    // concurrent reader hammering the tag query, must reconcile away every
    // membership row absent from the restored point-in-time.
    [Test]
    public async Task Restore_reconciles_large_tag_membership_under_concurrent_reads()
    {
        await _fixture.InitializeAsync();

        // Point-in-time: 20 'keep' keys only.
        const string source = "chaos-recon-src";
        var sourceTree = _fixture.GrainFactory.GetGrain<ILattice>(source);
        var kept = Enumerable.Range(0, 20).Select(i => $"keep-{i:D2}").ToArray();
        foreach (var k in kept) await sourceTree.SetAsync(k, Bytes($"v-{k}"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("chaos-recon", BackupScopeSelector.WholeTree(source)));

        // Live target: the 20 kept keys plus 100 'gone' keys, all tagged red.
        const string target = "chaos-recon-live";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeTagIndexFactory>();
        var index = factory.Create(_fixture.GrainFactory.GetGrain<ILattice>(target), "chaos-recon-colors");
        foreach (var k in kept) await index.SetValueWithTags(k, Bytes($"v-{k}"), "red").CommitAsync();
        var gone = Enumerable.Range(0, 100).Select(i => $"gone-{i:D3}").ToArray();
        foreach (var k in gone) await index.SetValueWithTags(k, Bytes($"v-{k}"), "red").CommitAsync();

        Assert.That(await RedKeysAsync(index), Has.Count.EqualTo(120),
            "All tagged keys must be indexed before the restore.");

        // A concurrent reader hammers the tag query across the reconcile.
        using var readerCts = new CancellationTokenSource();
        var universe = kept.Concat(gone).ToArray();
        var reader = Task.Run(async () =>
        {
            while (!readerCts.IsCancellationRequested)
            {
                // Must never throw or observe a key outside the known universe.
                var snapshot = await RedKeysAsync(index);
                Assert.That(snapshot, Is.SubsetOf(universe),
                    "A concurrent reader must never observe an out-of-universe key.");
            }
        });

        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));

        readerCts.Cancel();
        await reader;

        var after = await RedKeysAsync(index);
        Assert.That(after, Is.EquivalentTo(kept),
            "The reconcile fired by the restore must drop every membership row absent from the restored subject.");

        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        foreach (var k in gone)
        {
            Assert.That(await live.GetAsync(k), Is.Null, $"Restored subject must not contain '{k}'.");
        }
    }

    // Repeated shadow-cutover restores to successively earlier point-in-times on
    // the same target must each reconcile membership down to that restore's
    // subject - the index narrows monotonically and never strands a row from a
    // superseded restore.
    [Test]
    public async Task Repeated_restores_to_earlier_points_narrow_tag_membership_each_time()
    {
        await _fixture.InitializeAsync();

        const string source = "chaos-recon-multi-src";
        var sourceTree = _fixture.GrainFactory.GetGrain<ILattice>(source);

        // Earliest point-in-time P1: keep-00..keep-04.
        var p1Keys = Enumerable.Range(0, 5).Select(i => $"keep-{i:D2}").ToArray();
        foreach (var k in p1Keys) await sourceTree.SetAsync(k, Bytes($"v-{k}"));
        var p1 = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("chaos-recon-p1", BackupScopeSelector.WholeTree(source)));

        // Later point-in-time P2 adds keep-05..keep-09.
        var p2Extra = Enumerable.Range(5, 5).Select(i => $"keep-{i:D2}").ToArray();
        foreach (var k in p2Extra) await sourceTree.SetAsync(k, Bytes($"v-{k}"));
        var p2Keys = p1Keys.Concat(p2Extra).ToArray();
        var p2 = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("chaos-recon-p2", BackupScopeSelector.WholeTree(source)));

        // Live target: everything P2 has plus 40 'gone' keys, all tagged red.
        const string target = "chaos-recon-multi-live";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeTagIndexFactory>();
        var index = factory.Create(_fixture.GrainFactory.GetGrain<ILattice>(target), "chaos-recon-multi-colors");
        foreach (var k in p2Keys) await index.SetValueWithTags(k, Bytes($"v-{k}"), "red").CommitAsync();
        var gone = Enumerable.Range(0, 40).Select(i => $"gone-{i:D2}").ToArray();
        foreach (var k in gone) await index.SetValueWithTags(k, Bytes($"v-{k}"), "red").CommitAsync();
        Assert.That(await RedKeysAsync(index), Has.Count.EqualTo(p2Keys.Length + gone.Length));

        // Restore P2: gone drops, both keep generations remain.
        await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(p2.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(await RedKeysAsync(index), Is.EquivalentTo(p2Keys),
            "Restoring P2 must drop the gone keys and keep both generations.");

        // Restore P1 onto the same target: keep-05..keep-09 now also drop.
        await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(p1.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(await RedKeysAsync(index), Is.EquivalentTo(p1Keys),
            "Restoring the earlier P1 must further narrow membership to P1's subject.");

        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        foreach (var k in p2Extra)
        {
            Assert.That(await live.GetAsync(k), Is.Null,
                $"'{k}' absent from P1 must not survive on the twice-restored subject.");
        }
    }
}
