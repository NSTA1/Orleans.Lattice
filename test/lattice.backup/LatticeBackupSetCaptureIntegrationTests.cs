using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end coverage for <see cref="ILatticeBackupCaptureService.CaptureSetAsync"/>:
/// a cross-tree-consistent set never captures a partial cross-tree atomic batch
/// (all-or-none at the fence), a single-tree or non-flagged set issues no extra
/// coordination (no fence is selected), and the fence selection plus drain wait
/// are recorded in the backup meter.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupSetCaptureIntegrationTests
{
    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CaptureClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- All-or-none at the fence ---------------------------------------

    [Test]
    public async Task CaptureSetAsync_cross_tree_consistent_never_captures_a_partial_cross_tree_batch()
    {
        await _fixture.InitializeAsync();

        // Repeat under interleaving: each round seeds two trees with an "old"
        // cross-tree pair, then races a cross-tree atomic write flipping both to
        // "new" against a fenced set capture. The fence guarantees the captured
        // set observes the batch all-or-none: never one tree "new" and the other
        // "old".
        for (var round = 0; round < 12; round++)
        {
            var suffix = $"{round:D2}-{Guid.NewGuid():N}";
            var treeA = $"xt-set-a-{suffix}";
            var treeB = $"xt-set-b-{suffix}";
            var a = _fixture.GrainFactory.GetGrain<ILattice>(treeA);
            var b = _fixture.GrainFactory.GetGrain<ILattice>(treeB);
            await a.SetAsync("k", Bytes("old"));
            await b.SetAsync("k", Bytes("old"));

            var writer = Task.Run(() => _fixture.GrainFactory.SetManyAtomicAsync(
                new[]
                {
                    new LatticeTreeBatch(treeA, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
                    new LatticeTreeBatch(treeB, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
                },
                operationId: $"op-{suffix}"));

            var capture = _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
                $"set-{suffix}",
                new[] { BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB) },
                crossTreeConsistent: true));

            await Task.WhenAll(writer, capture);
            var result = await capture;

            var valueA = await ValueOfAsync(result, treeA, "k");
            var valueB = await ValueOfAsync(result, treeB, "k");

            Assert.That(
                valueA == "new",
                Is.EqualTo(valueB == "new"),
                $"round {round}: torn cross-tree batch - treeA='{valueA}', treeB='{valueB}'");
        }
    }

    // ---- No coordination for the cheap cases ----------------------------

    [Test]
    public async Task CaptureSetAsync_single_tree_selects_no_fence_and_pays_no_coordination()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>("solo");
        await tree.SetAsync("k1", Bytes("v1"));

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "solo-set",
            new[] { BackupScopeSelector.WholeTree("solo") },
            crossTreeConsistent: true));

        Assert.Multiple(() =>
        {
            // Even with the flag set, a single-tree set takes the cheap per-tree
            // cut: no fence is selected.
            Assert.That(result.SetManifest.CrossTreeConsistent, Is.False);
            Assert.That(result.SetManifest.Fence, Is.Null);
            Assert.That(result.Members, Has.Count.EqualTo(1));
            Assert.That(result.SetManifest.MemberBackupIds, Is.EqualTo(new[] { result.Members[0].BackupId }));
        });
    }

    [Test]
    public async Task CaptureSetAsync_multi_tree_without_the_flag_selects_no_fence()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("g1").SetAsync("k", Bytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>("g2").SetAsync("k", Bytes("v"));

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "grouped",
            new[] { BackupScopeSelector.WholeTree("g1"), BackupScopeSelector.WholeTree("g2") },
            crossTreeConsistent: false));

        Assert.Multiple(() =>
        {
            Assert.That(result.SetManifest.CrossTreeConsistent, Is.False);
            Assert.That(result.SetManifest.Fence, Is.Null);
            Assert.That(result.Members, Has.Count.EqualTo(2));
        });
    }

    // ---- Fence + drain recorded in metrics ------------------------------

    [Test]
    public async Task CaptureSetAsync_records_fence_selection_and_drain_wait_in_the_backup_meter()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("m1").SetAsync("k", Bytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>("m2").SetAsync("k", Bytes("v"));

        var fenceSelections = 0;
        var drainWaitRecorded = false;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == BackupMetrics.MeterName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            if (instrument.Name == "orleans.lattice.backup.cross_tree_fence.selections")
            {
                Interlocked.Add(ref fenceSelections, (int)measurement);
            }
        });
        listener.SetMeasurementEventCallback<double>((instrument, _, _, _) =>
        {
            if (instrument.Name == "orleans.lattice.backup.cross_tree_fence.drain_wait")
            {
                drainWaitRecorded = true;
            }
        });
        listener.Start();

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "metered",
            new[] { BackupScopeSelector.WholeTree("m1"), BackupScopeSelector.WholeTree("m2") },
            crossTreeConsistent: true));

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(result.SetManifest.CrossTreeConsistent, Is.True);
            Assert.That(result.SetManifest.Fence, Is.Not.Null);
            Assert.That(result.SetManifest.Fence!.HlcTimestamp, Is.GreaterThan(0));
            Assert.That(result.SetManifest.Fence.Attempts, Is.GreaterThanOrEqualTo(1));
            Assert.That(result.SetManifest.Fence.DrainWaitMilliseconds, Is.GreaterThanOrEqualTo(0));
            Assert.That(fenceSelections, Is.GreaterThanOrEqualTo(1));
            Assert.That(drainWaitRecorded, Is.True);
        });
    }

    // ---- Helpers --------------------------------------------------------

    private async Task<string?> ValueOfAsync(LatticeBackupSetCaptureResult result, string treeId, string key)
    {
        var member = result.Members.Single(m => m.Manifest.Scope.TreeId == treeId);
        var entries = await DecodeAsync(member.Manifest);
        var matches = entries.Where(e => e.Key == key).ToList();
        return matches.Count == 1 && matches[0].Value is { } value ? Str(value) : null;
    }

    private async Task<List<LwwEntry>> DecodeAsync(BackupManifest manifest)
    {
        var descriptor = manifest.ContentDescriptors.Single();
        var all = new List<LwwEntry>();
        await foreach (var chunk in _fixture.Sink.ReadArtifactAsync(descriptor.ArtifactId))
        {
            all.AddRange(_fixture.Serializer.Deserialize<LwwEntry[]>(chunk));
        }

        return all;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);
}
