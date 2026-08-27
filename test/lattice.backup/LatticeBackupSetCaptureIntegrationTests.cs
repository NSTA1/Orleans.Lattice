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

    // ---- Set-membership provenance --------------------------------------

    [Test]
    public async Task CaptureSetAsync_multi_tree_stamps_shared_set_membership_on_every_member()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("mem-a").SetAsync("k", Bytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>("mem-b").SetAsync("k", Bytes("v"));

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "member-set",
            new[] { BackupScopeSelector.WholeTree("mem-a"), BackupScopeSelector.WholeTree("mem-b") },
            crossTreeConsistent: false));

        Assert.That(result.Members, Has.Count.EqualTo(2));

        // Every returned member carries the set's id and name, so the members can
        // be grouped into one logical entry from a first-class fact.
        foreach (var member in result.Members)
        {
            Assert.Multiple(() =>
            {
                Assert.That(member.Manifest.SetId, Is.EqualTo(result.SetManifest.SetId));
                Assert.That(member.Manifest.SetName, Is.EqualTo("member-set"));
            });
        }

        // The stamp is durable: the catalogued copy read back by backup id carries
        // the same set membership, not just the in-memory result.
        foreach (var member in result.Members)
        {
            var stored = await _fixture.Catalog.GetAsync(member.BackupId);
            Assert.That(stored, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(stored!.SetId, Is.EqualTo(result.SetManifest.SetId));
                Assert.That(stored.SetName, Is.EqualTo("member-set"));
            });
        }
    }

    [Test]
    public async Task CaptureSetAsync_single_tree_leaves_set_membership_unstamped()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("lone").SetAsync("k", Bytes("v"));

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "lone-set",
            new[] { BackupScopeSelector.WholeTree("lone") },
            crossTreeConsistent: false));

        // A single-member set is indistinguishable from a plain backup, so it
        // carries no set membership and lists as one ordinary row.
        Assert.That(result.Members, Has.Count.EqualTo(1));
        var stored = await _fixture.Catalog.GetAsync(result.Members[0].BackupId);
        Assert.That(stored, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result.Members[0].Manifest.SetId, Is.Null);
            Assert.That(stored!.SetId, Is.Null);
            Assert.That(stored.SetName, Is.Null);
        });
    }

    // ---- The set id agrees with what the catalog records -----------------

    [Test]
    public async Task CaptureSetAsync_single_tree_returns_no_set_id_at_all()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("phantom").SetAsync("k", Bytes("v"));

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "phantom-set",
            new[] { BackupScopeSelector.WholeTree("phantom") },
            crossTreeConsistent: false));

        // Regression: a one-scope capture used to hand back a content-addressed
        // id indistinguishable from a valid one, which was never written to any
        // manifest - so a consumer grouping catalog rows by it silently found
        // nothing. The capture response now agrees with the catalog row.
        Assert.Multiple(() =>
        {
            Assert.That(result.SetManifest.SetId, Is.Null,
                "an id that is never stamped anywhere must not be handed to the caller");
            Assert.That(result.SetManifest.Name, Is.EqualTo("phantom-set"),
                "the set is still self-describing: only the unresolvable id is withheld");
            Assert.That(result.SetManifest.MemberBackupIds,
                Is.EqualTo(new[] { result.Members[0].BackupId }),
                "the member the caller must actually restore is still reported");
        });
    }

    [Test]
    public async Task CaptureSetAsync_set_id_agrees_with_the_catalogued_member_rows()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("agree-solo").SetAsync("k", Bytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>("agree-a").SetAsync("k", Bytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>("agree-b").SetAsync("k", Bytes("v"));

        var solo = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "agree-solo-set", new[] { BackupScopeSelector.WholeTree("agree-solo") }));
        var pair = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "agree-pair-set",
            new[] { BackupScopeSelector.WholeTree("agree-a"), BackupScopeSelector.WholeTree("agree-b") }));

        // The invariant the issue turns on, asserted for both arities: whatever the
        // create response reports as the set id is exactly what a catalog consumer
        // reads back off every member row. Previously the one-scope case reported a
        // real-looking id against a null row.
        var soloStored = await _fixture.Catalog.GetAsync(solo.Members[0].BackupId);
        Assert.That(soloStored, Is.Not.Null);
        Assert.That(soloStored!.SetId, Is.EqualTo(solo.SetManifest.SetId));

        Assert.That(pair.SetManifest.SetId, Is.Not.Null);
        foreach (var member in pair.Members)
        {
            var stored = await _fixture.Catalog.GetAsync(member.BackupId);
            Assert.That(stored, Is.Not.Null);
            Assert.That(stored!.SetId, Is.EqualTo(pair.SetManifest.SetId));
        }
    }

    [Test]
    public async Task CaptureSetAsync_multi_tree_set_id_is_the_content_address_of_its_members()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("addr-a").SetAsync("k", Bytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>("addr-b").SetAsync("k", Bytes("v"));

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "addressed-set",
            new[] { BackupScopeSelector.WholeTree("addr-a"), BackupScopeSelector.WholeTree("addr-b") }));

        // Withholding the one-member id must not have disturbed the addressing
        // scheme for the sets that do carry one.
        Assert.That(
            result.SetManifest.SetId,
            Is.EqualTo(BackupSetIdentity.Compute(result.SetManifest.MemberBackupIds)));
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
