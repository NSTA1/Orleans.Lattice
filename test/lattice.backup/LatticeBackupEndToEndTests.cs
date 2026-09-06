using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Acceptance-level end-to-end coverage for the backup surface: an atomic
/// multi-key saga is never torn across the consistency cut; a full and an
/// incremental captured at the same cut restore to the same live state; a
/// denied gate fails capture closed writing nothing; and a tree large enough to
/// span multiple snapshot pages round-trips faithfully through capture and
/// restore.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupEndToEndTests
{
    private const string Source = "orders";

    private CaptureClusterFixture _capture = null!;
    private RestoreClusterFixture _restore = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _capture = new CaptureClusterFixture();
        _restore = new RestoreClusterFixture();
    }

    [TearDown]
    public async Task TearDown()
    {
        await _capture.DisposeAsync();
        await _restore.DisposeAsync();
    }

    // ---- Causal consistency of the captured cut -------------------------

    [Test]
    public async Task CaptureAsync_atomic_saga_is_never_split_across_the_consistency_cut()
    {
        await _capture.InitializeAsync();
        var source = _capture.GrainFactory.GetGrain<ILattice>(Source);

        // A single atomic saga commits five keys as one causal unit.
        var saga = new List<KeyValuePair<string, byte[]>>
        {
            new("saga:1", Bytes("s1")),
            new("saga:2", Bytes("s2")),
            new("saga:3", Bytes("s3")),
            new("saga:4", Bytes("s4")),
            new("saga:5", Bytes("s5")),
        };
        await source.SetManyAtomicAsync(saga);
        await source.SetAsync("other", Bytes("o"));

        var backup = await _capture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("saga", BackupScopeSelector.WholeTree(Source)));
        var entries = await DecodeAsync(_capture, backup.Manifest);

        var sagaKeys = saga.Select(kv => kv.Key).ToHashSet();
        var captured = entries.Where(e => sagaKeys.Contains(e.Key)).ToList();

        Assert.Multiple(() =>
        {
            // All-or-nothing: every saga key is present in the cut, none is torn off.
            Assert.That(captured.Select(e => e.Key), Is.EquivalentTo(sagaKeys));

            // The whole atomic write survives intact - each saga value is captured
            // exactly as committed, so the cut contains the entire causal unit.
            foreach (var kv in saga)
            {
                var entry = captured.Single(e => e.Key == kv.Key);
                Assert.That(entry.Value, Is.EqualTo(kv.Value), $"saga entry {kv.Key} value must survive the cut");
                Assert.That(entry.IsTombstone, Is.False, $"saga entry {kv.Key} must not be a tombstone");
            }
        });
    }

    // ---- Full and incremental at the same cut agree ---------------------

    [Test]
    public async Task Full_and_incremental_at_the_same_cut_restore_to_the_same_live_state()
    {
        await _restore.InitializeAsync();
        var source = _restore.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));
        await source.SetAsync("k3", Bytes("v3"));

        var baseBackup = await _restore.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Source)));

        // Diverge the live tree from the base cut.
        await source.SetAsync("k1", Bytes("v1-updated"));
        await source.SetAsync("k4", Bytes("v4"));
        await source.DeleteAsync("k2");

        // Two captures of the SAME live cut (no writes between them): a fresh full
        // and an incremental layered on the base.
        var full = await _restore.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("full-2", BackupScopeSelector.WholeTree(Source)));
        var increment = await _restore.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "inc", BackupScopeSelector.WholeTree(Source), baseBackup.BackupId));

        Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));

        await _restore.Restore.RestoreAsync(new LatticeRestoreRequest(full.BackupId, "from-full"));
        await _restore.Restore.RestoreAsync(new LatticeRestoreRequest(increment.BackupId, "from-incremental"));

        var fromFull = await CaptureAndDecodeAsync(_restore, "from-full", "verify-full");
        var fromIncremental = await CaptureAndDecodeAsync(_restore, "from-incremental", "verify-inc");

        // The base + increment fold reproduces exactly the same state - keys,
        // values, and causal envelope - as the full taken at the same cut.
        Assert.Multiple(() =>
        {
            Assert.That(
                fromIncremental.Select(e => e.Key),
                Is.EquivalentTo(fromFull.Select(e => e.Key)));

            foreach (var expected in fromFull)
            {
                var actual = fromIncremental.Single(e => e.Key == expected.Key);
                Assert.That(actual.Timestamp, Is.EqualTo(expected.Timestamp), $"HLC for {expected.Key}");
                Assert.That(actual.IsTombstone, Is.EqualTo(expected.IsTombstone), $"tombstone for {expected.Key}");
                Assert.That(Str(actual.Value), Is.EqualTo(Str(expected.Value)), $"value for {expected.Key}");
            }
        });
    }

    // ---- Permission fail-closed on capture ------------------------------

    [Test]
    public async Task CaptureAsync_denied_permission_fails_closed_and_writes_no_backup()
    {
        await _capture.InitializeAsync();
        var source = _capture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        var denying = new BackupAccessAuthorizer(
            new DenyingAccessGate("no backup grant"), membership: null);
        var gatedCapture = _capture.CreateCaptureServiceWith(denying);

        Assert.That(
            async () => await gatedCapture.CaptureAsync(
                new LatticeBackupCaptureRequest("denied", BackupScopeSelector.WholeTree(Source))),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        // Fail-closed: nothing was catalogued.
        var catalogued = new List<BackupManifest>();
        await foreach (var manifest in _capture.Catalog.ListAsync())
        {
            catalogued.Add(manifest);
        }

        Assert.That(catalogued, Is.Empty);
    }

    // ---- Multi-leaf / multi-shard snapshot paging -----------------------

    [Test]
    public async Task Capture_and_restore_of_a_multi_leaf_tree_round_trips_faithfully()
    {
        await _restore.InitializeAsync();
        var source = _restore.GrainFactory.GetGrain<ILattice>(Source);

        // Enough keys to spill a single leaf and page the snapshot drain across
        // multiple leaves / shards.
        const int keyCount = 500;
        for (var i = 0; i < keyCount; i++)
        {
            await source.SetAsync($"key:{i:D5}", Bytes($"value-{i}"));
        }

        var backup = await _restore.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest(
                "large", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-large-restored";
        var result = await _restore.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target));

        var restored = _restore.GrainFactory.GetGrain<ILattice>(target);

        // Spot-check the boundaries and the middle, then confirm the full count.
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(keyCount));
            Assert.That(Str((await restored.GetAsync("key:00000"))!), Is.EqualTo("value-0"));
            Assert.That(Str((await restored.GetAsync("key:00250"))!), Is.EqualTo("value-250"));
            Assert.That(Str((await restored.GetAsync("key:00499"))!), Is.EqualTo("value-499"));
        });

        var restoredCount = 0;
        await foreach (var _ in restored.ScanKeysAsync())
        {
            restoredCount++;
        }

        Assert.That(restoredCount, Is.EqualTo(keyCount));
    }

    // ---- Helpers --------------------------------------------------------

    private static async Task<List<LwwEntry>> DecodeAsync(CaptureClusterFixture fixture, BackupManifest manifest)
    {
        var descriptor = manifest.ContentDescriptors.Single();
        var all = new List<LwwEntry>();
        await foreach (var chunk in fixture.Sink.ReadArtifactAsync(descriptor.ArtifactId))
        {
            all.AddRange(fixture.Serializer.Deserialize<LwwEntry[]>(chunk));
        }

        return all;
    }

    private static async Task<List<LwwEntry>> CaptureAndDecodeAsync(
        RestoreClusterFixture fixture, string treeId, string name)
    {
        var backup = await fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest(name, BackupScopeSelector.WholeTree(treeId)));
        var descriptor = backup.Manifest.ContentDescriptors.Single();
        var all = new List<LwwEntry>();
        await foreach (var chunk in fixture.Sink.ReadArtifactAsync(descriptor.ArtifactId))
        {
            all.AddRange(fixture.Serializer.Deserialize<LwwEntry[]>(chunk));
        }

        return all;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[]? b) => b is null ? "<absent>" : Encoding.UTF8.GetString(b);

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
