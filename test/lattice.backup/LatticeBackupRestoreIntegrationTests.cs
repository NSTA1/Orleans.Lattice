using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end coverage for <see cref="ILatticeBackupRestoreService"/>: a full
/// backup replays into an empty tree reproducing every value and its causal
/// envelope (hybrid-logical-clock, origin, tombstone) verbatim; a re-run converges
/// to the same state; a prefix- or key-scoped restore touches only that region; a
/// merge into an existing tree converges by last-writer-wins rather than blind
/// overwrite; a shadow-cutover swaps the registry alias and is revertible; the
/// permission gate fails closed without writing anything; and a tampered or missing
/// artifact is rejected before anything is installed.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupRestoreIntegrationTests
{
    private const string Source = "orders";

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Full restore into an empty tree --------------------------------

    [Test]
    public async Task RestoreAsync_full_backup_into_empty_tree_reproduces_values_and_metadata_exactly()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));
        await source.SetAsync("k3", Bytes("v3"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));
        var sourceEntries = await DecodeAsync(backup.Manifest);

        const string target = "orders-restored";
        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        var restoredEntries = await CaptureAndDecodeAsync(target, "verify-full");

        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(3));
            Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.InPlace));
            Assert.That(result.ManifestChain, Has.Count.EqualTo(1));
            Assert.That(result.TargetTreeId, Is.EqualTo(target));
        });

        Assert.Multiple(() =>
        {
            Assert.That(Str(restored.GetAsync("k1").Result!), Is.EqualTo("v1"));
            Assert.That(Str(restored.GetAsync("k2").Result!), Is.EqualTo("v2"));
            Assert.That(Str(restored.GetAsync("k3").Result!), Is.EqualTo("v3"));
        });

        // Every restored entry carries the source's causal envelope verbatim: same
        // HLC timestamp, same (null local) origin, same tombstone flag.
        foreach (var expected in sourceEntries)
        {
            var actual = restoredEntries.Single(e => e.Key == expected.Key);
            Assert.Multiple(() =>
            {
                Assert.That(actual.Timestamp, Is.EqualTo(expected.Timestamp), $"HLC for {expected.Key}");
                Assert.That(actual.OriginClusterId, Is.EqualTo(expected.OriginClusterId), $"origin for {expected.Key}");
                Assert.That(actual.IsTombstone, Is.EqualTo(expected.IsTombstone), $"tombstone for {expected.Key}");
                Assert.That(actual.Value, Is.EqualTo(expected.Value), $"value for {expected.Key}");
            });
        }
    }

    // ---- Incremental chain ----------------------------------------------

    [Test]
    public async Task RestoreAsync_base_plus_incremental_chain_folds_the_delta_over_the_base()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));
        await source.SetAsync("k3", Bytes("v3"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Source)));

        // Writes after the base cut: an overwrite, a new key, and a delete. These
        // are exactly what the increment must carry so the restored chain differs
        // from the base.
        await source.SetAsync("k1", Bytes("v1-updated"));
        await source.SetAsync("k4", Bytes("v4"));
        await source.DeleteAsync("k2");

        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(Source), baseBackup.BackupId));

        Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));

        // Restore the increment tip: the engine walks back to the base and replays
        // base-first, then folds the delta.
        var target = "orders-chain-restore";
        await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(increment.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        Assert.Multiple(() =>
        {
            Assert.That(Str(restored.GetAsync("k1").Result!), Is.EqualTo("v1-updated"), "overwrite folded");
            Assert.That(restored.GetAsync("k2").Result, Is.Null, "delete folded");
            Assert.That(Str(restored.GetAsync("k3").Result!), Is.EqualTo("v3"), "untouched base entry survives");
            Assert.That(Str(restored.GetAsync("k4").Result!), Is.EqualTo("v4"), "new key folded");
        });
    }

    // ---- Idempotency ----------------------------------------------------

    [Test]
    public async Task RestoreAsync_rerun_is_a_no_op()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("rerun", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-rerun";
        var request = new LatticeRestoreRequest(backup.BackupId, target);
        await _fixture.Restore.RestoreAsync(request);
        var firstState = await CaptureAndDecodeAsync(target, "rerun-1");

        // Second application converges to the same state (a no-op in effect): same
        // keys, same values, same HLC stamps.
        await _fixture.Restore.RestoreAsync(request);
        var secondState = await CaptureAndDecodeAsync(target, "rerun-2");

        Assert.Multiple(() =>
        {
            Assert.That(secondState.Select(e => e.Key), Is.EqualTo(firstState.Select(e => e.Key)));
            Assert.That(secondState.Select(e => e.Timestamp), Is.EqualTo(firstState.Select(e => e.Timestamp)));
            Assert.That(secondState.Select(e => Str(e.Value!)), Is.EqualTo(firstState.Select(e => Str(e.Value!))));
        });
    }

    // ---- Scoped restore -------------------------------------------------

    [Test]
    public async Task RestoreAsync_prefix_scope_restores_only_that_prefix()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("a:1", Bytes("va1"));
        await source.SetAsync("a:2", Bytes("va2"));
        await source.SetAsync("b:1", Bytes("vb1"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("scoped", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-prefix";
        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, BackupScopeSelector.Prefix(Source, "a:")));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);

        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(2));
            Assert.That(Str(restored.GetAsync("a:1").Result!), Is.EqualTo("va1"));
            Assert.That(Str(restored.GetAsync("a:2").Result!), Is.EqualTo("va2"));
            Assert.That(restored.GetAsync("b:1").Result, Is.Null);
        });
    }

    [Test]
    public async Task RestoreAsync_key_scope_restores_only_that_key()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("scoped-key", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-key";
        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, BackupScopeSelector.Key(Source, "k1")));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);

        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(1));
            Assert.That(Str(restored.GetAsync("k1").Result!), Is.EqualTo("v1"));
            Assert.That(restored.GetAsync("k2").Result, Is.Null);
        });
    }

    // ---- Per-key merge mode round-trips through capture + restore -------

    [Test]
    public async Task Capture_then_restore_carries_per_key_merge_mode_and_restores_values_for_a_mixed_tree()
    {
        await _fixture.InitializeAsync();

        // A local-only tree (no declared mode) mixing plain LWW keys with a CRDT
        // key. The per-key discriminator must let the capture stream label each
        // key with its true mode, and the values must restore faithfully.
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("lww1", Bytes("v1"));
        await source.OrSet("crdt1").AddAsync(Bytes("e1"), "r1");

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("mixed", BackupScopeSelector.WholeTree(Source)));

        // The streamed artifact carries the per-key discriminator: the CRDT key
        // is tagged with its true CRDT mode, the plain key carries no per-key mode
        // (it falls back to the declared tree default at read time).
        var streamed = (await DecodeAsync(backup.Manifest)).ToDictionary(e => e.Key);
        Assert.Multiple(() =>
        {
            Assert.That(streamed["crdt1"].MergeMode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(streamed["lww1"].MergeMode, Is.Null);
        });

        // The manifest descriptors mirror that: crdt1 -> Crdt, lww1 -> LWW.
        var descriptors = backup.Manifest.KeyDescriptors.ToDictionary(d => d.Key, d => d.MergeMode);
        Assert.Multiple(() =>
        {
            Assert.That(descriptors["crdt1"], Is.EqualTo(BackupKeyMergeMode.Crdt));
            Assert.That(descriptors["lww1"], Is.EqualTo(BackupKeyMergeMode.LastWriterWins));
        });

        // Restore reproduces both values faithfully: the plain register and the
        // converged CRDT state both land in the target.
        const string target = "orders-mixed-restore";
        await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(backup.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        var crdtHasElement = await restored.OrSet("crdt1").ContainsAsync(Bytes("e1"));
        Assert.Multiple(() =>
        {
            Assert.That(Str(restored.GetAsync("lww1").Result!), Is.EqualTo("v1"));
            Assert.That(crdtHasElement, Is.True,
                "the converged CRDT state restores as valid CRDT bytes.");
        });
    }

    // ---- Mode-faithful last-writer-wins merge into existing data --------

    [Test]
    public async Task RestoreAsync_merge_into_existing_converges_by_lww_preserving_newer_entries()
    {
        await _fixture.InitializeAsync();

        // Capture a source with two keys.
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("backup-v1"));
        await source.SetAsync("k2", Bytes("backup-v2"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("merge", BackupScopeSelector.WholeTree(Source)));

        // Seed the target so it already exists (forces the merge path, not the
        // bulk-load fast path). k2 in the target is written AFTER the backup, so it
        // is causally newer and must survive the restore; k1 is absent so the
        // backup value fills it in.
        const string target = "orders-merge";
        var existing = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await existing.SetAsync("k2", Bytes("live-newer-v2"));

        var result = await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(backup.BackupId, target));

        Assert.Multiple(() =>
        {
            // k1 had no live value, so the backup value converges in.
            Assert.That(Str(existing.GetAsync("k1").Result!), Is.EqualTo("backup-v1"));
            // k2's live write is causally newer than the backup, so last-writer-wins
            // keeps it: the restore is a faithful merge, not a blind overwrite.
            Assert.That(Str(existing.GetAsync("k2").Result!), Is.EqualTo("live-newer-v2"));
            Assert.That(result.EntriesApplied, Is.EqualTo(2));
        });
    }

    // ---- Shadow-cutover + revert ----------------------------------------

    [Test]
    public async Task RestoreAsync_shadow_cutover_swaps_alias_then_revert_restores_prior_tree()
    {
        await _fixture.InitializeAsync();

        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("backup-v1"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cutover", BackupScopeSelector.WholeTree(Source)));

        // The live target carries data that must survive a revert.
        const string target = "orders-live";
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await live.SetAsync("live-key", Bytes("live-value"));

        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            Assert.That(result.ShadowPhysicalTreeId, Is.Not.Null);
            Assert.That(result.PreviousPhysicalTreeId, Is.EqualTo(target));
            // After cutover the logical tree resolves to the shadow: backup data is
            // live, the pre-cutover key is gone.
            Assert.That(Str(live.GetAsync("k1").Result!), Is.EqualTo("backup-v1"));
            Assert.That(live.GetAsync("live-key").Result, Is.Null);
        });

        await _fixture.Restore.RevertRestoreAsync(result);

        Assert.Multiple(() =>
        {
            // The prior tree is retained and the revert swings the alias back.
            Assert.That(Str(live.GetAsync("live-key").Result!), Is.EqualTo("live-value"));
            Assert.That(live.GetAsync("k1").Result, Is.Null);
        });
    }

    [Test]
    public async Task RevertRestoreAsync_rejects_a_non_shadow_result()
    {
        await _fixture.InitializeAsync();
        var inPlace = new LatticeRestoreResult(
            "b", "t", LatticeRestoreMode.InPlace, "op", new[] { "b" }, 0);

        Assert.That(
            async () => await _fixture.Restore.RevertRestoreAsync(inPlace),
            Throws.InstanceOf<ArgumentException>());
    }

    // ---- Permission fail-closed -----------------------------------------

    [Test]
    public async Task RestoreAsync_denied_permission_fails_closed_and_writes_nothing()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("denied", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-denied";
        var denying = new BackupAccessAuthorizer(
            new DenyingAccessGate("no restore grant"), membership: null);
        var restore = _fixture.CreateRestoreServiceWith(denying);

        Assert.That(
            async () => await restore.RestoreAsync(new LatticeRestoreRequest(backup.BackupId, target)),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        // Fail-closed: nothing was installed into the target.
        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        Assert.That(restored.GetAsync("k1").Result, Is.Null);
    }

    // ---- Validation trust boundary --------------------------------------

    [Test]
    public async Task RestoreAsync_missing_backup_throws_validation()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await _fixture.Restore.RestoreAsync(
                new LatticeRestoreRequest("0000000000000000000000000000000000000000000000000000000000000000", "any-tree")),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    [Test]
    public async Task RestoreAsync_rejects_a_backup_whose_artifact_fails_integrity()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("tampered", BackupScopeSelector.WholeTree(Source)));

        // Tamper: overwrite the artifact bytes so its digest no longer matches the
        // manifest's recorded content hash.
        var artifactId = backup.Manifest.ContentDescriptors.Single().ArtifactId;
        await _fixture.Sink.WriteArtifactAsync(artifactId, SingleChunk(Bytes("tampered-bytes")));

        Assert.That(
            async () => await _fixture.Restore.RestoreAsync(
                new LatticeRestoreRequest(backup.BackupId, "orders-tampered")),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    // ---- Argument guards ------------------------------------------------

    [Test]
    public async Task RestoreAsync_null_request_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Restore.RestoreAsync(null!),
            Throws.ArgumentNullException);
    }

    // ---- Helpers --------------------------------------------------------

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

    private async Task<List<LwwEntry>> CaptureAndDecodeAsync(string treeId, string name)
    {
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest(name, BackupScopeSelector.WholeTree(treeId)));
        return await DecodeAsync(backup.Manifest);
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> SingleChunk(byte[] bytes)
    {
        yield return bytes;
        await Task.CompletedTask;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
