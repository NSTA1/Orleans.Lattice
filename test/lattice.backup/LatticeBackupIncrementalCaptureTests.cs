using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end coverage for <see cref="ILatticeBackupIncrementalCaptureService.CaptureIncrementalAsync"/>:
/// a base full backup followed by writes yields an increment that carries only the
/// delta as a uniform last-writer-wins entry array (byte-identical in shape to a
/// full capture, so the restore chain decodes a base and its increments through one
/// path); the base plus the increment folds to the same live state as a full backup
/// taken at the increment's cut; an increment with no intervening writes is an empty
/// no-op chained onto its base; an increment whose base resume point has fallen off
/// the WAL falls back to a fresh full backup; and a range delete in the delta window
/// - which the point-keyed entry artifact cannot faithfully encode - also falls back
/// to a fresh full backup rather than emitting a delta a chain restore could not fold.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupIncrementalCaptureTests
{
    private const string Tree = "orders";

    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CaptureClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Delta capture ---------------------------------------------------

    [Test]
    public async Task CaptureIncrementalAsync_captures_only_the_delta_committed_after_the_base()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v1"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Tree)));

        // Writes after the base cut: an overwrite, a brand-new key, and a delete.
        await tree.SetAsync("k1", Bytes("v2"));
        await tree.SetAsync("k3", Bytes("v1"));
        await tree.DeleteAsync("k2");

        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(Tree), baseBackup.BackupId));

        var entries = await DecodeEntriesAsync(increment.Manifest);

        Assert.Multiple(() =>
        {
            Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(increment.Manifest.BaseBackupId, Is.EqualTo(baseBackup.BackupId));

            // The delta carries exactly the three post-base writes, no base entries.
            var keys = entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray();
            Assert.That(keys, Is.EqualTo(new[] { "k1", "k2", "k3" }));

            // The delete surfaces as a tombstone entry; the sets carry live values.
            Assert.That(entries.Single(e => e.Key == "k2").IsTombstone, Is.True);
            Assert.That(entries.Single(e => e.Key == "k1").IsTombstone, Is.False);
            Assert.That(Str(entries.Single(e => e.Key == "k1").Value!), Is.EqualTo("v2"));
            // Exact HLC metadata rides every delta entry.
            Assert.That(entries.All(e => e.Timestamp.WallClockTicks > 0), Is.True);
        });
    }

    // ---- Base + increment folds to the same state as a full --------------

    [Test]
    public async Task Base_plus_increment_folds_to_the_same_state_as_a_full_at_the_cut()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("1"));
        await tree.SetAsync("c", Bytes("1"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Tree)));

        // Mutate the tree after the base cut.
        await tree.SetAsync("a", Bytes("2"));   // overwrite
        await tree.DeleteAsync("b");            // delete
        await tree.SetAsync("d", Bytes("1"));   // new key

        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(Tree), baseBackup.BackupId));

        // A full backup taken now is the reference for the increment's cut, since no
        // writes happen between the increment and this full.
        var fullAtCut = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("full-at-cut", BackupScopeSelector.WholeTree(Tree)));

        var baseEntries = await DecodeEntriesAsync(baseBackup.Manifest);
        var deltaEntries = await DecodeEntriesAsync(increment.Manifest);
        var folded = FoldBaseAndDelta(baseEntries, deltaEntries);

        var reference = (await DecodeEntriesAsync(fullAtCut.Manifest))
            .Where(e => !e.IsTombstone)
            .ToDictionary(e => e.Key, e => Str(e.Value!), StringComparer.Ordinal);

        Assert.That(folded, Is.EqualTo(reference));
    }

    // ---- Empty increment -------------------------------------------------

    [Test]
    public async Task CaptureIncrementalAsync_with_no_intervening_writes_is_an_empty_no_op()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v1"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Tree)));

        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc-empty", BackupScopeSelector.WholeTree(Tree), baseBackup.BackupId));

        var descriptor = increment.Manifest.ContentDescriptors.Single();
        var entries = await DecodeEntriesAsync(increment.Manifest);

        Assert.Multiple(() =>
        {
            // An empty increment is still a real, chained manifest - just with no
            // delta entries or streamed chunks.
            Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(increment.Manifest.BaseBackupId, Is.EqualTo(baseBackup.BackupId));
            Assert.That(entries, Is.Empty);
            Assert.That(increment.Manifest.KeyDescriptors, Is.Empty);
            Assert.That(descriptor.ChunkCount, Is.Zero);
            Assert.That(descriptor.ByteLength, Is.Zero);
            // The chained id is distinct from the base even though the delta is empty.
            Assert.That(increment.BackupId, Is.Not.EqualTo(baseBackup.BackupId));
        });
    }

    // ---- WAL fall-off falls back to a full -------------------------------

    [Test]
    public async Task CaptureIncrementalAsync_falls_back_to_a_full_when_the_base_resume_point_fell_off_the_wal()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        var keys = Enumerable.Range(0, 12).Select(i => $"k{i:D2}").ToArray();
        foreach (var key in keys)
        {
            await tree.SetAsync(key, Bytes("v1"));
        }

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Tree)));

        // Overwrite every key so each populated WAL partition grows past its base
        // resume offset, then trim the whole WAL so the resume point is gone.
        foreach (var key in keys)
        {
            await tree.SetAsync(key, Bytes("v2"));
        }

        var resolver = _fixture.SiloServices.GetRequiredService<LatticeOptionsResolver>();
        var provider = _fixture.SiloServices.GetRequiredService<IWalStorageProvider>();
        var partitions = await resolver.GetWalPartitionsAsync(Tree);
        for (var partition = 0; partition < partitions; partition++)
        {
            await provider.TrimAsync(Tree, partition, long.MaxValue, CancellationToken.None);
        }

        var result = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc-fallback", BackupScopeSelector.WholeTree(Tree), baseBackup.BackupId));

        Assert.Multiple(() =>
        {
            // Rather than emit a torn increment, the engine falls back to a fresh
            // full backup that inherits the base scope.
            Assert.That(result.Manifest.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(result.Manifest.BaseBackupId, Is.Null);
            Assert.That(result.Manifest.Scope.TreeId, Is.EqualTo(Tree));
        });
    }

    // ---- Range delete falls back to a full -------------------------------

    [Test]
    public async Task CaptureIncrementalAsync_falls_back_to_a_full_when_a_range_delete_surfaces_in_the_window()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        var keys = Enumerable.Range(0, 6).Select(i => $"k{i:D2}").ToArray();
        foreach (var key in keys)
        {
            await tree.SetAsync(key, Bytes("v1"));
        }

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Tree)));

        // A range delete after the base cut has no faithful point-keyed representation
        // in the uniform entry artifact, so it must force a full-backup fallback.
        await tree.DeleteRangeAsync("k02", "k04");

        var result = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc-range", BackupScopeSelector.WholeTree(Tree), baseBackup.BackupId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Manifest.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(result.Manifest.BaseBackupId, Is.Null);
            Assert.That(result.Manifest.Scope.TreeId, Is.EqualTo(Tree));
        });
    }

    // ---- Missing base ----------------------------------------------------

    [Test]
    public void CaptureIncrementalAsync_throws_when_the_base_backup_is_missing()
    {
        Assert.That(
            async () =>
            {
                await _fixture.InitializeAsync();
                await _fixture.Incremental.CaptureIncrementalAsync(
                    new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(Tree), "does-not-exist"));
            },
            Throws.TypeOf<KeyNotFoundException>());
    }

    // ---- Helpers ---------------------------------------------------------

    private async Task<List<LwwEntry>> DecodeEntriesAsync(BackupManifest manifest)
    {
        var descriptor = manifest.ContentDescriptors.Single();
        var all = new List<LwwEntry>();
        await foreach (var chunk in _fixture.Sink.ReadArtifactAsync(descriptor.ArtifactId))
        {
            all.AddRange(_fixture.Serializer.Deserialize<LwwEntry[]>(chunk));
        }

        return all;
    }

    /// <summary>
    /// Folds a base full backup's entries and an increment's delta entries into the
    /// resulting live key -> value map, resolving each key by last-writer-wins on the
    /// hybrid-logical-clock timestamp. Mirrors what a restore does when it replays a
    /// base followed by its ordered increments through the uniform entry path.
    /// </summary>
    private static Dictionary<string, string> FoldBaseAndDelta(
        List<LwwEntry> baseEntries,
        List<LwwEntry> delta)
    {
        var state = new Dictionary<string, (byte[]? Value, bool Tombstone, HybridLogicalClock Hlc)>(StringComparer.Ordinal);
        foreach (var entry in baseEntries.Concat(delta))
        {
            if (!state.TryGetValue(entry.Key, out var current) || entry.Timestamp.CompareTo(current.Hlc) >= 0)
            {
                state[entry.Key] = (entry.Value, entry.IsTombstone, entry.Timestamp);
            }
        }

        return state
            .Where(kv => !kv.Value.Tombstone)
            .ToDictionary(kv => kv.Key, kv => Str(kv.Value.Value!), StringComparer.Ordinal);
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);
}
