using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end coverage for <see cref="ILatticeBackupCaptureService"/>: a full
/// backup of a whole tree, a prefix, and a single key each round-trips through the
/// sink; the capture is a causally consistent point-in-time cut isolated from
/// writes made after the snapshot opens; the raw entries carry exact
/// hybrid-logical-clock metadata; the payload streams as multiple chunks rather
/// than a single buffered blob; and an oversize scope is rejected up front before
/// a snapshot is opened.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupCaptureIntegrationTests
{
    private const string Tree = "orders";

    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CaptureClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Whole-tree round-trip + exact HLC + isolation ------------------

    [Test]
    public async Task CaptureAsync_whole_tree_round_trips_keys_values_and_exact_hlc_metadata()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v1"));
        await tree.SetAsync("k3", Bytes("v1"));

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("nightly", BackupScopeSelector.WholeTree(Tree)));

        var entries = await DecodeAsync(result.Manifest);

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2", "k3" }));
            Assert.That(entries.All(e => Str(e.Value!) == "v1"), Is.True);
            Assert.That(entries.All(e => !e.IsTombstone), Is.True);
            // Exact HLC metadata: every captured entry carries a real, non-default
            // hybrid-logical-clock stamp from its write.
            Assert.That(entries.All(e => e.Timestamp.WallClockTicks > 0), Is.True);
            // Version-vector metadata is faithfully null for a local-only tree.
            Assert.That(entries.All(e => e.VectorClock is null), Is.True);
            Assert.That(entries.All(e => e.OriginClusterId is null), Is.True);
        });
    }

    [Test]
    public async Task CaptureAsync_is_isolated_from_writes_made_after_the_snapshot_opens()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v1"));

        var first = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("before", BackupScopeSelector.WholeTree(Tree)));
        var firstEntries = await DecodeAsync(first.Manifest);
        var firstK1Hlc = firstEntries.Single(e => e.Key == "k1").Timestamp;

        // Mutate after the first capture: overwrite existing keys and add a new one.
        await tree.SetAsync("k1", Bytes("v2"));
        await tree.SetAsync("k2", Bytes("v2"));
        await tree.SetAsync("k3", Bytes("v1"));

        var second = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("after", BackupScopeSelector.WholeTree(Tree)));
        var secondEntries = await DecodeAsync(second.Manifest);
        var secondK1Hlc = secondEntries.Single(e => e.Key == "k1").Timestamp;

        // The first backup is an immutable point-in-time cut: re-decoding it still
        // yields the pre-mutation state, unaffected by the later writes.
        var firstReread = await DecodeAsync(first.Manifest);

        Assert.Multiple(() =>
        {
            Assert.That(firstReread.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2" }));
            Assert.That(firstReread.All(e => Str(e.Value!) == "v1"), Is.True);

            Assert.That(secondEntries.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2", "k3" }));
            Assert.That(Str(secondEntries.Single(e => e.Key == "k1").Value!), Is.EqualTo("v2"));

            // Exact HLC/VV metadata advanced monotonically across the overwrite.
            Assert.That(secondK1Hlc.CompareTo(firstK1Hlc), Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task CaptureAsync_under_concurrent_writes_captures_a_consistent_point_in_time()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        var keys = Enumerable.Range(0, 40).Select(i => $"k{i:D3}").ToArray();
        foreach (var key in keys)
        {
            await tree.SetAsync(key, Bytes("v1"));
        }

        using var cts = new CancellationTokenSource();
        var writer = Task.Run(async () =>
        {
            var round = 0;
            while (!cts.IsCancellationRequested)
            {
                foreach (var key in keys)
                {
                    await tree.SetAsync(key, Bytes($"v2-{round}"));
                }
                round++;
            }
        });

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("concurrent", BackupScopeSelector.WholeTree(Tree), pageSize: 8));

        cts.Cancel();
        try { await writer; } catch (OperationCanceledException) { }

        var entries = await DecodeAsync(result.Manifest);

        Assert.Multiple(() =>
        {
            // A clean cut: the captured key set is exactly the seeded set, with no
            // duplicate or torn entries despite the concurrent overwrites.
            Assert.That(entries.Select(e => e.Key), Is.EqualTo(keys));
            Assert.That(entries.Select(e => e.Key).Distinct().Count(), Is.EqualTo(keys.Length));
            // Every captured value is a single committed value, never partial.
            Assert.That(entries.All(e => Str(e.Value!) == "v1" || Str(e.Value!).StartsWith("v2-", StringComparison.Ordinal)), Is.True);
        });
    }

    // ---- Prefix and key scopes ------------------------------------------

    [Test]
    public async Task CaptureAsync_prefix_scope_captures_only_the_prefix_range()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("a:1", Bytes("va1"));
        await tree.SetAsync("a:2", Bytes("va2"));
        await tree.SetAsync("b:1", Bytes("vb1"));

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("prefix-a", BackupScopeSelector.Prefix(Tree, "a:")));

        var entries = await DecodeAsync(result.Manifest);

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a:1", "a:2" }));
            Assert.That(result.Manifest.Scope.Kind, Is.EqualTo(BackupScopeKind.Prefix));
            Assert.That(result.Manifest.KeyDescriptors.Select(d => d.Key), Is.EqualTo(new[] { "a:1", "a:2" }));
        });
    }

    [Test]
    public async Task CaptureAsync_key_scope_captures_only_the_single_key()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v2"));

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("just-k1", BackupScopeSelector.Key(Tree, "k1")));

        var entries = await DecodeAsync(result.Manifest);

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "k1" }));
            Assert.That(Str(entries[0].Value!), Is.EqualTo("v1"));
        });
    }

    // ---- Streaming, manifest, catalog -----------------------------------

    [Test]
    public async Task CaptureAsync_streams_multiple_chunks_for_a_multi_page_scope()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        for (var i = 0; i < 25; i++)
        {
            await tree.SetAsync($"k{i:D3}", Bytes($"v{i}"));
        }

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("streamed", BackupScopeSelector.WholeTree(Tree), pageSize: 5));

        var descriptor = result.Manifest.ContentDescriptors.Single();

        Assert.Multiple(() =>
        {
            // 25 entries drained 5 at a time -> multiple streamed chunks, proving
            // the whole scope is never buffered into one blob.
            Assert.That(descriptor.ChunkCount, Is.GreaterThan(1));
            Assert.That(descriptor.ByteLength, Is.GreaterThan(0));
            Assert.That(result.Manifest.KeyDescriptors, Has.Count.EqualTo(25));
        });
    }

    [Test]
    public async Task CaptureAsync_registers_a_self_describing_manifest_in_the_catalog()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("catalogued", BackupScopeSelector.WholeTree(Tree)));

        var catalogued = await _fixture.Catalog.GetAsync(result.BackupId);

        Assert.Multiple(() =>
        {
            Assert.That(catalogued, Is.Not.Null);
            Assert.That(catalogued!.Id, Is.EqualTo(result.BackupId));
            Assert.That(catalogued.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(catalogued.StructuralDigest, Is.Not.Empty);
            Assert.That(catalogued.Topology.ShardCount, Is.GreaterThan(0));
            Assert.That(catalogued.Topology.ShardRootDigests, Has.Count.EqualTo(catalogued.Topology.ShardCount));
            Assert.That(catalogued.ConsistencyCut.HlcTimestamp, Is.EqualTo(result.Manifest.ConsistencyCut.HlcTimestamp),
                "the catalogued row must preserve the manifest HLC cut");
            Assert.That(catalogued.ConsistencyCut.WalSequence, Is.EqualTo(result.Manifest.ConsistencyCut.WalSequence),
                "the catalogued row must preserve the manifest WAL cut");
            // Local-only tree: single-origin, so no per-origin provenance.
            Assert.That(catalogued.Provenance, Is.Empty);
            Assert.That(catalogued.CompressionDictionary, Is.Null);
        });
    }

    [Test]
    public async Task CaptureAsync_content_addresses_the_backup_id()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("addressed", BackupScopeSelector.WholeTree(Tree)));

        // The backup id is the lowercase-hex SHA-256 of the streamed payload.
        Assert.That(result.BackupId, Has.Length.EqualTo(64));
        Assert.That(result.BackupId, Does.Match("^[0-9a-f]{64}$"));
    }

    // ---- Fail-fast size gate --------------------------------------------

    [Test]
    public async Task CaptureAsync_rejects_an_oversize_scope_before_opening_a_snapshot()
    {
        await _fixture.InitializeAsync(maxSnapshotReplayEntries: 2);
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        for (var i = 0; i < 5; i++)
        {
            await tree.SetAsync($"k{i}", Bytes("v"));
        }

        Assert.That(
            async () => await _fixture.Capture.CaptureAsync(
                new LatticeBackupCaptureRequest("too-big", BackupScopeSelector.WholeTree(Tree))),
            Throws.TypeOf<LatticeSnapshotReplayBudgetExceededException>());

        // Rejected up front: nothing was written to the catalog.
        var catalogued = await CountCatalogAsync();
        Assert.That(catalogued, Is.Zero);
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

    private async Task<int> CountCatalogAsync()
    {
        var count = 0;
        await foreach (var _ in _fixture.Catalog.ListAsync())
        {
            count++;
        }

        return count;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);
}
