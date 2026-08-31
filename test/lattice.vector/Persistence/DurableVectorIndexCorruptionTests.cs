using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Self-healing: every way a persisted index can be damaged must be detected and
/// answered by rebuilding it, never by serving it.
/// <para>
/// The asymmetry that makes this the right answer is worth stating: the index is
/// a derived projection, so discarding it costs only the time to recompute it,
/// while the store of record it derives from may never be treated that way.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexCorruptionTests
{
    private const int Corpus = 400;

    private static string Prefix => DurableIndexHarness.Options().KeyPrefix;

    private static IReadOnlyList<string> VectorChunkKeys(InMemoryVectorIndexStore store, long generation) =>
        store.KeysWithPrefix(VectorIndexStorageKeys.GenerationPrefix(Prefix, generation) + "v/");

    private static IReadOnlyList<string> CentroidChunkKeys(InMemoryVectorIndexStore store, long generation) =>
        store.KeysWithPrefix(VectorIndexStorageKeys.GenerationPrefix(Prefix, generation) + "c/");

    private static IReadOnlyList<string> PartitionStateKeys(InMemoryVectorIndexStore store, long generation) =>
        store.KeysWithPrefix(VectorIndexStorageKeys.PartitionStatePrefix(Prefix, generation));

    /// <summary>
    /// Damages a freshly built index, asserts that reopening refuses to serve any
    /// of it, and asserts that rebuilding restores the identical answer.
    /// </summary>
    private static async Task AssertRebuiltAsync(Action<InMemoryVectorIndexStore, long> damage)
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[DurableIndexHarness.Id(9)];

        var built = await DurableIndexHarness.BuiltAsync(store, source, options);
        var expected = DurableIndexHarness.SearchIds(built, query, 10);

        damage(store, built.Generation);

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.Zero,
                "A persisted index that cannot be verified must not be served, not even partially.");
            Assert.That(reopened.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.NotStarted));
            Assert.That(reopened.Progress.RestoredFromDurableState, Is.False);
            Assert.That(store.KeysWithPrefix(VectorIndexStorageKeys.AllGenerationsPrefix(Prefix)), Is.Empty,
                "The unusable generation is swept rather than left to be found again.");
        });

        await reopened.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(Corpus));
            Assert.That(DurableIndexHarness.SearchIds(reopened, query, 10), Is.EqualTo(expected),
                "A rebuild must reproduce the index, not merely produce one.");
        });
    }

    [Test]
    public Task A_corrupt_manifest_is_rebuilt() => AssertRebuiltAsync(
        (store, _) => store.Corrupt(
            VectorIndexStorageKeys.Manifest(Prefix), VectorIndexPersistenceFormat.RecordHeaderSize + 4));

    [Test]
    public Task A_truncated_manifest_is_rebuilt() => AssertRebuiltAsync(
        (store, _) => store.Truncate(VectorIndexStorageKeys.Manifest(Prefix), 12));

    [Test]
    public Task An_empty_manifest_record_is_rebuilt() => AssertRebuiltAsync(
        (store, _) => store.Overwrite(VectorIndexStorageKeys.Manifest(Prefix), []));

    [Test]
    public Task A_missing_manifest_is_rebuilt() => AssertRebuiltAsync(
        (store, _) => store.Drop(VectorIndexStorageKeys.Manifest(Prefix)));

    [Test]
    public Task A_manifest_from_an_unsupported_record_layout_is_rebuilt() => AssertRebuiltAsync(
        (store, _) =>
        {
            var record = store.Read(VectorIndexStorageKeys.Manifest(Prefix));
            record[4] = 99;
        });

    [Test]
    public Task A_manifest_carrying_an_unsupported_snapshot_version_is_rebuilt() => AssertRebuiltAsync(
        (store, _) =>
        {
            var key = VectorIndexStorageKeys.Manifest(Prefix);
            Assert.That(VectorIndexManifest.TryReadRecord(store.Read(key), out var manifest), Is.True);

            var future = manifest with
            {
                Header = manifest.Header with { FormatVersion = VectorIndexFormat.Version + 1 },
            };

            store.Overwrite(key, future.ToRecord());
        });

    [Test]
    public Task A_manifest_written_for_a_different_dimensionality_is_rebuilt() => AssertRebuiltAsync(
        (store, _) =>
        {
            var key = VectorIndexStorageKeys.Manifest(Prefix);
            Assert.That(VectorIndexManifest.TryReadRecord(store.Read(key), out var manifest), Is.True);

            var mismatched = manifest with
            {
                Header = manifest.Header with { Dimensions = DurableIndexHarness.Dimensions * 2 },
            };

            store.Overwrite(key, mismatched.ToRecord());
        });

    [Test]
    public Task A_manifest_written_for_a_different_metric_is_rebuilt() => AssertRebuiltAsync(
        (store, _) =>
        {
            var key = VectorIndexStorageKeys.Manifest(Prefix);
            Assert.That(VectorIndexManifest.TryReadRecord(store.Read(key), out var manifest), Is.True);

            var mismatched = manifest with
            {
                Header = manifest.Header with { Metric = VectorDistanceMetric.DotProduct },
            };

            store.Overwrite(key, mismatched.ToRecord());
        });

    [Test]
    public Task A_corrupt_vector_chunk_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) => store.Corrupt(
            VectorChunkKeys(store, generation)[0], VectorIndexPersistenceFormat.RecordHeaderSize + 3));

    [Test]
    public Task A_truncated_vector_chunk_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) => store.Truncate(VectorChunkKeys(store, generation)[0], 30));

    [Test]
    public Task A_missing_vector_chunk_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) => store.Drop(VectorChunkKeys(store, generation)[^1]));

    [Test]
    public Task A_corrupt_centroid_chunk_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) => store.Corrupt(
            CentroidChunkKeys(store, generation)[0], VectorIndexPersistenceFormat.RecordHeaderSize + 1));

    [Test]
    public Task A_missing_centroid_chunk_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) => store.Drop(CentroidChunkKeys(store, generation)[0]));

    [Test]
    public Task A_corrupt_partition_state_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) => store.Corrupt(
            PartitionStateKeys(store, generation)[0], VectorIndexPersistenceFormat.RecordHeaderSize));

    [Test]
    public Task A_missing_partition_state_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) => store.Drop(PartitionStateKeys(store, generation)[1]));

    [Test]
    public Task A_partition_state_claiming_chunks_that_do_not_exist_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) =>
        {
            var key = PartitionStateKeys(store, generation)[0];
            Assert.That(VectorIndexPartitionState.TryReadRecord(store.Read(key), out var state), Is.True);
            store.Overwrite(key, (state with { ChunkCount = state.ChunkCount + 5 }).ToRecord());
        });

    [Test]
    public Task A_partition_state_pointing_at_an_epoch_that_was_never_written_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) =>
        {
            var key = PartitionStateKeys(store, generation)[0];
            Assert.That(VectorIndexPartitionState.TryReadRecord(store.Read(key), out var state), Is.True);
            store.Overwrite(key, (state with { Epoch = state.Epoch + 1_000 }).ToRecord());
        });

    [Test]
    public Task A_partition_state_hiding_committed_chunks_is_rebuilt() => AssertRebuiltAsync(
        (store, generation) =>
        {
            // The count comes out short, which is the shape a mixture of two
            // generations would take: every record verifies on its own and only
            // the total disagrees.
            var key = PartitionStateKeys(store, generation)
                .First(candidate =>
                    VectorIndexPartitionState.TryReadRecord(store.Read(candidate), out var state)
                    && state.ChunkCount > 0);

            Assert.That(VectorIndexPartitionState.TryReadRecord(store.Read(key), out var found), Is.True);
            store.Overwrite(key, (found with { ChunkCount = found.ChunkCount - 1 }).ToRecord());
        });

    [Test]
    public async Task A_corrupt_identifier_mapping_is_dropped_and_reassigned_rather_than_guessed_at()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();

        var built = await DurableIndexHarness.BuiltAsync(store, source, options);
        var mappingKey = VectorIndexStorageKeys.KeyMap(Prefix, DurableIndexHarness.Id(5));
        Assert.That(built.TryGetKey(DurableIndexHarness.Id(5), out _), Is.True);

        store.Corrupt(mappingKey, VectorIndexPersistenceFormat.RecordHeaderSize);

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(reopened.Count, Is.Zero,
            "The mapping and the cells are one index; an undecodable mapping makes the loaded count disagree.");

        await reopened.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(Corpus));
            Assert.That(reopened.TryGetKey(DurableIndexHarness.Id(5), out _), Is.True);
        });
    }

    [Test]
    public async Task An_explicit_rebuild_discards_everything_and_recomputes_the_same_index()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[DurableIndexHarness.Id(13)];

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var expected = DurableIndexHarness.SearchIds(index, query, 10);

        await index.RebuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Count, Is.Zero);
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.NotStarted));
            Assert.That(store.KeysWithPrefix(VectorIndexStorageKeys.AllGenerationsPrefix(Prefix)), Is.Empty);
        });

        await index.RunBuildAsync();

        Assert.That(DurableIndexHarness.SearchIds(index, query, 10), Is.EqualTo(expected));
    }

    [Test]
    public async Task A_rebuild_never_reissues_an_identifier_key()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var before = new HashSet<long>();
        foreach (var id in source.Ids)
        {
            Assert.That(index.TryGetKey(id, out var key), Is.True);
            before.Add(key);
        }

        await index.RebuildAsync();
        await index.RunBuildAsync();

        foreach (var id in source.Ids)
        {
            Assert.That(index.TryGetKey(id, out var key), Is.True);
            Assert.That(before, Does.Not.Contain(key),
                "The identifier counter must not rewind on a rebuild: a reissued key could collide with a "
                + "reference taken before it.");
        }
    }
}
