using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The two arms of the durable restore that refuse committed state which parsed
/// cleanly but does not describe an index this build can rebuild: a manifest
/// header the index refuses to restore from, and a partition-state key that does
/// not name a partition the manifest declares.
/// </summary>
/// <remarks>
/// <para>
/// Both sit downstream of a record that passed its checksum, so no corruption
/// fixture reaches them - the envelope is intact and the fields decode. They are
/// the difference between "these bytes are damaged" and "these bytes are
/// self-consistent but describe something else", and only the first had tests.
/// </para>
/// <para>
/// The behaviour that matters is that neither is a fault. A durable index is a
/// derived projection, so the correct answer to state it cannot verify is to
/// discard and rebuild - and an exception escaping here would instead wedge
/// every open of that store until an operator intervened.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DurableVectorIndexRestoreRejectionTests
{
    private const int Corpus = 320;
    private const string Prefix = "restore-reject/";

    private static DurableVectorIndexOptions Options() => new()
    {
        KeyPrefix = Prefix,
        MaxItemsPerChunk = 32,
        IngestBatchSize = 128,
        Index = new VectorIndexOptions
        {
            Dimensions = DurableIndexHarness.Dimensions,
            PartitionCount = 8,
            Probes = 4,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 1_024,
        },
    };

    private static VectorIndexManifest ReadManifest(InMemoryVectorIndexStore store)
    {
        Assert.That(
            VectorIndexManifest.TryReadRecord(store.Read(VectorIndexStorageKeys.Manifest(Prefix)), out var manifest),
            Is.True,
            "The fixture needs a committed manifest to doctor.");
        return manifest;
    }

    [Test]
    public async Task A_manifest_the_index_refuses_to_restore_from_is_discarded_and_rebuilt()
    {
        // The header decodes - every field is in range, and the dimensionality
        // and metric still match the configured space, so the two checks ahead of
        // the restore both pass. What it describes is impossible: a partitioning
        // with no centroid chunks to restore it from. VectorIndex.Restore refuses
        // that, and the load has to treat the refusal as "rebuild", not as a
        // fault that escapes the open.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(store, source, Options());

        var manifest = ReadManifest(store);
        Assert.That(manifest.Header.PartitionCount, Is.GreaterThan(0));
        Assert.That(manifest.Header.CentroidChunkCount, Is.GreaterThan(0));

        var doctored = manifest with { Header = manifest.Header with { CentroidChunkCount = 0 } };
        Assert.That(doctored.Header.Dimensions, Is.EqualTo(Options().Index.Dimensions),
            "The embedding-space check must still pass, or a different arm refuses first.");
        Assert.That(doctored.Header.Metric, Is.EqualTo(Options().Index.Metric));
        store.Overwrite(VectorIndexStorageKeys.Manifest(Prefix), doctored.ToRecord());

        DurableVectorIndex reopened = null!;
        Assert.DoesNotThrowAsync(
            async () => reopened = await DurableIndexHarness.OpenAsync(store, source, Options()));

        Assert.That(reopened.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.UnloadableRecord),
            "An unrestorable header is an unloadable record, not an embedding-space change.");
        Assert.That(reopened.Count, Is.Zero, "The discarded index is rebuilt from the source, not adopted.");

        await reopened.RunBuildAsync();
        Assert.That(reopened.Count, Is.EqualTo(Corpus));
        Assert.That(
            DurableIndexHarness.SearchIds(reopened, source[DurableIndexHarness.Id(5)], 1),
            Is.EqualTo(new[] { DurableIndexHarness.Id(5) }));
    }

    [Test]
    public async Task An_unrestorable_manifest_is_refused_before_any_embedding_space_change_is_blamed()
    {
        // Scope discipline. The reason code drives what an operator does next, so
        // the catch must not be reachable by, or confused with, the mismatch the
        // check above it already names. Changing the metric takes the other arm
        // and must report the other reason.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(store, source, Options());

        var changed = Options();
        changed.Index.Metric = VectorDistanceMetric.DotProduct;

        var reopened = await DurableIndexHarness.OpenAsync(store, source, changed);

        Assert.That(reopened.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.EmbeddingSpaceChange));
    }

    [TestCase("not-a-number", TestName = "A partition-state key that does not parse is refused")]
    [TestCase("00000999", TestName = "A partition-state key naming a partition the manifest lacks is refused")]
    public async Task A_partition_state_key_that_does_not_name_a_declared_partition_is_discarded(string suffix)
    {
        // The key suffix is the only place the partition identifier comes from,
        // and it indexes straight into fixed-size per-partition arrays. A key
        // left behind by an older layout, or written under a different
        // partitioning, therefore has to be refused rather than parsed
        // optimistically - both terms of that guard, the parse and the range.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(store, source, Options());

        var manifest = ReadManifest(store);
        var statePrefix = VectorIndexStorageKeys.PartitionStatePrefix(Prefix, manifest.Generation);
        Assert.That(store.KeysWithPrefix(statePrefix), Is.Not.Empty,
            "The load must already be reading real partition states, or the injected key proves nothing.");

        store.Overwrite(statePrefix + suffix, [1, 2, 3, 4]);

        DurableVectorIndex reopened = null!;
        Assert.DoesNotThrowAsync(
            async () => reopened = await DurableIndexHarness.OpenAsync(store, source, Options()));

        Assert.That(reopened.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.UnloadableRecord));
        Assert.That(reopened.Count, Is.Zero);

        await reopened.RunBuildAsync();
        Assert.That(reopened.Count, Is.EqualTo(Corpus));
    }

    [Test]
    public async Task An_untouched_store_is_adopted_rather_than_rebuilt()
    {
        // Anti-vacuity for the fixture as a whole: every test above asserts a
        // discard, and a load that discarded unconditionally would satisfy all of
        // them. This is the same store, same options, nothing doctored.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(store, source, Options());

        var reopened = await DurableIndexHarness.OpenAsync(store, source, Options());

        Assert.That(reopened.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.None));
        Assert.That(reopened.Count, Is.EqualTo(Corpus));
    }
}
