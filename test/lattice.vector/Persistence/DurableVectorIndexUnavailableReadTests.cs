using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Issue #3905: a converged index was discarded as an unloadable record because
/// the store could not serve some of its reads while a WAL replay-permit storm
/// was in progress. Nothing about the index was damaged; the reader simply could
/// not reach part of it at that moment.
/// </summary>
/// <remarks>
/// <para>
/// The distinction these tests pin is between "this record is not valid" and
/// "this read did not return the record". Only the first justifies destroying
/// hours of build, and the two are separated by asking a second read path: a
/// record one read omitted but another returns is intact, so the load defers and
/// retries instead of discarding. A record that is absent on every path is
/// still discarded, which <see cref="DurableVectorIndexCorruptionTests"/> covers
/// case by case and the last tests below re-assert through this fixture's store.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DurableVectorIndexUnavailableReadTests
{
    private const int Corpus = 400;

    /// <summary>Which read path fails to return which record.</summary>
    public enum Gap
    {
        /// <summary>A batched read omits one vector chunk.</summary>
        VectorChunkFromBatchRead,

        /// <summary>A prefix scan skips one partition state.</summary>
        PartitionStateFromScan,

        /// <summary>A prefix scan skips one centroid chunk.</summary>
        CentroidChunkFromScan,

        /// <summary>A point read reports the manifest absent.</summary>
        ManifestFromPointRead,
    }

    private static string Prefix => DurableIndexHarness.Options().KeyPrefix;

    private static async Task<(ReadGapVectorIndexStore Store, ListVectorSource Source, List<string> Expected, long Generation)>
        BuiltAsync()
    {
        var inner = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var built = await DurableIndexHarness.BuiltAsync(inner, source, DurableIndexHarness.Options());
        var expected = DurableIndexHarness.SearchIds(built, source[DurableIndexHarness.Id(9)], 10);
        return (new ReadGapVectorIndexStore(inner), source, expected, built.Generation);
    }

    private static void Open(ReadGapVectorIndexStore store, Gap gap, long generation)
    {
        var generationPrefix = VectorIndexStorageKeys.GenerationPrefix(Prefix, generation);
        switch (gap)
        {
            case Gap.VectorChunkFromBatchRead:
                store.HiddenFromBatchReads.Add(store.Inner.KeysWithPrefix(generationPrefix + "v/")[^1]);
                break;
            case Gap.PartitionStateFromScan:
                store.HiddenFromScans.Add(
                    store.Inner.KeysWithPrefix(VectorIndexStorageKeys.PartitionStatePrefix(Prefix, generation))[1]);
                break;
            case Gap.CentroidChunkFromScan:
                store.HiddenFromScans.Add(store.Inner.KeysWithPrefix(generationPrefix + "c/")[0]);
                break;
            case Gap.ManifestFromPointRead:
                store.HiddenFromPointReads.Add(VectorIndexStorageKeys.Manifest(Prefix));
                break;
        }
    }

    [TestCase(Gap.VectorChunkFromBatchRead)]
    [TestCase(Gap.PartitionStateFromScan)]
    [TestCase(Gap.CentroidChunkFromScan)]
    [TestCase(Gap.ManifestFromPointRead)]
    public async Task A_record_one_read_omits_and_another_returns_defers_the_load_instead_of_discarding(Gap gap)
    {
        var (store, source, expected, generation) = await BuiltAsync();
        var records = store.Inner.RecordCount;
        var writes = store.Inner.Writes;
        Open(store, gap, generation);

        var index = DurableVectorIndex.CreateUnloaded(store, source, DurableIndexHarness.Options());

        Assert.That(
            async () => await index.LoadOrResumeAsync(),
            Throws.TypeOf<VectorIndexRecordUnavailableException>(),
            "A record the store could not serve on one read path but did serve on another is intact. "
            + "Treating the gap as damage is what destroyed a converged 174,900-vector index in #3905.");

        Assert.Multiple(() =>
        {
            Assert.That(store.Inner.Writes, Is.EqualTo(writes), "A deferred load must not delete or write anything.");
            Assert.That(store.Inner.RecordCount, Is.EqualTo(records), "Every durable record must survive the deferral.");
            Assert.That(index.IsLoaded, Is.False);
            Assert.That(index.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.None));
            Assert.That(index.LoadDiscardedManifest, Is.Null);
            Assert.That(index.HasBankedLoadProgress, Is.True,
                "The completed key walk is kept, so the retry does not re-read the identifier map.");
        });

        // The pressure clears and the same instance is retried, as the handle does
        // on its next tick. It must converge on the index that was built.
        store.Heal();
        await index.LoadOrResumeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.IsLoaded, Is.True);
            Assert.That(index.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.None));
            Assert.That(index.Count, Is.EqualTo(Corpus));
            Assert.That(index.Progress.RestoredFromDurableState, Is.True);
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(DurableIndexHarness.SearchIds(index, source[DurableIndexHarness.Id(9)], 10),
                Is.EqualTo(expected));
        });
    }

    [TestCase(Gap.VectorChunkFromBatchRead)]
    [TestCase(Gap.PartitionStateFromScan)]
    [TestCase(Gap.CentroidChunkFromScan)]
    public async Task A_record_hidden_from_every_read_path_is_still_discarded(Gap gap)
    {
        // Anti-vacuity for the deferral: the confirmation must be able to say
        // "absent". A load that deferred on every gap would pass the test above
        // and wedge a genuinely damaged index for ever.
        var (store, source, _, generation) = await BuiltAsync();
        Open(store, gap, generation);
        var hidden = store.HiddenFromBatchReads.Concat(store.HiddenFromScans).Single();
        store.HiddenFromBatchReads.Add(hidden);
        store.HiddenFromScans.Add(hidden);
        store.HiddenFromPointReads.Add(hidden);

        var index = await DurableVectorIndex.OpenAsync(store, source, DurableIndexHarness.Options());

        Assert.Multiple(() =>
        {
            Assert.That(index.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.UnloadableRecord));
            Assert.That(index.Count, Is.Zero);
        });
    }

    [Test]
    public async Task A_discard_of_a_committed_index_reports_the_manifest_it_destroyed()
    {
        var inner = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var built = await DurableIndexHarness.BuiltAsync(inner, source, DurableIndexHarness.Options());
        Assert.That(
            VectorIndexManifest.TryReadRecord(inner.Read(VectorIndexStorageKeys.Manifest(Prefix)), out var manifest),
            Is.True);

        var generationPrefix = VectorIndexStorageKeys.GenerationPrefix(Prefix, built.Generation);
        inner.Drop(inner.KeysWithPrefix(generationPrefix + "v/")[^1]);

        var reopened = await DurableIndexHarness.OpenAsync(inner, source, DurableIndexHarness.Options());

        Assert.Multiple(() =>
        {
            Assert.That(reopened.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.UnloadableRecord));
            Assert.That(reopened.LoadDiscardedManifest, Is.EqualTo(manifest),
                "The cost of a discard has to be visible: which generation, how many vectors, how many "
                + "partitions were destroyed.");
            Assert.That(reopened.LoadDiscardedManifest!.Value.IndexedCount, Is.EqualTo(Corpus));
        });
    }

    [Test]
    public async Task A_discard_with_no_decodable_manifest_reports_none()
    {
        var inner = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(inner, source, DurableIndexHarness.Options());
        inner.Overwrite(VectorIndexStorageKeys.Manifest(Prefix), [1, 2, 3]);

        var reopened = await DurableIndexHarness.OpenAsync(inner, source, DurableIndexHarness.Options());

        Assert.Multiple(() =>
        {
            Assert.That(reopened.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.UnloadableRecord));
            Assert.That(reopened.LoadDiscardedManifest, Is.Null,
                "Nothing converged can be named when the commit record itself is unreadable.");
        });
    }

    [Test]
    public async Task An_untouched_store_reports_no_discarded_manifest()
    {
        var (store, source, _, _) = await BuiltAsync();

        var index = await DurableVectorIndex.OpenAsync(store, source, DurableIndexHarness.Options());

        Assert.Multiple(() =>
        {
            Assert.That(index.LoadDiscardReason, Is.EqualTo(VectorIndexLoadDiscardReason.None));
            Assert.That(index.LoadDiscardedManifest, Is.Null);
            Assert.That(index.Count, Is.EqualTo(Corpus));
        });
    }
}
