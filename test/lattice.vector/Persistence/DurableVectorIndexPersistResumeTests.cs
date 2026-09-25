using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Committing a trained layout must be resumable: a failure part-way through it
/// is retried, and the retry must neither rewrite what the failed attempt already
/// committed nor write a generation beyond the one the commit is for (#3547).
/// <para>
/// Both halves of the commit are faulted in isolation. A failure while writing
/// the new generation must leave the partitions it committed recorded, so the
/// retry writes only the rest. A failure while deleting the superseded generation
/// must leave that generation remembered, so the retry only deletes: before the
/// fix it wrote a whole further generation and orphaned the superseded one.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexPersistResumeTests
{
    private const int Corpus = 700;
    private const string Prefix = "vidx/";

    [Test]
    public async Task A_persist_that_fails_part_way_resumes_after_the_partitions_it_committed()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var cleanWrites = await CleanPersistWritesAsync(source, options);

        var store = new InMemoryVectorIndexStore();
        var index = await TrainedAsync(store, source, options);
        var superseded = index.Generation;
        var target = superseded + 1;

        store.ResetBytesWritten();
        store.FailAfterWrites = store.Writes + (cleanWrites / 2);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.BuildStepAsync());
        var committed = CommittedPartitions(store.KeysWritten, target, options);
        Assert.That(committed, Is.Not.Empty.And.Count.LessThan(options.Index.PartitionCount),
            "The fault must land after some partitions committed and before all of them did, "
            + "or the resume has nothing to skip and nothing to write.");

        store.FailAfterWrites = -1;
        store.ResetBytesWritten();
        await index.BuildStepAsync();
        var retried = store.KeysWritten;

        Assert.Multiple(() =>
        {
            foreach (var partition in committed)
            {
                Assert.That(
                    retried.Where(key => IsPartitionKey(key, target, partition)),
                    Is.Empty,
                    $"Partition {partition} was committed by the failed attempt and must not be written again.");
            }

            for (var partition = 0; partition < options.Index.PartitionCount; partition++)
            {
                if (!committed.Contains(partition))
                {
                    Assert.That(
                        retried,
                        Does.Contain(VectorIndexStorageKeys.PartitionState(Prefix, target, partition)),
                        $"Partition {partition} was not committed before the fault and must be written now.");
                }
            }

            Assert.That(index.Generation, Is.EqualTo(target));
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(store.KeysWithPrefix(GenerationPrefix(superseded)), Is.Empty);
        });

        await AssertReloadsWholeAsync(store, source, options, target, Corpus);
    }

    [Test]
    public async Task A_persist_whose_superseded_delete_fails_retries_only_the_delete()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();
        var index = await TrainedAsync(store, source, options);
        var superseded = index.Generation;
        var target = superseded + 1;

        store.FailDeletePrefix = prefix => prefix == GenerationPrefix(superseded);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.BuildStepAsync());
        Assert.Multiple(() =>
        {
            Assert.That(index.Generation, Is.EqualTo(target), "The write landed; only the delete failed.");
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Persisting));
            Assert.That(store.KeysWithPrefix(GenerationPrefix(superseded)), Is.Not.Empty);
        });

        store.FailDeletePrefix = null;
        store.ResetBytesWritten();
        await index.BuildStepAsync();

        AssertOnlyDeleted(store, index, superseded, target);
        await AssertReloadsWholeAsync(store, source, options, target, Corpus);
    }

    [Test]
    public async Task A_superseded_delete_that_failed_before_a_restart_is_finished_by_the_next_build_step()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();
        var index = await TrainedAsync(store, source, options);
        var superseded = index.Generation;
        var target = superseded + 1;

        store.FailDeletePrefix = prefix => prefix == GenerationPrefix(superseded);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.BuildStepAsync());
        store.FailDeletePrefix = null;

        // The process dies here: nothing but the store survives.
        var restarted = await DurableIndexHarness.OpenAsync(store, source, options);
        Assert.Multiple(() =>
        {
            Assert.That(restarted.Generation, Is.EqualTo(target));
            Assert.That(restarted.Count, Is.EqualTo(Corpus));
            Assert.That(restarted.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Persisting),
                "The stale build state is the only durable trace of the unfinished delete, so the "
                + "restarted writer must resume it rather than report Ready over an orphan.");
        });

        store.ResetBytesWritten();
        await restarted.RunBuildAsync();

        AssertOnlyDeleted(store, restarted, superseded, target);
        await AssertReloadsWholeAsync(store, source, options, target, Corpus);
    }

    [Test]
    public async Task A_lazy_reader_leaves_an_unfinished_delete_to_the_writer()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();
        var index = await TrainedAsync(store, source, options);
        var superseded = index.Generation;

        store.FailDeletePrefix = prefix => prefix == GenerationPrefix(superseded);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.BuildStepAsync());
        store.FailDeletePrefix = null;

        var reader = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        Assert.Multiple(() =>
        {
            Assert.That(reader.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready),
                "A reader cannot perform the cleanup, so it must not report a phase only a writer can leave.");
            Assert.That(store.KeysWithPrefix(GenerationPrefix(superseded)), Is.Not.Empty);
            Assert.That(store.KeysWithPrefix(VectorIndexStorageKeys.BuildState(Prefix)), Is.Not.Empty);
        });
    }

    [Test]
    public async Task A_resumed_persist_writes_a_vector_upserted_between_the_attempts()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var cleanWrites = await CleanPersistWritesAsync(source, options);

        var store = new InMemoryVectorIndexStore();
        var index = await TrainedAsync(store, source, options);
        var target = index.Generation + 1;

        store.FailAfterWrites = store.Writes + (cleanWrites / 2);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.BuildStepAsync());
        store.FailAfterWrites = -1;

        // Replaces a vector in every partition the failed attempt may already have
        // committed, as well as adding one, so a resume that skipped a committed
        // partition without checking its version would lose the change.
        var extra = VectorCorpus.Clustered(Corpus + 1, DurableIndexHarness.Dimensions, 8, 99);
        for (var i = 0; i < Corpus; i += 7)
        {
            await index.UpsertAsync(DurableIndexHarness.Id(i), extra[i]);
        }

        const string added = "doc-added";
        await index.UpsertAsync(added, extra[Corpus]);
        await index.BuildStepAsync();

        var reloaded = await AssertReloadsWholeAsync(store, source, options, target, Corpus + 1);
        Assert.That(DurableIndexHarness.SearchIds(reloaded, extra[Corpus], 1), Is.EqualTo(new[] { added }));
        for (var i = 0; i < Corpus; i += 7)
        {
            Assert.That(DurableIndexHarness.SearchIds(reloaded, extra[i], 1),
                Is.EqualTo(new[] { DurableIndexHarness.Id(i) }),
                $"The replacement of '{DurableIndexHarness.Id(i)}' was lost by the resumed write.");
        }
    }

    [Test]
    public async Task A_flush_during_an_unfinished_persist_completes_it_under_the_same_generation()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var cleanWrites = await CleanPersistWritesAsync(source, options);

        var store = new InMemoryVectorIndexStore();
        var index = await TrainedAsync(store, source, options);
        var superseded = index.Generation;
        var target = superseded + 1;

        store.FailAfterWrites = store.Writes + (cleanWrites / 2);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.BuildStepAsync());
        store.FailAfterWrites = -1;

        await index.FlushAsync();
        Assert.That(index.Generation, Is.EqualTo(target),
            "A flush must commit the generation in flight, not the one it replaces.");

        await index.BuildStepAsync();
        Assert.Multiple(() =>
        {
            Assert.That(index.Generation, Is.EqualTo(target));
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(store.KeysWithPrefix(GenerationPrefix(superseded)), Is.Empty);
            Assert.That(store.KeysWithPrefix(GenerationPrefix(target + 1)), Is.Empty);
        });

        await AssertReloadsWholeAsync(store, source, options, target, Corpus);
    }

    [Test]
    public async Task A_retrain_that_fails_part_way_resumes_after_the_partitions_it_committed()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();

        var control = new InMemoryVectorIndexStore();
        var clean = await DurableIndexHarness.BuiltAsync(control, source, options);
        var before = control.Writes;
        await clean.RetrainAsync();
        var cleanWrites = control.Writes - before;

        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var superseded = index.Generation;
        var target = superseded + 1;

        store.ResetBytesWritten();
        store.FailAfterWrites = store.Writes + (cleanWrites / 2);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.RetrainAsync());
        var committed = CommittedPartitions(store.KeysWritten, target, options);
        Assert.That(committed, Is.Not.Empty.And.Count.LessThan(options.Index.PartitionCount));

        store.FailAfterWrites = -1;
        store.ResetBytesWritten();
        await index.RetrainAsync();
        var retried = store.KeysWritten;

        Assert.Multiple(() =>
        {
            foreach (var partition in committed)
            {
                Assert.That(
                    retried.Where(key => IsPartitionKey(key, target, partition)),
                    Is.Empty,
                    $"Partition {partition} was committed by the failed retrain and must not be written again.");
            }

            Assert.That(index.Generation, Is.EqualTo(target));
            Assert.That(store.KeysWithPrefix(GenerationPrefix(superseded)), Is.Empty);
            Assert.That(store.KeysWithPrefix(GenerationPrefix(target + 1)), Is.Empty);
        });

        await AssertReloadsWholeAsync(store, source, options, target, Corpus);
    }

    [Test]
    public async Task A_retrain_whose_superseded_delete_fails_retries_only_the_delete()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var superseded = index.Generation;
        var target = superseded + 1;

        store.FailDeletePrefix = prefix => prefix == GenerationPrefix(superseded);
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.RetrainAsync());
        Assert.That(index.Generation, Is.EqualTo(target));

        store.FailDeletePrefix = null;
        store.ResetBytesWritten();
        await index.RetrainAsync();

        AssertOnlyDeleted(store, index, superseded, target);
        await AssertReloadsWholeAsync(store, source, options, target, Corpus);
    }

    [Test]
    public async Task A_retrain_after_a_completed_one_starts_a_fresh_generation()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var first = index.Generation;

        await index.RetrainAsync();
        await index.RetrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Generation, Is.EqualTo(first + 2),
                "A completed retrain leaves nothing in flight, so the next one must train and commit anew.");
            Assert.That(index.UpdatesSinceTraining, Is.Zero);
            Assert.That(store.KeysWithPrefix(GenerationPrefix(first)), Is.Empty);
            Assert.That(store.KeysWithPrefix(GenerationPrefix(first + 1)), Is.Empty);
        });

        await AssertReloadsWholeAsync(store, source, options, first + 2, Corpus);
    }

    private static string GenerationPrefix(long generation) =>
        VectorIndexStorageKeys.GenerationPrefix(Prefix, generation);

    private static async Task<DurableVectorIndex> TrainedAsync(
        InMemoryVectorIndexStore store, ListVectorSource source, DurableVectorIndexOptions options)
    {
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        while (index.Progress.Phase != VectorIndexBuildPhase.Training)
        {
            await index.BuildStepAsync();
        }

        return index;
    }

    /// <summary>
    /// The number of store writes the training step of an unfaulted build issues,
    /// which is what a fault is placed half-way into.
    /// </summary>
    private static async Task<int> CleanPersistWritesAsync(
        ListVectorSource source, DurableVectorIndexOptions options)
    {
        var store = new InMemoryVectorIndexStore();
        var index = await TrainedAsync(store, source, options);
        var before = store.Writes;
        await index.BuildStepAsync();
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
        return store.Writes - before;
    }

    private static HashSet<int> CommittedPartitions(
        IReadOnlyList<string> written, long generation, DurableVectorIndexOptions options)
    {
        var committed = new HashSet<int>();
        for (var partition = 0; partition < options.Index.PartitionCount; partition++)
        {
            if (written.Contains(VectorIndexStorageKeys.PartitionState(Prefix, generation, partition)))
            {
                committed.Add(partition);
            }
        }

        return committed;
    }

    private static bool IsPartitionKey(string key, long generation, int partition) =>
        key == VectorIndexStorageKeys.PartitionState(Prefix, generation, partition) ||
        key.StartsWith(VectorIndexStorageKeys.PartitionVectorPrefix(Prefix, generation, partition), StringComparison.Ordinal);

    private static void AssertOnlyDeleted(
        InMemoryVectorIndexStore store, DurableVectorIndex index, long superseded, long target)
    {
        var allGenerations = VectorIndexStorageKeys.AllGenerationsPrefix(Prefix);
        Assert.Multiple(() =>
        {
            Assert.That(store.KeysWritten.Where(key => key.StartsWith(allGenerations, StringComparison.Ordinal)),
                Is.Empty, "A retry after a failed delete must only delete; it wrote generation data.");
            Assert.That(store.DeletedPrefixes, Does.Contain(GenerationPrefix(superseded)));
            Assert.That(index.Generation, Is.EqualTo(target), "The retry must not start another generation.");
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(store.KeysWithPrefix(GenerationPrefix(superseded)), Is.Empty,
                "The superseded generation was orphaned.");
            Assert.That(store.KeysWithPrefix(GenerationPrefix(target + 1)), Is.Empty);
            Assert.That(store.KeysWithPrefix(GenerationPrefix(target)), Is.Not.Empty);
            Assert.That(store.KeysWithPrefix(VectorIndexStorageKeys.BuildState(Prefix)), Is.Empty);
        });
    }

    private static async Task<DurableVectorIndex> AssertReloadsWholeAsync(
        InMemoryVectorIndexStore store,
        ListVectorSource source,
        DurableVectorIndexOptions options,
        long generation,
        int count)
    {
        var reloaded = await DurableIndexHarness.OpenAsync(store, source, options);
        Assert.Multiple(() =>
        {
            Assert.That(reloaded.Generation, Is.EqualTo(generation));
            Assert.That(reloaded.Count, Is.EqualTo(count));
            Assert.That(reloaded.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(reloaded.Progress.IsReady, Is.True);
        });

        return reloaded;
    }
}
