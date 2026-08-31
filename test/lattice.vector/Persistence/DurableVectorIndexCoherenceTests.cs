using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The first rule of the coherence contract, tested from every direction a
/// deletion can be interrupted: a retired vector never appears in a result.
/// <para>
/// The dangerous case is not the tidy one. It is the crash <i>between</i> the
/// removal being applied in memory and the cells that hold it being rewritten,
/// where the durable index still contains the vector and only the journal knows
/// it should not.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexCoherenceTests
{
    private const int Corpus = 500;
    private const string Retired = "doc-000042";

    private static bool Contains(DurableVectorIndex index, string id, float[] query)
    {
        var results = new VectorSearchResult[Corpus];
        var found = index.Search(query, results, out _);
        for (var i = 0; i < found; i++)
        {
            if (index.TryGetId(results[i].Key, out var found_) && found_ == id)
            {
                return true;
            }
        }

        return false;
    }

    [Test]
    public async Task A_retired_vector_never_appears_again()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[Retired];

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        Assert.That(Contains(index, Retired, query), Is.True, "The fixture must start from a findable vector.");

        Assert.That(await index.RemoveAsync(Retired), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(Contains(index, Retired, query), Is.False);
            Assert.That(index.TryGetKey(Retired, out _), Is.False);
            Assert.That(index.Count, Is.EqualTo(Corpus - 1));
        });
    }

    [Test]
    public async Task A_retired_vector_stays_gone_across_a_restart_that_interrupted_the_deletion()
    {
        // No flush after the removal: the persisted cells still carry the vector
        // and only the retirement journal knows better. This is the exact window
        // a crash would land in.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[Retired];

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        await index.RemoveAsync(Retired);

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(Contains(reopened, Retired, query), Is.False,
                "The journal must complete the deletion the crash interrupted.");
            Assert.That(reopened.TryGetKey(Retired, out _), Is.False,
                "The identifier mapping goes with the vector, so no key is left dangling.");
            Assert.That(reopened.Count, Is.EqualTo(Corpus - 1));
        });
    }

    [Test]
    public async Task A_retired_vector_stays_gone_across_a_restart_that_interrupted_the_flush()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[Retired];

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        await index.RemoveAsync(Retired);

        store.FailAfterWrites = store.Writes + 1;
        Assert.That(async () => await index.FlushAsync(), Throws.TypeOf<SimulatedStoreFailureException>());
        store.FailAfterWrites = -1;

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(Contains(reopened, Retired, query), Is.False,
            "A flush that only half landed must not resurrect the vector it was retiring.");
    }

    [Test]
    public async Task A_retired_vector_stays_gone_after_the_journal_is_swept()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[Retired];

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        await index.RemoveAsync(Retired);
        await index.FlushAsync();

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(Contains(reopened, Retired, query), Is.False);
            Assert.That(
                store.KeysWithPrefix(VectorIndexStorageKeys.RetirementPrefix(options.KeyPrefix)), Is.Empty,
                "Once the removal is durable the journal entry has done its job and is reclaimed.");
        });
    }

    [Test]
    public async Task Many_retirements_survive_a_restart_together()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var retired = new List<string>();
        for (var i = 0; i < Corpus; i += 7)
        {
            var id = DurableIndexHarness.Id(i);
            await index.RemoveAsync(id);
            retired.Add(id);
        }

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(reopened.Count, Is.EqualTo(Corpus - retired.Count));
        foreach (var id in retired)
        {
            Assert.That(Contains(reopened, id, source[id]), Is.False, $"'{id}' came back after the restart.");
        }
    }

    [Test]
    public async Task Retiring_an_unknown_identifier_is_a_no_op()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(100);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        Assert.That(await index.RemoveAsync("never-indexed"), Is.False);
        Assert.That(index.Count, Is.EqualTo(100));
    }

    [Test]
    public async Task Retiring_twice_is_idempotent()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(100);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        Assert.That(await index.RemoveAsync(DurableIndexHarness.Id(3)), Is.True);
        Assert.That(await index.RemoveAsync(DurableIndexHarness.Id(3)), Is.False);
        Assert.That(index.Count, Is.EqualTo(99));
    }

    [Test]
    public async Task Reconciling_removes_vectors_the_source_no_longer_holds()
    {
        // The case the journal cannot cover: something deleted from the store of
        // record without ever telling the index. The sweep settles it in the
        // source's favour, which is the only direction the contract allows.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var vanished = new[] { DurableIndexHarness.Id(1), DurableIndexHarness.Id(2), DurableIndexHarness.Id(3) };
        var queries = vanished.ToDictionary(id => id, id => source[id]);
        foreach (var id in vanished)
        {
            source.Remove(id);
        }

        var removed = await index.ReconcileAsync();
        await index.FlushAsync();
        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.EqualTo(3));
            Assert.That(index.Count, Is.EqualTo(Corpus - 3));
            Assert.That(reopened.Count, Is.EqualTo(Corpus - 3));
        });

        foreach (var id in vanished)
        {
            Assert.That(Contains(reopened, id, queries[id]), Is.False);
        }
    }

    [Test]
    public async Task Reconciling_a_coherent_index_removes_nothing()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        Assert.That(await index.ReconcileAsync(), Is.Zero);
        Assert.That(index.Count, Is.EqualTo(200));
    }

    [Test]
    public async Task A_retired_identifier_that_is_re_added_is_findable_again()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);
        var options = DurableIndexHarness.Options();
        var vector = source[Retired[..4] + "000005"];

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        await index.RemoveAsync(DurableIndexHarness.Id(5));
        await index.UpsertAsync(DurableIndexHarness.Id(5), vector);
        await index.FlushAsync();

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(200));
            Assert.That(Contains(reopened, DurableIndexHarness.Id(5), vector), Is.True,
                "A re-added identifier must not be suppressed by the tombstone that retired the old one.");
        });
    }

    [Test]
    public async Task A_lazily_loaded_index_never_serves_a_retired_vector_from_a_cell_it_fetches_later()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[Retired];

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        await index.RemoveAsync(Retired);

        // No flush: the retired vector is still in the durable cell that a lazy
        // reader will fetch on demand.
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);
        var results = new VectorSearchResult[20];
        var outcome = await lazy.SearchAsync(query, results);

        for (var i = 0; i < outcome.Count; i++)
        {
            Assert.That(lazy.TryGetId(results[i].Key, out var id) ? id : null, Is.Not.EqualTo(Retired),
                "The journal must be replayed against every cell as it arrives, not only at load.");
        }
    }

    [Test]
    public async Task A_lazily_loaded_index_refuses_to_be_mutated()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);
        var options = DurableIndexHarness.Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await lazy.RemoveAsync(DurableIndexHarness.Id(1)),
                Throws.InvalidOperationException);
            Assert.That(async () => await lazy.UpsertAsync(DurableIndexHarness.Id(1), source[DurableIndexHarness.Id(1)]),
                Throws.InvalidOperationException);
            Assert.That(async () => await lazy.FlushAsync(), Throws.InvalidOperationException);
            Assert.That(async () => await lazy.BuildStepAsync(), Throws.InvalidOperationException);
            Assert.That(async () => await lazy.ReconcileAsync(), Throws.InvalidOperationException);
            Assert.That(async () => await lazy.RebuildAsync(), Throws.InvalidOperationException);
        });
    }

    [Test]
    public void Mutating_refuses_a_null_identifier()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () =>
            {
                var index = await DurableIndexHarness.BuiltAsync(
                    new InMemoryVectorIndexStore(),
                    DurableIndexHarness.Source(20),
                    DurableIndexHarness.Options());
                await index.RemoveAsync(null!);
            }, Throws.ArgumentNullException);

            Assert.That(async () =>
            {
                var index = await DurableIndexHarness.BuiltAsync(
                    new InMemoryVectorIndexStore(),
                    DurableIndexHarness.Source(20),
                    DurableIndexHarness.Options());
                await index.UpsertAsync(null!, new float[DurableIndexHarness.Dimensions]);
            }, Throws.ArgumentNullException);
        });
    }
}
