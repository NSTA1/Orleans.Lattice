using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The load resumes across BOTH of its phases, not only inside the identifier
/// key walk.
/// <para>
/// <b>Why this is a separate property from the one
/// <c>VectorKeyDictionaryResumeTests</c> covers.</b> The dictionary resumes an
/// interrupted walk from its own cursor and clears that cursor when the walk
/// finishes. So an interruption landing anywhere in the restore that follows -
/// the manifest read, the centroids, the partition states, the chunks - found a
/// null cursor and re-issued the whole walk from zero. The walk is the O(corpus)
/// read in the open, so that is issue #2953's amplification exactly, merely moved
/// one phase later.
/// </para>
/// <para>
/// It became load-bearing when the open was given a wall-clock bound (#3130): a
/// bounded attempt that re-walked the corpus every time would spend its entire
/// budget redoing completed work and could never reach the end, which is the trap
/// #2953 warns a bound must not fall into. A bound is only safe on a load that
/// banks.
/// </para>
/// <para>
/// <b>The assertion that separates the two is the served count, not the result.</b>
/// A load that re-walked and one that resumed produce the identical index, so
/// every natural assertion passes under both.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexLoadPhaseResumeTests
{
    private const int Corpus = 64;
    private const string Prefix = "phase-resume/";

    private static DurableVectorIndexOptions Options() => new()
    {
        KeyPrefix = Prefix,
        MaxItemsPerChunk = 8,
        IngestBatchSize = 32,
        Index = new VectorIndexOptions
        {
            Dimensions = DurableIndexHarness.Dimensions,
            PartitionCount = 4,
            Probes = 2,
            MinimumTrainingCount = 8,
            TrainingSampleSize = 256,
        },
    };

    [Test]
    public async Task A_load_interrupted_after_the_key_walk_does_not_reissue_it()
    {
        var inner = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(inner, source, Options());

        var store = new CountingStore(inner, VectorIndexStorageKeys.KeyMapPrefix(Prefix));
        var mappings = store.CountUnderWatchedPrefix();
        Assert.That(mappings, Is.GreaterThan(0),
            "The walk has to have something to walk, or a 'read exactly once' assertion below is "
            + "satisfied vacuously by reading nothing twice.");

        store.ResetServed();
        var index = DurableVectorIndex.CreateUnloaded(store, source, Options());

        // Faults on the manifest read, which is the FIRST thing the load does after
        // the key walk completes. That places the interruption squarely in the
        // window this fixture exists for: past the cursor, before the load ends.
        store.FaultOnceOnRead(VectorIndexStorageKeys.Manifest(Prefix));

        Assert.That(
            async () => await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken),
            Throws.InstanceOf<IOException>(),
            "The armed store must actually fault. A scenario that silently completed would make every "
            + "assertion below a statement about an ordinary load.");

        var servedAfterFault = store.ServedUnderWatchedPrefix;
        Assert.Multiple(() =>
        {
            Assert.That(servedAfterFault, Is.EqualTo(mappings),
                "The walk itself completed before the fault, so the whole map was served once.");
            Assert.That(index.IsLoaded, Is.False);
            Assert.That(index.HasBankedLoadProgress, Is.True,
                "A completed walk IS banked progress even though no cursor survives it, and this is the "
                + "reading that tells an operator the resume did the most good it can do. Reporting it "
                + "as 'not resumed' would say the resume failed at exactly the moment it worked.");
        });

        await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(index.IsLoaded, Is.True);
            Assert.That(store.ServedUnderWatchedPrefix, Is.EqualTo(mappings),
                "ACROSS BOTH ATTEMPTS the key map must be served exactly once. A second attempt that "
                + "re-walks doubles this, which is the amplification #2953 measures - and under a "
                + "wall-clock bound it is unbounded rather than merely wasteful, because every attempt "
                + "would spend its budget on work already done.");
        });
    }

    [Test]
    public async Task A_resumed_load_reaches_the_same_state_as_an_uninterrupted_one()
    {
        var inner = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(inner, source, Options());

        var reference = await DurableVectorIndex.OpenAsync(inner, source, Options());

        var store = new CountingStore(inner, VectorIndexStorageKeys.KeyMapPrefix(Prefix));
        var index = DurableVectorIndex.CreateUnloaded(store, source, Options());
        store.FaultOnceOnRead(VectorIndexStorageKeys.Manifest(Prefix));

        Assert.That(
            async () => await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken),
            Throws.InstanceOf<IOException>());

        await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(index.Count, Is.EqualTo(reference.Count),
                "Skipping the completed walk must not skip its RESULT. If the resumed load kept the "
                + "flag but discarded the mapping it would open holding nothing, which is a silent "
                + "wrong-document failure rather than a slow one.");
            Assert.That(index.Progress.Phase, Is.EqualTo(reference.Progress.Phase));
            Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(reference.Progress.VectorsIndexed));
        });
    }

    [Test]
    public async Task A_completed_load_reports_no_banked_progress()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(store, source, Options());

        var index = DurableVectorIndex.CreateUnloaded(store, source, Options());
        await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(index.IsLoaded, Is.True);
            Assert.That(index.HasBankedLoadProgress, Is.False,
                "A finished load resumes nothing, because there is nothing left to continue. Without the "
                + "IsLoaded gate the remembered walk would make this read true forever, and a caller "
                + "counting resumptions would record one on every healthy open.");
        });
    }

    /// <summary>
    /// Wraps a store, counts the records served under one watched prefix, and can
    /// be armed to fault once on a nominated read.
    /// <para>
    /// It honours <c>exclusiveStartKey</c> itself rather than inheriting the
    /// interface default, because the served count IS the measurement here: a
    /// record served twice by the harness looks exactly like the defect it is
    /// meant to detect.
    /// </para>
    /// </summary>
    private sealed class CountingStore(IVectorIndexStore inner, string watchedPrefix) : IVectorIndexStore
    {
        private string? _faultKey;

        public int ServedUnderWatchedPrefix { get; private set; }

        public void ResetServed() => ServedUnderWatchedPrefix = 0;

        public void FaultOnceOnRead(string key) => _faultKey = key;

        public int CountUnderWatchedPrefix()
        {
            var count = 0;
            var enumerator = inner.ScanAsync(watchedPrefix, CancellationToken.None).GetAsyncEnumerator();
            try
            {
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    count++;
                }
            }
            finally
            {
                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }

            return count;
        }

        public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
        {
            if (_faultKey is not null && string.Equals(key, _faultKey, StringComparison.Ordinal))
            {
                _faultKey = null;
                throw new IOException("Injected read fault after the key walk.");
            }

            return inner.ReadAsync(key, cancellationToken);
        }

        public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
            IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
            => inner.ReadManyAsync(keys, cancellationToken);

        public Task WriteAsync(
            IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
            => inner.WriteAsync(entries, cancellationToken);

        public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(keys, cancellationToken);

        public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
            => inner.DeletePrefixAsync(keyPrefix, cancellationToken);

        public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix, CancellationToken cancellationToken = default)
            => ScanAsync(keyPrefix, null, cancellationToken);

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix,
            string? exclusiveStartKey,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var watched = string.Equals(keyPrefix, watchedPrefix, StringComparison.Ordinal);

            await foreach (var entry in inner.ScanAsync(keyPrefix, cancellationToken).ConfigureAwait(false))
            {
                if (exclusiveStartKey is not null
                    && string.CompareOrdinal(entry.Key, exclusiveStartKey) <= 0)
                {
                    continue;
                }

                if (watched)
                {
                    ServedUnderWatchedPrefix++;
                }

                yield return entry;
            }
        }
    }
}
