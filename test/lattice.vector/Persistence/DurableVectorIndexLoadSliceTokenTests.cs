using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The load takes two tokens, and the split is what makes bounding the open safe
/// (#3130).
/// <para>
/// <b>Only the key walk may be sliced, because only the key walk banks.</b> The
/// walk records its position per entry and resumes from it. The restore that
/// follows - manifest, centroids, partition states, chunks - builds into a local
/// and assigns only on success, so an interruption there banks NOTHING. Bounding
/// it would make every attempt restart it, and an index whose restore outlasts one
/// slice could then never open at all: not a slow open, but one that provably
/// cannot terminate. That is the trap #2953 names, and a caller that passed its
/// deadline to both tokens would fall straight into it.
/// </para>
/// <para>
/// <b>Both halves are asserted on the token, not on elapsed time.</b> A fixture
/// that waited to see whether the restore was cancelled would race the
/// propagation and need a sleep; cancelling deterministically at a known read and
/// asserting the load survived it cannot.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexLoadSliceTokenTests
{
    private const int Corpus = 64;
    private const string Prefix = "slice-token/";
    private const int WalkStopsAfter = 10;

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
    public async Task The_key_walk_token_stops_the_walk_and_banks_what_it_read()
    {
        var (store, source) = await SeededAsync();
        var index = DurableVectorIndex.CreateUnloaded(store, source, Options());
        var ct = TestContext.CurrentContext.CancellationToken;

        using var slice = new CancellationTokenSource();
        store.CancelAfterServing(WalkStopsAfter, slice);

        Assert.That(
            async () => await index.LoadOrResumeAsync(slice.Token, ct),
            Throws.InstanceOf<OperationCanceledException>(),
            "The slice token must actually reach the walk. If it did not, the bound would be inert and "
            + "the open would run for as long as it ran - the defect #3130 names.");

        Assert.Multiple(() =>
        {
            Assert.That(index.LoadedKeyCount, Is.EqualTo(WalkStopsAfter),
                "The walk banks per entry, so a sliced walk keeps exactly what it read. This count is "
                + "what lets a caller tell a slice that advanced from one that did not, which is the "
                + "only way it can distinguish a slow open from a wedged one.");
            Assert.That(index.HasBankedLoadProgress, Is.True);
            Assert.That(index.IsLoaded, Is.False);
        });
    }

    [Test]
    public async Task The_key_walk_token_does_not_cancel_the_restore()
    {
        var (store, source) = await SeededAsync();
        var index = DurableVectorIndex.CreateUnloaded(store, source, Options());
        var ct = TestContext.CurrentContext.CancellationToken;

        using var slice = new CancellationTokenSource();

        // Fires on the manifest read - the first step AFTER the walk - so the walk
        // has completed and the load is squarely inside the phase that banks
        // nothing. A restore wired to this token aborts here every time.
        store.CancelOnRead(VectorIndexStorageKeys.Manifest(Prefix), slice);

        Assert.That(
            async () => await index.LoadOrResumeAsync(slice.Token, ct),
            Throws.Nothing,
            "THE RESTORE MUST OUTLIVE THE SLICE TOKEN. It assigns only on success, so cancelling it "
            + "discards everything and the next attempt starts over - an index whose restore outlasts "
            + "one slice would never open, which is strictly worse than never bounding the open.");

        Assert.Multiple(() =>
        {
            Assert.That(slice.IsCancellationRequested, Is.True,
                "Asserted so the test cannot pass by never cancelling at all, which would make the "
                + "reading above a statement about an ordinary load.");
            Assert.That(index.IsLoaded, Is.True);
            Assert.That(index.LoadedKeyCount, Is.EqualTo(Corpus),
                "The walk ran to completion before the cancellation, so the whole mapping is held.");
        });
    }

    [Test]
    public async Task A_sliced_walk_resumes_and_reaches_the_same_state_as_an_uninterrupted_load()
    {
        var (store, source) = await SeededAsync();
        var index = DurableVectorIndex.CreateUnloaded(store, source, Options());
        var ct = TestContext.CurrentContext.CancellationToken;

        using var slice = new CancellationTokenSource();
        store.CancelAfterServing(WalkStopsAfter, slice);
        Assert.That(
            async () => await index.LoadOrResumeAsync(slice.Token, ct),
            Throws.InstanceOf<OperationCanceledException>());

        store.ResetServed();
        await index.LoadOrResumeAsync(ct, ct);

        Assert.Multiple(() =>
        {
            Assert.That(index.IsLoaded, Is.True);
            Assert.That(index.LoadedKeyCount, Is.EqualTo(Corpus));
            Assert.That(store.ServedUnderWatchedPrefix, Is.EqualTo(Corpus - WalkStopsAfter),
                "The resumed walk re-read ONLY what the slice had not reached. Re-reading from zero is "
                + "the amplification that makes a bounded open unable to converge, and it is invisible "
                + "in the finished index - so only a served count can catch it.");
        });
    }

    [Test]
    public async Task The_single_token_overload_governs_the_whole_load()
    {
        var (store, source) = await SeededAsync();
        var index = DurableVectorIndex.CreateUnloaded(store, source, Options());

        using var caller = new CancellationTokenSource();
        store.CancelOnRead(VectorIndexStorageKeys.Manifest(Prefix), caller);

        Assert.That(
            async () => await index.LoadOrResumeAsync(caller.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "The one-token overload is the CALLER's token on both phases, so a caller asking the load "
            + "to stop is obeyed in the restore too. Only the slice deadline is confined to the walk; "
            + "silently ignoring a caller there would hand back a load nobody asked to continue.");

        Assert.That(index.IsLoaded, Is.False);
    }

    private static async Task<(CancellingStore Store, IVectorSource Source)> SeededAsync()
    {
        var inner = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(inner, source, Options());

        var store = new CancellingStore(inner, VectorIndexStorageKeys.KeyMapPrefix(Prefix));
        store.ResetServed();
        return (store, source);
    }

    /// <summary>
    /// Wraps a store and cancels a nominated source at a chosen point - after N
    /// records of the watched scan, or on a nominated read - then lets the
    /// underlying operation observe it.
    /// <para>
    /// Cancelling from inside the store is what makes these fixtures
    /// deterministic: the cancellation lands at a known phase of the load rather
    /// than after a wall-clock wait that would have to be slept for.
    /// </para>
    /// <para>
    /// It honours <c>exclusiveStartKey</c> itself rather than inheriting the
    /// interface default, because the served count IS the measurement: a record
    /// served twice by the harness looks exactly like the defect it detects.
    /// </para>
    /// </summary>
    private sealed class CancellingStore(IVectorIndexStore inner, string watchedPrefix) : IVectorIndexStore
    {
        private string? _cancelReadKey;
        private CancellationTokenSource? _readTrigger;
        private CancellationTokenSource? _scanTrigger;
        private int _cancelAfter = -1;

        public int ServedUnderWatchedPrefix { get; private set; }

        public void ResetServed() => ServedUnderWatchedPrefix = 0;

        public void CancelAfterServing(int records, CancellationTokenSource trigger)
        {
            _cancelAfter = records;
            _scanTrigger = trigger;
        }

        public void CancelOnRead(string key, CancellationTokenSource trigger)
        {
            _cancelReadKey = key;
            _readTrigger = trigger;
        }

        public async Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
        {
            if (_cancelReadKey is not null && string.Equals(key, _cancelReadKey, StringComparison.Ordinal))
            {
                _cancelReadKey = null;
                await _readTrigger!.CancelAsync().ConfigureAwait(false);
            }

            return await inner.ReadAsync(key, cancellationToken).ConfigureAwait(false);
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
            var servedThisCall = 0;

            await foreach (var entry in inner.ScanAsync(keyPrefix, cancellationToken).ConfigureAwait(false))
            {
                if (exclusiveStartKey is not null
                    && string.CompareOrdinal(entry.Key, exclusiveStartKey) <= 0)
                {
                    continue;
                }

                if (watched && _scanTrigger is not null && servedThisCall == _cancelAfter)
                {
                    await _scanTrigger.CancelAsync().ConfigureAwait(false);

                    // Observed here because a real store cancels its own scan. The
                    // consumer banks per entry, so what it kept is decided by where
                    // this throws rather than by how the consumer polls.
                    cancellationToken.ThrowIfCancellationRequested();
                }

                if (watched)
                {
                    servedThisCall++;
                    ServedUnderWatchedPrefix++;
                }

                yield return entry;
            }
        }
    }
}
