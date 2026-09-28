using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Covers the batched key-map write path.
/// <para>
/// The build previously issued one single-key store write per vector, which on a
/// corpus-sized build is one durable write - and one write-ahead-log append - per
/// vector. These assert that the batched path writes once per flush instead,
/// without weakening the two properties the mapping depends on: that a key is
/// resolvable the moment it is assigned, and that a failed flush loses nothing.
/// </para>
/// </summary>
[TestFixture]
public class VectorKeyDictionaryBatchingTests
{
    private const string Prefix = "vidx/";

    // Large enough that one reservation covers every identifier these tests
    // assign, so the only writes are the single reservation and the flushes.
    private const int ReservationBlock = 1024;

    [Test]
    public async Task Buffered_assignment_writes_nothing_until_it_is_flushed()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        await keys.GetOrAddBufferedAsync("a");

        // The reservation is deliberately NOT buffered - the watermark must be
        // durable before any identifier in its block is handed out - so exactly
        // one write has happened at this point, and it is not a key-map record.
        var afterReservation = store.Writes;

        await keys.GetOrAddBufferedAsync("b");
        await keys.GetOrAddBufferedAsync("c");

        Assert.Multiple(() =>
        {
            Assert.That(store.Writes, Is.EqualTo(afterReservation), "buffered assignments must not write");
            Assert.That(keys.PendingWriteCount, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task A_flush_writes_every_buffered_record_in_one_batch()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        for (var i = 0; i < 50; i++)
        {
            await keys.GetOrAddBufferedAsync($"id-{i}");
        }

        var before = store.Writes;
        await keys.FlushPendingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Writes - before, Is.EqualTo(1), "a flush is one store write, not one per record");
            Assert.That(store.LargestBatchEntries, Is.EqualTo(50));
            Assert.That(keys.PendingWriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_flush_with_nothing_buffered_does_not_write()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        var before = store.Writes;
        await keys.FlushPendingAsync();

        Assert.That(store.Writes, Is.EqualTo(before));
    }

    [Test]
    public async Task A_buffered_key_is_resolvable_before_it_is_durable()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        var assigned = await keys.GetOrAddBufferedAsync("a");

        Assert.Multiple(() =>
        {
            Assert.That(keys.TryGetKey("a", out var found), Is.True);
            Assert.That(found, Is.EqualTo(assigned));
            Assert.That(keys.TryGetId(assigned, out var id), Is.True);
            Assert.That(id, Is.EqualTo("a"));
        });
    }

    [Test]
    public async Task Re_assigning_a_buffered_identifier_returns_the_same_key_and_buffers_once()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        var first = await keys.GetOrAddBufferedAsync("a");
        var second = await keys.GetOrAddBufferedAsync("a");

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.EqualTo(first));
            Assert.That(keys.PendingWriteCount, Is.EqualTo(1), "the record must not be buffered twice");
        });
    }

    [Test]
    public async Task A_flushed_assignment_survives_into_a_new_dictionary()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        var assigned = await keys.GetOrAddBufferedAsync("a");
        await keys.GetOrAddBufferedAsync("b");
        await keys.FlushPendingAsync();

        var reopened = new VectorKeyDictionary(store, Prefix, ReservationBlock);
        await reopened.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reopened.TryGetKey("a", out var found), Is.True);
            Assert.That(found, Is.EqualTo(assigned));
            Assert.That(reopened.TryGetKey("b", out _), Is.True);
        });
    }

    [Test]
    public async Task A_failed_flush_keeps_the_buffer_so_a_retry_loses_nothing()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        await keys.GetOrAddBufferedAsync("a");
        await keys.GetOrAddBufferedAsync("b");

        // The next write - the flush - is refused.
        store.FailAfterWrites = 0;
        Assert.That(async () => await keys.FlushPendingAsync(), Throws.Exception);

        // The buffer must survive. The in-memory maps have already adopted these
        // identifiers, so GetOrAddBufferedAsync will not re-buffer them: dropping
        // the buffer here would lose the records permanently while the mapping
        // went on claiming they existed.
        Assert.That(keys.PendingWriteCount, Is.EqualTo(2));

        store.FailAfterWrites = -1;
        await keys.FlushPendingAsync();

        var reopened = new VectorKeyDictionary(store, Prefix, ReservationBlock);
        await reopened.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(keys.PendingWriteCount, Is.Zero);
            Assert.That(reopened.TryGetKey("a", out _), Is.True);
            Assert.That(reopened.TryGetKey("b", out _), Is.True);
        });
    }

    [Test]
    public async Task A_fresh_load_discards_a_stale_buffer()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = new VectorKeyDictionary(store, Prefix, ReservationBlock);

        await keys.GetOrAddBufferedAsync("a");
        Assert.That(keys.PendingWriteCount, Is.EqualTo(1));

        // The load replaces the mapping these records were assigned against, so
        // carrying them would later write records for a state this instance no
        // longer holds.
        await keys.LoadAsync();

        Assert.That(keys.PendingWriteCount, Is.Zero);
    }

    [Test]
    public async Task Buffered_and_unbuffered_assignment_agree_on_the_key_sequence()
    {
        var buffered = new VectorKeyDictionary(new InMemoryVectorIndexStore(), Prefix, ReservationBlock);
        var unbuffered = new VectorKeyDictionary(new InMemoryVectorIndexStore(), Prefix, ReservationBlock);

        for (var i = 0; i < 20; i++)
        {
            var a = await buffered.GetOrAddBufferedAsync($"id-{i}");
            var b = await unbuffered.GetOrAddAsync($"id-{i}");
            Assert.That(a, Is.EqualTo(b), $"the two paths diverged at id-{i}");
        }

        Assert.That(buffered.NextKey, Is.EqualTo(unbuffered.NextKey));
    }

    [Test]
    public async Task The_batched_path_issues_one_write_per_flush_where_the_unbatched_path_issues_one_per_vector()
    {
        // A DIRECT A/B OF THE MECHANISM, run against both code paths in one
        // fixture. This is deliberately a WRITE COUNT and not a timing: a
        // container A/B over a fresh volume is embedder-bound (the corpus has to
        // be embedded before there is anything for the build to take in), so it
        // cannot resolve this change at all. The property that actually changed
        // is how many durable writes a build issues, and that is exact,
        // deterministic, and free of a clock.
        const int Ids = 500;

        var unbatchedStore = new InMemoryVectorIndexStore();
        var unbatched = new VectorKeyDictionary(unbatchedStore, Prefix, ReservationBlock);
        for (var i = 0; i < Ids; i++)
        {
            await unbatched.GetOrAddAsync($"id-{i}");
        }

        var batchedStore = new InMemoryVectorIndexStore();
        var batched = new VectorKeyDictionary(batchedStore, Prefix, ReservationBlock);
        for (var i = 0; i < Ids; i++)
        {
            await batched.GetOrAddBufferedAsync($"id-{i}");
        }

        await batched.FlushPendingAsync();

        // Both paths reserve identically, so the reservation writes cancel out
        // and the difference is entirely key-map records.
        var reservations = (int)Math.Ceiling(Ids / (double)ReservationBlock);

        Assert.Multiple(() =>
        {
            Assert.That(unbatchedStore.Writes, Is.EqualTo(Ids + reservations),
                "the unbatched path issues one durable write per new identifier");
            Assert.That(batchedStore.Writes, Is.EqualTo(1 + reservations),
                "the batched path issues one durable write per flush");
            Assert.That(batchedStore.LargestBatchEntries, Is.EqualTo(Ids));
        });

        TestContext.Out.WriteLine(
            $"writes: unbatched={unbatchedStore.Writes} batched={batchedStore.Writes} " +
            $"for {Ids} identifiers (reservations={reservations})");
    }

    [Test]
    public void GetOrAddBufferedAsync_rejects_a_null_or_empty_identifier()
    {
        var keys = new VectorKeyDictionary(new InMemoryVectorIndexStore(), Prefix, ReservationBlock);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await keys.GetOrAddBufferedAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await keys.GetOrAddBufferedAsync(string.Empty), Throws.ArgumentException);
        });
    }
}
