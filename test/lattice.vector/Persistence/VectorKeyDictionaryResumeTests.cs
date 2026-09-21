using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The cross-call resume on the identifier key-map load.
/// <para>
/// <b>What makes these tests non-vacuous.</b> A resumed load and a restarted one
/// end in the <i>identical</i> final mapping - same identifiers, same keys, same
/// watermark - so any assertion made only about the finished dictionary passes
/// whether the resume works or not. Each test here therefore asserts on something
/// that differs between the two: how many records the store was asked to serve,
/// and how many the dictionary still held while the load was interrupted. Both go
/// red if the resume is removed, and the specific red value is stated in each
/// assertion message so a failure names the regression rather than a number.
/// </para>
/// </summary>
[TestFixture]
public sealed class VectorKeyDictionaryResumeTests
{
    private const string Prefix = "keys/";
    private const int Mappings = 50;
    private const int FaultAfter = 20;

    [Test]
    public async Task An_interrupted_load_resumes_instead_of_reissuing_the_records_it_already_read()
    {
        var store = new FaultingScanStore(Prefix, Mappings, FaultAfter);
        var keys = new VectorKeyDictionary(store, Prefix, 8);

        Assert.That(
            async () => await keys.LoadAsync(),
            Throws.TypeOf<TimeoutException>(),
            "The store was rigged to fault partway, so the first load must not complete. If this passes, the "
            + "fixture is not exercising the fault path at all and every assertion below is vacuous.");

        // The load banked what it read. Asserted BEFORE the second attempt, because
        // afterwards a completed load clears the cursor and the evidence is gone.
        Assert.Multiple(() =>
        {
            Assert.That(keys.HasBankedLoadProgress, Is.True,
                "The interrupted load banked nothing, so the next attempt will restart from the beginning. "
                + "This is the #2953 amplification exactly.");
            Assert.That(keys.Count, Is.EqualTo(FaultAfter),
                $"The interrupted load discarded the {FaultAfter} mappings it had already decoded.");
            Assert.That(store.EntriesServed, Is.EqualTo(FaultAfter),
                "The first attempt read a different number of records than the fixture rigged it to.");
        });

        await keys.LoadAsync();

        Assert.Multiple(() =>
        {
            // The load-bearing assertion. Restarting would serve FaultAfter records
            // a second time, so this reads Mappings + FaultAfter = 70 the moment the
            // resume is removed.
            Assert.That(store.EntriesServed, Is.EqualTo(Mappings),
                $"The resumed load reissued records it had already read: expected {Mappings} reads across both "
                + $"attempts, and a load that restarts from the beginning instead reads "
                + $"{Mappings + FaultAfter}.");

            // Independent of the one above, and red for the opposite defect: a
            // resume that advanced the cursor but cleared the dictionary would read
            // exactly Mappings records and end holding only Mappings - FaultAfter.
            Assert.That(keys.Count, Is.EqualTo(Mappings),
                $"The resumed load lost the mappings banked by the first attempt: a resume that clears the "
                + $"dictionary ends holding {Mappings - FaultAfter} of {Mappings}.");

            Assert.That(keys.HasBankedLoadProgress, Is.False,
                "A load that completed must clear its resume cursor, so a later deliberate reload is a full "
                + "re-read and not a no-op resume off the end of the range.");
        });

        for (var i = 0; i < Mappings; i++)
        {
            Assert.That(keys.TryGetKey(FaultingScanStore.IdOf(i), out var key), Is.True,
                $"'{FaultingScanStore.IdOf(i)}' was dropped by the resumed load.");
            Assert.That(key, Is.EqualTo(i),
                $"'{FaultingScanStore.IdOf(i)}' resolved to the wrong key after a resumed load.");
        }
    }

    [Test]
    public async Task A_load_that_completed_starts_again_rather_than_resuming()
    {
        // The guard on the resume: it must continue an INTERRUPTED load and must not
        // let a caller that deliberately reloads inherit a stale partial. Without
        // this, a completed load would leave a cursor at the end of the range and
        // every later reload would read nothing and silently keep whatever it had.
        var store = new FaultingScanStore(Prefix, Mappings, faultAfter: null);
        var keys = new VectorKeyDictionary(store, Prefix, 8);

        await keys.LoadAsync();
        Assert.That(store.EntriesServed, Is.EqualTo(Mappings));

        await keys.LoadAsync();

        Assert.That(store.EntriesServed, Is.EqualTo(Mappings * 2),
            "A second deliberate load re-read nothing, so it resumed a walk that had already finished instead "
            + "of starting a fresh one.");
        Assert.That(keys.Count, Is.EqualTo(Mappings));
    }

    [Test]
    public async Task The_interface_default_resume_skips_exactly_what_the_caller_already_consumed()
    {
        // The default implementation on IVectorIndexStore is inherited by any store
        // that does not push the bound into its own range scan, so its correctness
        // is not optional even though it buys no saving. Driven through a store that
        // deliberately does NOT override the resume overload.
        // Typed as the interface deliberately: a default interface member is only
        // reachable through the interface, so binding through the concrete type
        // would silently call the two-argument prefix scan and the test would
        // exercise nothing.
        IVectorIndexStore store = new UnoptimisedStore(Prefix, Mappings);
        var seen = new List<string>();

        var boundary = FaultingScanStore.KeyOf(Prefix, 9);
        await foreach (var entry in store.ScanAsync(Prefix, boundary))
        {
            seen.Add(entry.Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(seen, Has.Count.EqualTo(Mappings - 10),
                "The default resume skipped the wrong number of records.");
            Assert.That(
                seen.All(k => string.CompareOrdinal(k, boundary) > 0), Is.True,
                "The default resume yielded a record at or below the key the caller had already consumed.");
            Assert.That(seen[0], Is.EqualTo(FaultingScanStore.KeyOf(Prefix, 10)),
                "The default resume did not restart at the successor of the consumed key.");
        });
    }

    /// <summary>
    /// A store holding a rigged key map that faults partway through the first scan
    /// and honours the resume bound, which is the shape
    /// <see cref="LatticeVectorIndexStore"/> presents in production.
    /// </summary>
    private sealed class FaultingScanStore(string prefix, int mappings, int? faultAfter) : IVectorIndexStore
    {
        private readonly SortedDictionary<string, byte[]> _records =
            Build(prefix, mappings);

        private bool _faulted;

        /// <summary>Records yielded to a caller across every scan on this store.</summary>
        public int EntriesServed { get; private set; }

        internal static string IdOf(int i) => $"id-{i:D4}";

        internal static string KeyOf(string prefix, int i) =>
            VectorIndexStorageKeys.KeyMap(prefix, IdOf(i));

        private static SortedDictionary<string, byte[]> Build(string prefix, int mappings)
        {
            var records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
            Span<byte> payload = stackalloc byte[sizeof(long)];
            for (var i = 0; i < mappings; i++)
            {
                // Framed exactly as the dictionary writes them. A bare eight-byte
                // payload decodes to nothing, which presents as an empty mapping
                // rather than as an error and would make every count below read a
                // silent zero.
                BinaryPrimitives.WriteInt64LittleEndian(payload, i);
                records[KeyOf(prefix, i)] = VectorIndexRecord.Wrap(payload);
            }

            return records;
        }

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var entry in ScanAsync(keyPrefix, null, cancellationToken).ConfigureAwait(false))
            {
                yield return entry;
            }
        }

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix,
            string? exclusiveStartKey,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var served = 0;
            foreach (var entry in _records)
            {
                if (!entry.Key.StartsWith(keyPrefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (exclusiveStartKey is not null &&
                    string.CompareOrdinal(entry.Key, exclusiveStartKey) <= 0)
                {
                    continue;
                }

                if (!_faulted && faultAfter is { } limit && served == limit)
                {
                    _faulted = true;
                    throw new TimeoutException("Rigged scan fault.");
                }

                served++;
                EntriesServed++;
                yield return entry;
                await Task.CompletedTask.ConfigureAwait(false);
            }
        }

        public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult(_records.TryGetValue(key, out var value) ? value : null);

        public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
            IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        {
            var found = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_records.TryGetValue(key, out var value))
                {
                    found[key] = value;
                }
            }

            return Task.FromResult<IReadOnlyDictionary<string, byte[]>>(found);
        }

        public Task WriteAsync(
            IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
        {
            foreach (var entry in entries)
            {
                _records[entry.Key] = entry.Value;
            }

            return Task.CompletedTask;
        }

        public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        {
            foreach (var key in keys)
            {
                _records.Remove(key);
            }

            return Task.CompletedTask;
        }

        public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
        {
            foreach (var key in _records.Keys
                         .Where(k => k.StartsWith(keyPrefix, StringComparison.Ordinal)).ToList())
            {
                _records.Remove(key);
            }

            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// A store that implements only the prefix scan, so the resume overload falls
    /// through to the interface default.
    /// </summary>
    private sealed class UnoptimisedStore(string prefix, int mappings) : IVectorIndexStore
    {
        private readonly SortedDictionary<string, byte[]> _records = BuildRecords(prefix, mappings);

        private static SortedDictionary<string, byte[]> BuildRecords(string prefix, int mappings)
        {
            var records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
            for (var i = 0; i < mappings; i++)
            {
                records[FaultingScanStore.KeyOf(prefix, i)] = new byte[8];
            }

            return records;
        }

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var entry in _records)
            {
                if (entry.Key.StartsWith(keyPrefix, StringComparison.Ordinal))
                {
                    yield return entry;
                }

                await Task.CompletedTask.ConfigureAwait(false);
            }
        }

        public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult<byte[]?>(null);

        public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
            IReadOnlyList<string> keys, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyDictionary<string, byte[]>>(
                new Dictionary<string, byte[]>(StringComparer.Ordinal));

        public Task WriteAsync(
            IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
