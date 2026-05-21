using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Contract tests for the <see cref="IWalStorageProvider.ReadEncodedAsync"/>
/// seam. The seam ships with a default-method body that decodes the
/// classic <see cref="IWalStorageProvider.ReadAsync"/> stream and
/// re-encodes each entry via the supplied
/// <see cref="IWalRecordEncoder"/>; providers that hold the encoded
/// bytes natively override the method to skip the round-trip. These
/// tests pin the default behaviour against the in-memory provider and
/// against a third-party stub provider so a future change to the
/// default body cannot silently break either consumer.
/// </summary>
[TestFixture]
public class IWalStorageProviderReadEncodedAsyncTests
{
    private const string Tree = "tree";
    private static ServiceProvider _services = null!;
    private static Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static IWalRecordEncoder CreateEncoder() => new OrleansBinaryWalRecordEncoder(_serializer);

    private static WalEntry Entry(long offset, string key = "k", byte[]? value = null) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = key,
            Value = value ?? new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    [Test]
    public async Task ReadEncodedAsync_default_fallback_returns_empty_page_for_missing_shard()
    {
        var sut = new InMemoryWalStorageProvider();

        var page = await ((IWalStorageProvider)sut).ReadEncodedAsync(
            Tree, 0, -1L, 64, CreateEncoder(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.EncodedEntries.Length, Is.EqualTo(0));
            Assert.That(page.Offsets.Length, Is.EqualTo(0));
            Assert.That(page.HighestOffsetInclusive, Is.EqualTo(-1L));
        });
    }

    [Test]
    public async Task ReadEncodedAsync_default_fallback_returns_segments_and_offsets_in_order()
    {
        var sut = new InMemoryWalStorageProvider();
        var entries = new[] { Entry(0, "a"), Entry(1, "b"), Entry(2, "c") };
        await sut.AppendBatchAsync(Tree, 0, entries, CancellationToken.None);

        var page = await ((IWalStorageProvider)sut).ReadEncodedAsync(
            Tree, 0, -1L, 64, CreateEncoder(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.EncodedEntries.Length, Is.EqualTo(3));
            Assert.That(page.Offsets.Length, Is.EqualTo(3));
            Assert.That(page.Offsets.Span.ToArray(), Is.EqualTo(new[] { 0L, 1L, 2L }));
            Assert.That(page.HighestOffsetInclusive, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task ReadEncodedAsync_default_fallback_respects_from_offset_exclusive()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var page = await ((IWalStorageProvider)sut).ReadEncodedAsync(
            Tree, 0, fromOffsetExclusive: 1L, maxEntries: 64, CreateEncoder(), CancellationToken.None);

        Assert.That(page.Offsets.Span.ToArray(), Is.EqualTo(new[] { 2L, 3L }));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(3L));
    }

    [Test]
    public async Task ReadEncodedAsync_default_fallback_respects_max_entries()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var page = await ((IWalStorageProvider)sut).ReadEncodedAsync(
            Tree, 0, -1L, maxEntries: 2, CreateEncoder(), CancellationToken.None);

        Assert.That(page.Offsets.Span.ToArray(), Is.EqualTo(new[] { 0L, 1L }));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(1L));
    }

    [Test]
    public async Task ReadEncodedAsync_default_fallback_segments_round_trip_through_decode()
    {
        var sut = new InMemoryWalStorageProvider();
        var entries = new[]
        {
            Entry(0, "alpha", new byte[] { 0xAA, 0xBB }),
            Entry(1, "beta",  new byte[] { 0xCC }),
            Entry(2, "gamma", new byte[] { 0xDD, 0xEE, 0xFF }),
        };
        await sut.AppendBatchAsync(Tree, 0, entries, CancellationToken.None);

        var encoder = CreateEncoder();
        var page = await ((IWalStorageProvider)sut).ReadEncodedAsync(
            Tree, 0, -1L, 64, encoder, CancellationToken.None);

        Assert.That(page.EncodedEntries.Length, Is.EqualTo(3));
        var segments = page.EncodedEntries.Span;
        for (var i = 0; i < segments.Length; i++)
        {
            var decoded = encoder.Decode(segments[i].AsSpan());
            Assert.Multiple(() =>
            {
                Assert.That(decoded.Key, Is.EqualTo(entries[i].Mutation.Key), $"entry[{i}].Key");
                Assert.That(decoded.Value, Is.EqualTo(entries[i].Mutation.Value), $"entry[{i}].Value");
                Assert.That(decoded.Op, Is.EqualTo(entries[i].Mutation.Kind), $"entry[{i}].Op");
                Assert.That(decoded.TreeId, Is.EqualTo(entries[i].Mutation.TreeId), $"entry[{i}].TreeId");
            });
        }
    }

    [Test]
    public async Task ReadEncodedAsync_default_fallback_1024_entries_equivalent_to_ReadAsync()
    {
        // 1024-entry round-trip equivalence: every offset visible on
        // ReadAsync is visible on ReadEncodedAsync (and vice versa),
        // and each segment decodes back to a WalRecord whose
        // converter-projected LatticeMutation field-for-field matches
        // the entry the synchronous ReadAsync yields.
        var sut = new InMemoryWalStorageProvider();
        const int N = 1024;
        var entries = new WalEntry[N];
        for (var i = 0; i < N; i++)
        {
            entries[i] = Entry(i, "k-" + i.ToString("D4"), new byte[] { (byte)(i & 0xFF), (byte)((i >> 8) & 0xFF) });
        }
        await sut.AppendBatchAsync(Tree, 0, entries, CancellationToken.None);

        var encoder = CreateEncoder();
        var classicEntries = new List<WalEntry>(N);
        await foreach (var entry in sut.ReadAsync(Tree, 0, -1L, N, CancellationToken.None))
        {
            classicEntries.Add(entry);
        }

        var page = await ((IWalStorageProvider)sut).ReadEncodedAsync(Tree, 0, -1L, N, encoder, CancellationToken.None);

        Assert.That(page.EncodedEntries.Length, Is.EqualTo(N));
        Assert.That(page.Offsets.Length, Is.EqualTo(N));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(N - 1L));
        Assert.That(classicEntries.Count, Is.EqualTo(N));

        var segments = page.EncodedEntries.Span;
        var offsets = page.Offsets.Span;
        for (var i = 0; i < N; i++)
        {
            Assert.That(offsets[i], Is.EqualTo(classicEntries[i].Offset), $"offset mismatch at {i}");
            var decoded = encoder.Decode(segments[i].AsSpan());
            var projected = Orleans.Lattice.BPlusTree.Grains.WalRecordConverter.FromWalRecord(in decoded);
            Assert.Multiple(() =>
            {
                Assert.That(projected.Key, Is.EqualTo(classicEntries[i].Mutation.Key), $"key[{i}]");
                Assert.That(projected.Value, Is.EqualTo(classicEntries[i].Mutation.Value), $"value[{i}]");
                Assert.That(projected.Kind, Is.EqualTo(classicEntries[i].Mutation.Kind), $"kind[{i}]");
                Assert.That(projected.TreeId, Is.EqualTo(classicEntries[i].Mutation.TreeId), $"treeId[{i}]");
                Assert.That(projected.Timestamp, Is.EqualTo(classicEntries[i].Mutation.Timestamp), $"timestamp[{i}]");
            });
        }
    }

    [Test]
    public void ReadEncodedAsync_default_fallback_throws_on_null_treeId()
    {
        var sut = (IWalStorageProvider)new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.ReadEncodedAsync(null!, 0, -1L, 1, CreateEncoder(), CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReadEncodedAsync_default_fallback_throws_on_null_encoder()
    {
        var sut = (IWalStorageProvider)new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.ReadEncodedAsync(Tree, 0, -1L, 1, null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReadEncodedAsync_default_fallback_throws_on_zero_max_entries()
    {
        var sut = (IWalStorageProvider)new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.ReadEncodedAsync(Tree, 0, -1L, 0, CreateEncoder(), CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReadEncodedAsync_default_fallback_respects_pre_cancelled_token()
    {
        var sut = (IWalStorageProvider)new InMemoryWalStorageProvider();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Assert.That(
            async () => await sut.ReadEncodedAsync(Tree, 0, -1L, 1, CreateEncoder(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// Third-party-stub regression: a provider whose only override is
    /// <see cref="IWalStorageProvider.ReadAsync"/> (and the append
    /// surfaces) must continue to expose
    /// <see cref="IWalStorageProvider.ReadEncodedAsync"/> through the
    /// default fallback. This stub holds no encoded bytes - the
    /// fallback re-encodes on its behalf.
    /// </summary>
    [Test]
    public async Task ReadEncodedAsync_default_fallback_works_for_third_party_stub_provider()
    {
        var stub = new StubWalStorageProvider();
        var entries = new[] { Entry(0, "x"), Entry(1, "y"), Entry(2, "z") };
        await stub.AppendBatchAsync(Tree, 0, entries, CancellationToken.None);

        var encoder = CreateEncoder();
        var page = await ((IWalStorageProvider)stub).ReadEncodedAsync(
            Tree, 0, -1L, 64, encoder, CancellationToken.None);

        Assert.That(page.EncodedEntries.Length, Is.EqualTo(3));
        var segments = page.EncodedEntries.Span;
        for (var i = 0; i < segments.Length; i++)
        {
            var decoded = encoder.Decode(segments[i].AsSpan());
            Assert.That(decoded.Key, Is.EqualTo(entries[i].Mutation.Key), $"entry[{i}].Key");
        }
    }

    /// <summary>
    /// Minimal third-party provider implementation that only fills in
    /// the abstract members and inherits the default
    /// <see cref="IWalStorageProvider.ReadEncodedAsync"/> implementation.
    /// Used to confirm the default fallback compiles and runs against
    /// a foreign provider type.
    /// </summary>
    private sealed class StubWalStorageProvider : IWalStorageProvider
    {
        private readonly List<WalEntry> _entries = new();

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            _entries.AddRange(entries);
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId,
            int shardIndex,
            long fromOffsetExclusive,
            int maxEntries,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var yielded = 0;
            foreach (var entry in _entries)
            {
                if (entry.Offset <= fromOffsetExclusive)
                {
                    continue;
                }
                if (yielded >= maxEntries)
                {
                    yield break;
                }
                yield return entry;
                yielded++;
            }
            await Task.CompletedTask.ConfigureAwait(false);
        }

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(_entries.Count == 0 ? -1L : _entries[^1].Offset);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(_entries.Count == 0 ? -1L : _entries[0].Offset);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
        {
            _entries.RemoveAll(e => e.Offset <= throughOffsetInclusive);
            return Task.CompletedTask;
        }
    }
}
