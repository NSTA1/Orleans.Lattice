using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Behavioural coverage for
/// <see cref="AzureTableWalStorageProvider.ReadFilteredAsync"/> and the two
/// decode helpers behind it, driven against an in-memory table.
/// <para>
/// The filtered replay read is the third of the provider's dark subsystems:
/// its entire file measured at zero line coverage under the default filter,
/// because the only fixtures exercising it are emulator-gated. These tests
/// cover both classification routes (routing-prefix and full-decode), the
/// window bounds, the trailing-excluded row rule, and the degenerate windows
/// that short-circuit before any I/O.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderFilteredReadTests
{
    private const string TreeId = "tree-filtered";
    private const int ShardIndex = 0;

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <summary>
    /// Builds a provider over an in-memory table. <paramref name="routed"/>
    /// selects the fast classification route (read the routing prefix without
    /// a full decode); without it the provider falls back to decoding every
    /// row and testing the materialised key, which is the other arm of
    /// <c>TryDecodeUnlessExcluded</c>.
    /// </summary>
    private (AzureTableWalStorageProvider Provider, InMemoryWalTable Table) CreateProvider(
        bool routed,
        LatticeCompression compression = LatticeCompression.None)
    {
        var table = new InMemoryWalTable();
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = table.BuildServiceClient(),
            TableName = "Tfiltered",
            Compression = compression,
            CompressionMinPayloadBytes = 0,
        };
        var provider = new AzureTableWalStorageProvider(
            Options.Create(options),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)],
            routing: routed
                ? new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>())
                : null);
        return (provider, table);
    }

    private static WalEntry Entry(long offset, string key) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = [(byte)offset, 0x2B],
        },
    };

    private static async Task<List<WalEntry>> ReadFilteredAsync(
        AzureTableWalStorageProvider provider,
        WalKeyFilter filter,
        long toOffsetInclusive,
        long fromOffsetExclusive = -1L,
        int maxEntries = 1024)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in provider.ReadFilteredAsync(
            TreeId, ShardIndex, fromOffsetExclusive, toOffsetInclusive, maxEntries, filter, CancellationToken.None))
        {
            collected.Add(entry);
        }

        return collected;
    }

    private async Task<AzureTableWalStorageProvider> SeededAsync(bool routed, LatticeCompression compression = LatticeCompression.None)
    {
        var (provider, _) = CreateProvider(routed, compression);
        await provider.AppendBatchAsync(
            TreeId,
            ShardIndex,
            [Entry(0, "a0"), Entry(1, "m1"), Entry(2, "b2"), Entry(3, "m3"), Entry(4, "z4")],
            CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        return provider;
    }

    [Test]
    public async Task ReadFilteredAsync_yields_only_owned_rows_with_a_routing_reader()
    {
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(provider, new WalKeyFilter("m", "n"), toOffsetInclusive: 3);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L }));
        Assert.That(read.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "m1", "m3" }));
        Assert.That(read[0].Mutation.Value.ToArray(), Is.EqualTo(new byte[] { 1, 0x2B }));
    }

    [Test]
    public async Task ReadFilteredAsync_yields_only_owned_rows_without_a_routing_reader()
    {
        // The no-routing arm decodes every row and tests the materialised key.
        // It must reach the same answer as the prefix route above; pairing the
        // two is what proves the fast path is an optimisation and not a
        // different filter.
        await using var provider = await SeededAsync(routed: false);

        var read = await ReadFilteredAsync(provider, new WalKeyFilter("m", "n"), toOffsetInclusive: 3);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L }));
        Assert.That(read.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "m1", "m3" }));
    }

    [Test]
    public async Task ReadFilteredAsync_returns_a_trailing_excluded_row_routing_only()
    {
        // The row that ends the window is returned even when excluded, so the
        // consumer can advance its cursor - but with routing fields only and
        // no payload. Asserting the null value alongside the present key is
        // what distinguishes "routing-only" from "included".
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(provider, new WalKeyFilter("m", "n"), toOffsetInclusive: 4);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L, 4L }));
        Assert.That(read[^1].Mutation.Key, Is.EqualTo("z4"));
        Assert.That(read[^1].Mutation.Value, Is.Null, "the trailing excluded row carries routing fields only");
    }

    [Test]
    public async Task ReadFilteredAsync_returns_a_trailing_excluded_row_routing_only_without_a_routing_reader()
    {
        await using var provider = await SeededAsync(routed: false);

        var read = await ReadFilteredAsync(provider, new WalKeyFilter("m", "n"), toOffsetInclusive: 4);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L, 4L }));
        Assert.That(read[^1].Mutation.Key, Is.EqualTo("z4"));
        Assert.That(read[^1].Mutation.Value, Is.Null);
    }

    [Test]
    public async Task ReadFilteredAsync_does_not_emit_a_trailing_row_when_the_window_ends_on_an_owned_row()
    {
        // The trailing-excluded slot must be cleared by a later inclusion,
        // otherwise a row excluded mid-scan would be re-emitted at the end.
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(provider, new WalKeyFilter("m", "n"), toOffsetInclusive: 3);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L }));
        Assert.That(read.Select(e => e.Mutation.Value), Has.All.Not.Null);
    }

    [Test]
    public async Task ReadFilteredAsync_with_an_unbounded_filter_returns_every_row()
    {
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(provider, default, toOffsetInclusive: 4);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
        Assert.That(read.Select(e => e.Mutation.Value), Has.All.Not.Null);
    }

    [Test]
    public async Task ReadFilteredAsync_honours_the_exclusive_lower_bound()
    {
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(provider, default, toOffsetInclusive: 4, fromOffsetExclusive: 2L);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 3L, 4L }));
    }

    [Test]
    public async Task ReadFilteredAsync_honours_the_inclusive_upper_bound()
    {
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(provider, default, toOffsetInclusive: 2);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReadFilteredAsync_stops_at_maxEntries()
    {
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(provider, default, toOffsetInclusive: 4, maxEntries: 2);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public async Task ReadFilteredAsync_stops_scanning_manifest_rows_past_the_window()
    {
        // A batch that starts beyond the window ends the manifest walk, which
        // is a different break from the per-row upper bound above.
        var (provider, _) = CreateProvider(routed: true);
        await using var _p = provider;

        await provider.AppendBatchAsync(
            TreeId, ShardIndex, [Entry(0, "a0"), Entry(1, "m1")], CancellationToken.None);
        await provider.AppendBatchAsync(
            TreeId, ShardIndex, [Entry(2, "m2"), Entry(3, "m3")], CancellationToken.None);
        await provider.AppendBatchAsync(
            TreeId, ShardIndex, [Entry(4, "m4"), Entry(5, "m5")], CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await ReadFilteredAsync(provider, default, toOffsetInclusive: 3);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task ReadFilteredAsync_yields_nothing_for_an_empty_window()
    {
        await using var provider = await SeededAsync(routed: true);

        Assert.Multiple(async () =>
        {
            Assert.That(
                await ReadFilteredAsync(provider, default, toOffsetInclusive: 2, fromOffsetExclusive: 2L),
                Is.Empty,
                "an upper bound equal to the exclusive lower bound is an empty window");
            Assert.That(
                await ReadFilteredAsync(provider, default, toOffsetInclusive: 1, fromOffsetExclusive: 3L),
                Is.Empty,
                "an inverted window yields nothing");
        });
    }

    [Test]
    public async Task ReadFilteredAsync_yields_nothing_when_the_lower_bound_saturates()
    {
        // long.MaxValue has no successor, so the window cannot contain a row
        // and the method must short-circuit rather than overflow.
        await using var provider = await SeededAsync(routed: true);

        var read = await ReadFilteredAsync(
            provider, default, toOffsetInclusive: long.MaxValue, fromOffsetExclusive: long.MaxValue);

        Assert.That(read, Is.Empty);
    }

    [Test]
    public async Task ReadFilteredAsync_yields_nothing_for_an_absent_shard()
    {
        var (provider, _) = CreateProvider(routed: true);
        await using var _p = provider;

        var collected = new List<WalEntry>();
        await foreach (var entry in provider.ReadFilteredAsync(
            "absent", 9, -1L, 100L, 10, default, CancellationToken.None))
        {
            collected.Add(entry);
        }

        Assert.That(collected, Is.Empty);
    }

    [Test]
    public async Task ReadFilteredAsync_classifies_compressed_rows_from_the_inflated_prefix()
    {
        // Compression changes where the routing prefix is read from and
        // nothing about the verdict, so the answer must match the
        // uncompressed case exactly.
        await using var compressed = await SeededAsync(routed: true, compression: LatticeCompression.Zstd);

        var read = await ReadFilteredAsync(compressed, new WalKeyFilter("m", "n"), toOffsetInclusive: 4);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L, 4L }));
        Assert.That(read[0].Mutation.Value.ToArray(), Is.EqualTo(new byte[] { 1, 0x2B }));
        Assert.That(read[^1].Mutation.Value, Is.Null);
    }

    [Test]
    public async Task ReadFilteredAsync_classifies_compressed_rows_without_a_routing_reader()
    {
        await using var compressed = await SeededAsync(routed: false, compression: LatticeCompression.Zstd);

        var read = await ReadFilteredAsync(compressed, new WalKeyFilter("m", "n"), toOffsetInclusive: 4);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L, 4L }));
        Assert.That(read[^1].Mutation.Value, Is.Null);
    }

    [Test]
    public async Task ReadFilteredAsync_agrees_with_and_without_a_routing_reader_on_a_shard_constrained_filter()
    {
        // A filter constrained on both the key range and the shard axis, over
        // keys that need a real UTF-8 decode to compare. The two classification
        // routes must be indistinguishable.
        var entries = new[]
        {
            Entry(0, "a0"), Entry(1, "m1"), Entry(2, "\u00FCber"), Entry(3, "m\uD83D\uDE00"),
            Entry(4, "m4"), Entry(5, "b5"), Entry(6, "m6"), Entry(7, "m7"),
        };

        var (routed, _) = CreateProvider(routed: true);
        await using var _r = routed;
        await routed.AppendBatchAsync(TreeId, ShardIndex, entries, CancellationToken.None);
        await routed.FlushPhaseTwoAsync(CancellationToken.None);

        var (plain, _) = CreateProvider(routed: false);
        await using var _pl = plain;
        await plain.AppendBatchAsync(TreeId, ShardIndex, entries, CancellationToken.None);
        await plain.FlushPhaseTwoAsync(CancellationToken.None);

        var filter = new WalKeyFilter("m", null, ShardMap.CreateDefault(64, 2), 1);

        var fast = await ReadFilteredAsync(routed, filter, toOffsetInclusive: 7);
        var slow = await ReadFilteredAsync(plain, filter, toOffsetInclusive: 7);

        static string Render(WalEntry e) =>
            $"{e.Offset}:{e.Mutation.Kind}:{e.Mutation.Key}:{Convert.ToHexString(e.Mutation.Value ?? [])}";

        var owned = entries.Select(e => e.Mutation.Key).Where(filter.Owns).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(fast.Select(Render), Is.EqualTo(slow.Select(Render)));
            Assert.That(owned, Is.Not.Empty, "a filter owning nothing would make the comparison vacuous");
            Assert.That(
                owned,
                Has.Length.LessThan(entries.Length),
                "a filter owning everything would not exercise the exclusion arms");
        });
    }

    [Test]
    public void ReadFilteredAsync_rejects_a_maxEntries_below_one()
    {
        var (provider, _) = CreateProvider(routed: true);

        Assert.That(
            async () => await ReadFilteredAsync(provider, default, toOffsetInclusive: 4, maxEntries: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReadFilteredAsync_rejects_a_null_treeId()
    {
        var (provider, _) = CreateProvider(routed: true);

        Assert.That(
            async () =>
            {
                await foreach (var _ in provider.ReadFilteredAsync(
                    null!, 0, -1L, 10L, 10, default, CancellationToken.None))
                {
                    // The guard throws before the first yield.
                }
            },
            Throws.InstanceOf<ArgumentNullException>());
    }
}
