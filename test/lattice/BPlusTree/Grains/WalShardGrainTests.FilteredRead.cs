using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <see cref="WalShardGrain.ReadFilteredAsync"/> (issue #3565): the WAL grain
/// applies the filtered-read rule itself, over whatever the provider returns, so
/// no excluded payload crosses its boundary even from a provider that ignores
/// the filter, and it clamps the window it lets the provider examine to the
/// durable, gap-free prefix.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly WalKeyFilter FilteredOwnsM = new("m", "n");

    private static WalEntry FilteredEntry(long offset, string key) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = [(byte)offset],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    /// <summary>
    /// Preloads the provider before the grain activates, so its durable tail
    /// already covers every entry and no poll is needed.
    /// </summary>
    private static async Task<WalShardGrain> CreatePreloadedGrainAsync(params string[] keys)
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            TreeId,
            ShardIndex,
            keys.Select((key, i) => FilteredEntry(i, key)).ToArray(),
            CancellationToken.None);
        return await CreateGrainAsync(provider);
    }

    [Test]
    public async Task ReadFilteredAsync_returns_owned_entries_and_the_last_examined_entry_routing_only()
    {
        var grain = await CreatePreloadedGrainAsync("a0", "m1", "b2", "m3", "z4");

        var page = await grain.ReadFilteredAsync(0, 10, 10, FilteredOwnsM, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L, 3L, 4L }));
            Assert.That(page.NextSequence, Is.EqualTo(5), "The page reports it scanned past every dropped entry.");
            Assert.That(page.Entries[0].Entry.Value, Is.EqualTo(new byte[] { 1 }), "An owned entry arrives in full.");
            Assert.That(page.Entries[0].Entry.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(page.Entries[2].Entry.Key, Is.EqualTo("z4"));
            Assert.That(page.Entries[2].Entry.Value, Is.Null, "The trailing excluded entry crosses the boundary routing-only.");
            Assert.That(page.Entries[2].Entry.OriginClusterId, Is.Null);
        });
    }

    [Test]
    public async Task ReadFilteredAsync_examines_no_more_than_max_entries()
    {
        var grain = await CreatePreloadedGrainAsync("a0", "b1", "m2");

        var page = await grain.ReadFilteredAsync(0, 10, 2, FilteredOwnsM, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L }));
            Assert.That(page.NextSequence, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ReadFilteredAsync_bounds_the_window_by_its_inclusive_upper_sequence()
    {
        var grain = await CreatePreloadedGrainAsync("m0", "a1", "m2");

        var page = await grain.ReadFilteredAsync(0, 1, 10, FilteredOwnsM, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 0L, 1L }));
            Assert.That(page.NextSequence, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ReadFilteredAsync_of_a_window_past_the_durable_tail_is_empty()
    {
        var grain = await CreatePreloadedGrainAsync("m0", "m1");

        var page = await grain.ReadFilteredAsync(2, 10, 10, FilteredOwnsM, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ReadFilteredAsync_reapplies_the_rule_over_a_provider_that_ignores_the_filter()
    {
        // A provider that overrides the filtered read but returns every entry in
        // full. The grain boundary, not the provider, is what guarantees that no
        // excluded payload reaches the replaying leaf.
        var entries = new[] { FilteredEntry(0, "a0"), FilteredEntry(1, "m1"), FilteredEntry(2, "b2"), FilteredEntry(3, "z3") };
        var provider = Substitute.For<IWalStorageProvider>();
        provider.GetHighestOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(3L);
        provider
            .ReadFilteredAsync(Arg.Is(TreeId), Arg.Is(ShardIndex), Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(_ => Yield(entries));
        var grain = await CreateGrainAsync(provider);

        var page = await grain.ReadFilteredAsync(0, 10, 10, FilteredOwnsM, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L, 3L }));
            Assert.That(page.Entries[1].Entry.Value, Is.Null, "The provider's full payload for z3 must not cross the boundary.");
        });
    }

    [Test]
    public async Task ReadFilteredAsync_validates_its_arguments()
    {
        var grain = await CreatePreloadedGrainAsync("m0");

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await grain.ReadFilteredAsync(-1, 10, 10, FilteredOwnsM, CancellationToken.None),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                async () => await grain.ReadFilteredAsync(0, 10, 0, FilteredOwnsM, CancellationToken.None),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    private static async IAsyncEnumerable<WalEntry> Yield(
        WalEntry[] entries,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return entry;
        }
    }
}
