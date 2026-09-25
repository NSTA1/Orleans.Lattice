using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="WalFilteredRead"/>, the rule every layer of the
/// filtered replay read follows (issue #3565): excluded records are dropped,
/// except that the last record examined is always delivered - routing-only when
/// excluded - so a reader that advances by the last offset it receives still
/// passes everything the filter dropped, and the examined count, not the
/// delivered count, bounds the work.
/// </summary>
[TestFixture]
public sealed class WalFilteredReadTests
{
    private static readonly WalKeyFilter OwnsM = new("m", "n");

    private static WalEntry Entry(long offset, string key, MutationKind kind = MutationKind.Set) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = kind,
            Key = key,
            Value = [(byte)offset],
            Timestamp = new HybridLogicalClock { WallClockTicks = 100 + offset },
            OriginClusterId = "site-a",
        },
    };

    private static async IAsyncEnumerable<WalEntry> Stream(params WalEntry[] entries)
    {
        foreach (var entry in entries)
        {
            await Task.Yield();
            yield return entry;
        }
    }

    private static async Task<List<WalEntry>> DrainAsync(IAsyncEnumerable<WalEntry> source)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in source)
        {
            collected.Add(entry);
        }

        return collected;
    }

    [Test]
    public async Task ApplyAsync_delivers_owned_entries_and_the_trailing_excluded_entry_routing_only()
    {
        var read = await DrainAsync(WalFilteredRead.ApplyAsync(
            Stream(Entry(0, "a"), Entry(1, "m1"), Entry(2, "b"), Entry(3, "z")),
            toOffsetInclusive: 10,
            maxExamined: 10,
            OwnsM));

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L }),
                "Offsets 0 and 2 are dropped; offset 3 is the last examined entry and must still arrive.");
            Assert.That(read[0].Mutation.Value, Is.EqualTo(new byte[] { 1 }), "An owned entry arrives in full.");
            Assert.That(read[1].Mutation.Key, Is.EqualTo("z"));
            Assert.That(read[1].Mutation.Kind, Is.EqualTo(MutationKind.Set));
            Assert.That(read[1].Mutation.Value, Is.Null, "The trailing excluded entry is routing-only.");
            Assert.That(read[1].Mutation.Timestamp, Is.EqualTo(default(HybridLogicalClock)));
            Assert.That(read[1].Mutation.OriginClusterId, Is.Null);
        });
    }

    [Test]
    public async Task ApplyAsync_delivers_nothing_extra_when_the_last_examined_entry_is_owned()
    {
        var read = await DrainAsync(WalFilteredRead.ApplyAsync(
            Stream(Entry(0, "a"), Entry(1, "b"), Entry(2, "m2")),
            toOffsetInclusive: 10,
            maxExamined: 10,
            OwnsM));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L }));
    }

    [Test]
    public async Task ApplyAsync_counts_excluded_entries_against_the_examined_bound()
    {
        // Three examined, all excluded: the read stops there and reports how far
        // it got through the routing-only entry, rather than reading on until it
        // has found something to keep.
        var read = await DrainAsync(WalFilteredRead.ApplyAsync(
            Stream(Entry(0, "a"), Entry(1, "b"), Entry(2, "c"), Entry(3, "m3")),
            toOffsetInclusive: 10,
            maxExamined: 3,
            OwnsM));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L }));
    }

    [Test]
    public async Task ApplyAsync_stops_at_the_inclusive_upper_bound()
    {
        var read = await DrainAsync(WalFilteredRead.ApplyAsync(
            Stream(Entry(0, "m0"), Entry(1, "a"), Entry(2, "m2"), Entry(3, "m3")),
            toOffsetInclusive: 1,
            maxExamined: 10,
            OwnsM));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }),
            "Offset 1 is the last entry inside the window, so it arrives routing-only; nothing past it is examined.");
    }

    [Test]
    public async Task ApplyAsync_with_an_unbounded_filter_is_the_identity_over_the_window()
    {
        var entries = new[] { Entry(0, "a"), Entry(1, "m1"), Entry(2, "z") };

        var read = await DrainAsync(WalFilteredRead.ApplyAsync(Stream(entries), 10, 10, default));

        Assert.That(read, Is.EqualTo(entries));
    }

    [Test]
    public async Task ApplyAsync_of_an_empty_window_is_empty()
    {
        var read = await DrainAsync(WalFilteredRead.ApplyAsync(Stream(), 10, 10, OwnsM));

        Assert.That(read, Is.Empty, "An empty result must still mean the window held no entry.");
    }

    [Test]
    public async Task ApplyAsync_never_drops_range_deletes_or_saga_terminals()
    {
        var read = await DrainAsync(WalFilteredRead.ApplyAsync(
            Stream(
                Entry(0, "a", MutationKind.DeleteRange),
                Entry(1, "3", MutationKind.TxCommit),
                Entry(2, "3", MutationKind.TxAbort),
                Entry(3, "b")),
            toOffsetInclusive: 10,
            maxExamined: 10,
            OwnsM));

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
            Assert.That(read.Take(3).All(e => e.Mutation.Value is not null), Is.True,
                "Kinds whose ownership is not one key's arrive in full.");
        });
    }

    [Test]
    public async Task ApplyAsync_over_commit_log_tuples_follows_the_same_rule()
    {
        static async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> Tuples(params WalEntry[] entries)
        {
            foreach (var entry in entries)
            {
                await Task.Yield();
                yield return (entry.Offset, entry.Mutation);
            }
        }

        var read = new List<(long Offset, LatticeMutation Mutation)>();
        await foreach (var entry in WalFilteredRead.ApplyAsync(
            Tuples(Entry(0, "a"), Entry(1, "m1"), Entry(2, "b"), Entry(3, "c"), Entry(4, "m4")),
            toOffsetInclusive: 10,
            maxExamined: 4,
            OwnsM))
        {
            read.Add(entry);
        }

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L }));
            Assert.That(read[1].Mutation.Value, Is.Null, "The trailing excluded tuple is routing-only.");
        });
    }

    [Test]
    public void ApplyAsync_rejects_an_examined_bound_below_one()
    {
        Assert.That(
            async () => await DrainAsync(WalFilteredRead.ApplyAsync(Stream(), 10, 0, OwnsM)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void RoutingOnly_keeps_only_the_tree_kind_and_key()
    {
        var mutation = Entry(7, "k", MutationKind.Delete).Mutation;
        var record = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Tombstone,
            Key = "k",
            Value = [1],
            OriginClusterId = "site-a",
        };

        var routingMutation = WalFilteredRead.RoutingOnly(in mutation);
        var routingRecord = WalFilteredRead.RoutingOnly(in record);

        Assert.Multiple(() =>
        {
            Assert.That(routingMutation, Is.EqualTo(new LatticeMutation { TreeId = "tree", Kind = MutationKind.Delete, Key = "k" }));
            Assert.That(routingRecord.Op, Is.EqualTo(MutationKind.Tombstone));
            Assert.That(routingRecord.Key, Is.EqualTo("k"));
            Assert.That(routingRecord.TreeId, Is.EqualTo("tree"));
            Assert.That(routingRecord.Value, Is.Null);
            Assert.That(routingRecord.OriginClusterId, Is.Null);
        });
    }
}
