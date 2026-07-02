using System.Text;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests.Views;

/// <summary>
/// Unit tests for the custom-fold path of <see cref="AggregationApplier"/> (the
/// <see cref="AggregationKind.Fold"/> reduce), driven against an in-memory
/// <see cref="IAggregationViewStore"/> so the re-fold-on-change convergence
/// properties are checked without a grain or cluster. The applier re-folds a
/// group over its surviving members in ascending (HLC, sourceKey) order on every
/// change, so the materialised value is a pure function of the group's member set
/// regardless of contribution delivery order.
/// </summary>
[TestFixture]
public class FoldAggregationApplierTests
{
    private sealed class InMemoryStore : IAggregationViewStore
    {
        public Dictionary<string, byte[]> Map { get; } = new(StringComparer.Ordinal);

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult(Map.TryGetValue(key, out var v) ? v : null);

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            Map[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            Map.Remove(key);
            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            foreach (var (key, value) in entries)
            {
                Map[key] = value;
            }

            return Task.CompletedTask;
        }
    }

    // A fold that concatenates each member's value (a byte tag) in HLC order,
    // making the applier's re-fold ordering directly observable in the result.
    private sealed class ConcatFold : ILatticeFoldProjection
    {
        public AggregationKind Aggregation => AggregationKind.Fold;

        public string ProjectionVersion => "concat-v1";

        public byte[] Initial() => [];

        public byte[] Apply(byte[] accumulator, string sourceKey, byte[] sourceValue, HybridLogicalClock timestamp)
        {
            var result = new byte[accumulator.Length + sourceValue.Length];
            accumulator.CopyTo(result, 0);
            sourceValue.CopyTo(result, accumulator.Length);
            return result;
        }

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) => [];
    }

    private static HybridLogicalClock Clock(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static AggregationApplier Applier(IAggregationViewStore store, ILatticeFoldProjection fold, int fanout = 1) =>
        new(store, AggregationKind.Fold, fanout, 0, "0", fold);

    private static string? Result(InMemoryStore store, string group) =>
        store.Map.TryGetValue(group, out var v) ? Encoding.UTF8.GetString(v) : null;

    private static AggregationContribution Fold(string group, string key, string value, HybridLogicalClock hlc) =>
        AggregationContribution.Fold(group, key, Encoding.UTF8.GetBytes(value), hlc);

    [Test]
    public async Task Fold_materialises_members_in_hlc_order()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        // Applied out of HLC order; the re-fold must still order by HLC.
        await applier.ApplyAsync(Fold("g", "b", "B", Clock(2)));
        await applier.ApplyAsync(Fold("g", "a", "A", Clock(1)));
        await applier.ApplyAsync(Fold("g", "c", "C", Clock(3)));

        Assert.That(Result(store, "g"), Is.EqualTo("ABC"));
    }

    [Test]
    public async Task Fold_delivery_order_does_not_change_result()
    {
        var forward = new InMemoryStore();
        var reverse = new InMemoryStore();

        var f = Applier(forward, new ConcatFold());
        await f.ApplyAsync(Fold("g", "a", "A", Clock(1)));
        await f.ApplyAsync(Fold("g", "b", "B", Clock(2)));
        await f.ApplyAsync(Fold("g", "c", "C", Clock(3)));

        var r = Applier(reverse, new ConcatFold());
        await r.ApplyAsync(Fold("g", "c", "C", Clock(3)));
        await r.ApplyAsync(Fold("g", "b", "B", Clock(2)));
        await r.ApplyAsync(Fold("g", "a", "A", Clock(1)));

        Assert.That(Result(forward, "g"), Is.EqualTo(Result(reverse, "g")));
        Assert.That(Result(forward, "g"), Is.EqualTo("ABC"));
    }

    [Test]
    public async Task Fold_equal_hlc_breaks_tie_by_source_key()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("g", "b", "B", Clock(1)));
        await applier.ApplyAsync(Fold("g", "a", "A", Clock(1)));

        Assert.That(Result(store, "g"), Is.EqualTo("AB"));
    }

    [Test]
    public async Task Fold_overwrite_refolds_with_new_value()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("g", "a", "A", Clock(1)));
        await applier.ApplyAsync(Fold("g", "b", "B", Clock(2)));
        // a overwrites its value at a later HLC; ordering shifts a after b.
        await applier.ApplyAsync(Fold("g", "a", "X", Clock(3)));

        Assert.That(Result(store, "g"), Is.EqualTo("BX"));
    }

    [Test]
    public async Task Fold_retract_refolds_over_survivors()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("g", "a", "A", Clock(1)));
        await applier.ApplyAsync(Fold("g", "b", "B", Clock(2)));
        await applier.ApplyAsync(Fold("g", "c", "C", Clock(3)));
        await applier.ApplyAsync(AggregationContribution.Retract("b", Clock(4)));

        Assert.That(Result(store, "g"), Is.EqualTo("AC"));
    }

    [Test]
    public async Task Fold_last_member_retracted_deletes_group()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("g", "a", "A", Clock(1)));
        await applier.ApplyAsync(AggregationContribution.Retract("a", Clock(2)));

        Assert.That(Result(store, "g"), Is.Null);
    }

    [Test]
    public async Task Fold_regroup_moves_member_between_groups()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("g1", "a", "A", Clock(1)));
        await applier.ApplyAsync(Fold("g2", "a", "A", Clock(2)));

        Assert.That(Result(store, "g1"), Is.Null, "g1 should be emptied");
        Assert.That(Result(store, "g2"), Is.EqualTo("A"));
    }

    [Test]
    public async Task Fold_retract_of_unknown_key_is_noop()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(AggregationContribution.Retract("ghost", Clock(1)));

        Assert.That(store.Map, Is.Empty);
    }

    [Test]
    public async Task Fold_converges_across_fanout_shards()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold(), fanout: 4);

        await applier.ApplyAsync(Fold("g", "alpha", "1", Clock(1)));
        await applier.ApplyAsync(Fold("g", "bravo", "2", Clock(2)));
        await applier.ApplyAsync(Fold("g", "charlie", "3", Clock(3)));

        Assert.That(Result(store, "g"), Is.EqualTo("123"));
    }

    [Test]
    public async Task Fold_group_keys_containing_the_row_delimiter_do_not_collide()
    {
        // Internal fold rows are keyed "\u0000f{group}\u0000{slot}". A group key
        // that itself embeds the NUL delimiter and a slot-like digit ("a\u00000")
        // could alias group "a"'s row if the layout split on anything but the
        // final NUL. It does not: the slot suffix is always NUL-free, so the last
        // NUL is an unambiguous delimiter. The two groups must stay independent.
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("a", "k1", "A", Clock(1)));
        await applier.ApplyAsync(Fold("a\u00000", "k2", "B", Clock(1)));

        Assert.Multiple(() =>
        {
            Assert.That(Result(store, "a"), Is.EqualTo("A"));
            Assert.That(Result(store, "a\u00000"), Is.EqualTo("B"));
        });
    }

    [Test]
    public async Task Fold_source_keys_containing_the_row_delimiter_stay_distinct()
    {
        // Two members of one group whose source keys differ only by an embedded
        // NUL must remain distinct entries in the group's inverse map (not collapse
        // into one), so both contribute to the re-fold.
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("g", "s", "A", Clock(1)));
        await applier.ApplyAsync(Fold("g", "s\u00000", "B", Clock(2)));

        Assert.That(Result(store, "g"), Is.EqualTo("AB"));
    }

    [Test]
    public async Task Fold_reserved_group_key_is_rejected_and_writes_nothing()
    {
        // A group-key selector that produces a reserved key (NUL-prefixed or empty)
        // would otherwise write a group value into the reserved region - invisible
        // to reads and liable to collide with an internal row. The applier drops it.
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("\u0000evil", "a", "A", Clock(1)));
        await applier.ApplyAsync(Fold(string.Empty, "b", "B", Clock(2)));

        Assert.That(store.Map, Is.Empty, "Reserved group keys must not touch the store.");
    }

    [Test]
    public async Task Fold_reserved_group_key_does_not_disturb_a_valid_group()
    {
        // A rejected contribution must not perturb existing valid state: a source
        // key that later regroups into a reserved key keeps its prior valid group.
        var store = new InMemoryStore();
        var applier = Applier(store, new ConcatFold());

        await applier.ApplyAsync(Fold("g", "a", "A", Clock(1)));
        await applier.ApplyAsync(Fold("\u0000bad", "a", "Z", Clock(2)));

        Assert.That(Result(store, "g"), Is.EqualTo("A"),
            "The valid group is unchanged when a later regroup targets a reserved key.");
    }
}
