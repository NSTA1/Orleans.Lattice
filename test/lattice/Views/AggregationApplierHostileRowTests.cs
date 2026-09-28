using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression tests for the applier's row-shape discipline on the inverse and
/// fold passes. Both passes read rows the accumulator pass already guards
/// against: a slot that a retraction flipped to the empty sentinel rather than
/// deleting, and (under <c>LatticeViewReplicationMode.ShipView</c>) a row that
/// arrived from a replication peer. Handing either straight to the codec threw
/// out of the drain loop, so each read site must screen for
/// <c>null</c> and the empty sentinel before decoding.
/// </summary>
[TestFixture]
public sealed class AggregationApplierHostileRowTests
{
    private sealed class InMemoryAggregationViewStore : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);

        public void Seed(string key, byte[] value) => _map[key] = value;

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
            => Task.FromResult(_map.TryGetValue(key, out var v) ? v : null);

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_map.TryGetValue(key, out var v))
                {
                    result[key] = v;
                }
            }

            return Task.FromResult(result);
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            _map[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            _map.Remove(key);
            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            foreach (var e in entries)
            {
                _map[e.Key] = e.Value;
            }

            return Task.CompletedTask;
        }
    }

    private static HybridLogicalClock Hlc() => HybridLogicalClock.Tick(new HybridLogicalClock());

    private static ILatticeFoldProjection Fold() =>
        new LatticeFoldProjection(_ => "g", () => [], (acc, _, _, _) => acc, "v1");

    [Test]
    public async Task Inverse_mutate_treats_an_empty_sentinel_slot_as_an_absent_map()
    {
        // A retraction that emptied the last member of a slot leaves the empty
        // sentinel behind rather than deleting the row. The next contribution to
        // that slot must start from a fresh map, not decode a one-byte row.
        var store = new InMemoryAggregationViewStore();
        store.Seed(AggregationRowCodec.InverseKey("g", 0), AggregationRowCodec.EmptyRow());
        var applier = new AggregationApplier(store, AggregationKind.Min, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 4.0, Hlc()));

        Assert.That(await store.GetAsync("g"), Is.Not.Null,
            "the contribution must land even though the slot held the empty sentinel");
    }

    [Test]
    public async Task Inverse_materialise_skips_an_empty_sentinel_slot()
    {
        // Fanout 2 so the contributed source key lands in one slot while the
        // other holds an empty sentinel. The materialise pass gathers every slot,
        // so the untouched one is handed to the decoder unless it is screened.
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Min, fanout: 2, maxGroupEntries: 0, operationEpoch: "e1");
        store.Seed(AggregationRowCodec.InverseKey("g", 0), AggregationRowCodec.EmptyRow());
        store.Seed(AggregationRowCodec.InverseKey("g", 1), AggregationRowCodec.EmptyRow());

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 4.0, Hlc()));

        Assert.That(await store.GetAsync("g"), Is.Not.Null,
            "the materialise pass must skip an empty-sentinel slot instead of decoding it");
    }

    [Test]
    public async Task SetUnion_materialise_skips_an_empty_sentinel_slot()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.SetUnion, fanout: 2, maxGroupEntries: 0, operationEpoch: "e1");
        store.Seed(AggregationRowCodec.InverseKey("g", 0), AggregationRowCodec.EmptyRow());
        store.Seed(AggregationRowCodec.InverseKey("g", 1), AggregationRowCodec.EmptyRow());

        await applier.ApplyAsync(AggregationContribution.SetMember("g", "s1", "m1", Hlc()));

        Assert.That(await store.GetAsync("g"), Is.Not.Null);
    }

    [Test]
    public async Task Fold_mutate_treats_an_empty_sentinel_slot_as_an_absent_map()
    {
        var store = new InMemoryAggregationViewStore();
        store.Seed(AggregationRowCodec.FoldInverseKey("g", 0), AggregationRowCodec.EmptyRow());
        var applier = new AggregationApplier(
            store, AggregationKind.Fold, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", fold: Fold());

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [1, 2, 3], Hlc()));

        Assert.That(await store.GetAsync("g"), Is.Not.Null,
            "the fold contribution must land even though the slot held the empty sentinel");
    }

    [Test]
    public async Task Fold_materialise_skips_an_empty_sentinel_slot()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(
            store, AggregationKind.Fold, fanout: 2, maxGroupEntries: 0, operationEpoch: "e1", fold: Fold());
        store.Seed(AggregationRowCodec.FoldInverseKey("g", 0), AggregationRowCodec.EmptyRow());
        store.Seed(AggregationRowCodec.FoldInverseKey("g", 1), AggregationRowCodec.EmptyRow());

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [1, 2, 3], Hlc()));

        Assert.That(await store.GetAsync("g"), Is.Not.Null,
            "the fold materialise pass must skip an empty-sentinel slot instead of decoding it");
    }

    [Test]
    public void Inverse_mutate_surfaces_a_malformed_row_as_a_framing_fault()
    {
        // A row that is neither absent nor the empty sentinel but cannot be
        // decoded (a truncated or hostile peer-shipped row) must raise the
        // codec's catchable framing fault, not an index-out-of-range escape.
        var store = new InMemoryAggregationViewStore();
        store.Seed(AggregationRowCodec.InverseKey("g", 0), [0x7F, 0xFF, 0xFF, 0xFF]);
        var applier = new AggregationApplier(store, AggregationKind.Min, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");

        Assert.That(
            async () => await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 4.0, Hlc())),
            Throws.InstanceOf<InvalidDataException>(),
            "a malformed inverse row must fault as bad framing rather than allocating on a wire-supplied count");
    }
}
