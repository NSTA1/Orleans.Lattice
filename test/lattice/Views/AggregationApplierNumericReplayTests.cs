using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>Numeric retries obey the production atomic operation's immutable key-set contract.</summary>
[TestFixture]
public sealed class AggregationApplierNumericReplayTests
{
    private sealed class KeySetBoundStore : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _rows = new(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<string>> _operations = new(StringComparer.Ordinal);

        public int AtomicCalls { get; private set; }
        public int Fault { get; set; }

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.GetValueOrDefault(key));

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.Where(pair => keys.Contains(pair.Key))
                .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal));

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            FailPublicWrite(key);
            _rows[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            FailPublicWrite(key);
            _rows.Remove(key);
            return Task.CompletedTask;
        }

        private void FailPublicWrite(string key)
        {
            if (Fault == 2 && !key.StartsWith(AggregationRowCodec.ReservedPrefix, StringComparison.Ordinal))
            {
                Fault = 0;
                throw new TimeoutException("public numeric materialisation failed after the atomic flip");
            }
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId,
            CancellationToken cancellationToken = default)
        {
            AtomicCalls++;
            var keys = entries.Select(pair => pair.Key).ToHashSet(StringComparer.Ordinal);
            if (_operations.TryGetValue(operationId, out var original))
            {
                if (!original.SetEquals(keys))
                {
                    throw new LatticeIdempotencyKeyMismatchException("numeric replay changed its atomic key set", operationId);
                }

                return Task.CompletedTask;
            }

            _operations.Add(operationId, keys);
            foreach (var (key, value) in entries)
            {
                _rows[key] = value;
            }

            if (Fault == 1)
            {
                Fault = 0;
                throw new TimeoutException("numeric flip committed but its response was lost");
            }

            return Task.CompletedTask;
        }
    }

    [TestCase(AggregationKind.Count, 0)]
    [TestCase(AggregationKind.Count, 1)]
    [TestCase(AggregationKind.Count, 2)]
    [TestCase(AggregationKind.Sum, 0)]
    [TestCase(AggregationKind.Sum, 1)]
    [TestCase(AggregationKind.Sum, 2)]
    public async Task ApplyAsync_regrouped_ordinary_replay_completes_intent_without_changing_the_atomic_key_set(
        AggregationKind kind, int fault)
    {
        var store = new KeySetBoundStore();
        var applier = Create(store, kind);
        await applier.ApplyAsync(Contribution(kind, "red", 3, 10));
        var replacement = Contribution(kind, "blue", 7, 200);
        store.Fault = fault;
        if (fault == 0)
        {
            await applier.ApplyAsync(replacement);
        }
        else
        {
            Assert.ThrowsAsync<TimeoutException>(() => applier.ApplyAsync(replacement));
            Assert.That(await store.GetAsync(AggregationRowCodec.NumericMaterialisationKey("source")), Is.Not.Null);
        }

        applier = Create(store, kind);
        await applier.ApplyAsync(replacement);
        await applier.ApplyAsync(replacement);
        var red = await store.GetAsync("red");
        var pending = await store.GetAsync(AggregationRowCodec.NumericMaterialisationKey("source"));
        Assert.Multiple(() =>
        {
            Assert.That(store.AtomicCalls, Is.EqualTo(2), "already-installed membership must not rebind the operation");
            Assert.That(red, Is.Null);
            Assert.That(pending, Is.Null);
        });
        AssertValue(kind, (await store.GetAsync("blue"))!, 7);

        await applier.ApplyAsync(Contribution(kind, "green", 11, 300));
        await applier.ApplyAsync(Contribution(kind, "green", 13, 400));
        Assert.That(await store.GetAsync("blue"), Is.Null);
        AssertValue(kind, (await store.GetAsync("green"))!, 13);
        Assert.That(store.AtomicCalls, Is.EqualTo(kind == AggregationKind.Count ? 3 : 4),
            "different memberships must still publish; an unchanged Count membership is already installed");
    }

    private static AggregationApplier Create(KeySetBoundStore store, AggregationKind kind) =>
        new(store, kind, fanout: 1, maxGroupEntries: 0, operationEpoch: "same-physical-generation");

    private static AggregationContribution Contribution(AggregationKind kind, string group, double value, long ticks) =>
        kind == AggregationKind.Count
            ? AggregationContribution.Membership(group, "source", new HybridLogicalClock { WallClockTicks = ticks })
            : AggregationContribution.OfNumeric(group, "source", value, new HybridLogicalClock { WallClockTicks = ticks });

    private static void AssertValue(AggregationKind kind, byte[] bytes, double sum)
    {
        Assert.That(bytes, Is.Not.Null);
        if (kind == AggregationKind.Count)
        {
            Assert.That(LatticeAggregationValue.DecodeInt64(bytes), Is.EqualTo(1));
        }
        else
        {
            Assert.That(LatticeAggregationValue.DecodeDouble(bytes), Is.EqualTo(sum));
        }
    }
}
