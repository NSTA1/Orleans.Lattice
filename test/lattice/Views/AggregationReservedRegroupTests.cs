using System.Text;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression tests for #5236: a source key that regroups from a valid group
/// into a reserved group key (empty, or NUL-prefixed) must retract its prior
/// membership, exactly as a filter exit does, so an aggregation view is a
/// function of current source state. Incremental maintenance (applying every
/// contribution in WAL order) must then leave the same store as a rebuild
/// (projecting only the current source values into an empty view), and the
/// rejection must still be counted on <c>orleans.lattice.view.aggregation_rejected</c>.
/// </summary>
[TestFixture]
public sealed class AggregationReservedRegroupTests
{
    private sealed class InMemoryAggregationViewStore : IAggregationViewStore
    {
        public SortedDictionary<string, byte[]> Map { get; } = new(StringComparer.Ordinal);

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
            => Task.FromResult(Map.TryGetValue(key, out var v) ? v : null);

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (Map.TryGetValue(key, out var v))
                {
                    result[key] = v;
                }
            }

            return Task.FromResult(result);
        }

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

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    private static AggregationContribution Contribution(AggregationKind kind, string group, string sourceKey, double value, long ticks) => kind switch
    {
        AggregationKind.Count => AggregationContribution.Membership(group, sourceKey, Clock(ticks)),
        AggregationKind.SetUnion => AggregationContribution.SetMember(group, sourceKey, value.ToString(System.Globalization.CultureInfo.InvariantCulture), Clock(ticks)),
        AggregationKind.Fold => AggregationContribution.Fold(group, sourceKey, Encoding.UTF8.GetBytes(value.ToString(System.Globalization.CultureInfo.InvariantCulture)), Clock(ticks)),
        _ => AggregationContribution.OfNumeric(group, sourceKey, value, Clock(ticks)),
    };

    private static AggregationApplier Applier(IAggregationViewStore store, AggregationKind kind, string viewName) =>
        new(store, kind, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", fold: kind == AggregationKind.Fold ? new ConcatFold() : null, viewName: viewName);

    [TestCase(AggregationKind.Count, "")]
    [TestCase(AggregationKind.Count, "\u0000reserved")]
    [TestCase(AggregationKind.Sum, "")]
    [TestCase(AggregationKind.Sum, "\u0000reserved")]
    [TestCase(AggregationKind.Min, "")]
    [TestCase(AggregationKind.Max, "\u0000reserved")]
    [TestCase(AggregationKind.SetUnion, "")]
    [TestCase(AggregationKind.Fold, "")]
    [TestCase(AggregationKind.Fold, "\u0000reserved")]
    public async Task ApplyAsync_reserved_regroup_retracts_prior_membership_so_incremental_equals_rebuild(AggregationKind kind, string reserved)
    {
        var viewName = "reserved-regroup-" + Guid.NewGuid().ToString("N");
        var rejected = 0L;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ViewAggregationRejected,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagView && Equals(tag.Value, viewName))
                    {
                        Interlocked.Add(ref rejected, measurement);
                    }
                }
            }));

        // Incremental: every source mutation in WAL order. "a" leaves a group
        // that keeps another member ("blue" keeps "b"); "c" leaves a group it
        // was alone in ("green" must vanish).
        var incremental = new InMemoryAggregationViewStore();
        var live = Applier(incremental, kind, viewName);
        await live.ApplyAsync(Contribution(kind, "blue", "a", 7, 1));
        await live.ApplyAsync(Contribution(kind, "blue", "b", 3, 2));
        await live.ApplyAsync(Contribution(kind, "green", "c", 5, 3));
        await live.ApplyAsync(Contribution(kind, reserved, "a", 9, 4));
        await live.ApplyAsync(Contribution(kind, reserved, "c", 1, 5));

        // Rebuild: project only the current source values into an empty view.
        var rebuilt = new InMemoryAggregationViewStore();
        var rebuild = Applier(rebuilt, kind, viewName);
        await rebuild.ApplyAsync(Contribution(kind, reserved, "a", 9, 4));
        await rebuild.ApplyAsync(Contribution(kind, "blue", "b", 3, 2));
        await rebuild.ApplyAsync(Contribution(kind, reserved, "c", 1, 5));

        Assert.That(incremental.Map.ContainsKey("green"), Is.False,
            "a key regrouped into a reserved group must leave its prior group, which here had no other member");
        Assert.That(incremental.Map, Is.EqualTo(rebuilt.Map),
            "incremental maintenance must leave the same store as a rebuild over the same source state");
        Assert.That(Interlocked.Read(ref rejected), Is.EqualTo(4),
            "every reserved contribution (two incremental, two rebuild) is still counted as rejected");
    }
}
