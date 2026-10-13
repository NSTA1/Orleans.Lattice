using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    private sealed class PerKeySumProjection : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "per-key-sum-v1";

        public AggregationKind Aggregation => AggregationKind.Sum;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation)
        {
            if (mutation.Value is { Length: > 0 } value)
            {
                yield return AggregationContribution.OfNumeric(mutation.Key, mutation.Key, value[0], mutation.Timestamp);
            }
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Aggregation_crossed_prepare_clocks_publish_omitted_member_replacements_in_one_observable_closure(
        bool loseResponse)
    {
        var registration = new ViewRegistration(ViewName, SourceTreeId, Projection: null,
            AggregationProjection: new PerKeySumProjection());
        var view = new FakeViewTree();
        var state = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, state, registration: registration);
        var log = new List<LatticeMutation>
        {
            Ordinary(1, 1),
            new() { TreeId = SourceTreeId, Kind = MutationKind.Set, Key = "j", Value = [1],
                Timestamp = Clock(2), Category = MutationCategory.User },
        };
        ArmLog(h, log);
        await h.Grain.DrainAsync();
        view.Snapshots.Clear();

        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        log.AddRange([
            Prepared(first, Key, 2, 20, 2, 0),
            Prepared(first, "j", 2, 40, 2, 1),
            Prepared(second, Key, 3, 30, 2, 0),
            Prepared(second, "j", 3, 10, 2, 1),
            Commit(first, 41),
            Commit(second, 31),
        ]);
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;
        await h.Grain.DrainAsync();
        view.LoseNextAtomicResponse = loseResponse;
        if (loseResponse)
        {
            Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.DrainAsync());
            h = Create(view, state, registration: registration);
            ArmLog(h, log);
            h.HeadOffset.Value = log.Count;
            await h.Grain.DrainAsync();
        }

        await h.Grain.DrainAsync();

        Assert.That(view.Snapshots, Is.Not.Empty);
        foreach (var snapshot in view.Snapshots)
        {
            var k = LatticeAggregationValue.DecodeDouble(snapshot[Key]);
            var j = LatticeAggregationValue.DecodeDouble(snapshot["j"]);
            Assert.That((k, j), Is.EqualTo((1d, 1d)).Or.EqualTo((2d, 2d)).Or.EqualTo((3d, 2d)),
                "omitting a superseded prepared member may not expose its peer without the replacement (#4869)");
        }
        Assert.That(LatticeAggregationValue.DecodeDouble(view.Rows[Key]), Is.EqualTo(3));
        Assert.That(LatticeAggregationValue.DecodeDouble(view.Rows["j"]), Is.EqualTo(2));
    }
}
