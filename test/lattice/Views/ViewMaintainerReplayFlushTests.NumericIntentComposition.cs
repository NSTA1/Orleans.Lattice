using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    private sealed class OptionalNumericProjection(AggregationKind kind) : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "optional-numeric-v1";

        public AggregationKind Aggregation => kind;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation)
        {
            if (mutation.Value is { Length: > 0 } value && value[0] != 0)
            {
                yield return AggregationContribution.OfNumeric(value[0] == 3 ? "red" : "blue",
                    mutation.Key, value[0], mutation.Timestamp);
            }
        }
    }

    [TestCase(AggregationKind.Count)]
    [TestCase(AggregationKind.Sum)]
    public async Task A_suppressed_interrupted_numeric_event_and_empty_successor_complete_repair_before_checkpoint(
        AggregationKind kind)
    {
        var registration = new ViewRegistration(ViewName, SourceTreeId, Projection: null,
            AggregationProjection: new OptionalNumericProjection(kind));
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>(), registration: registration);
        var log = new List<LatticeMutation> { Ordinary(3, 10) };
        ArmLog(h, log);
        await h.Grain.DrainAsync();
        Assert.That(view.Rows.ContainsKey("red"), Is.True);

        log.Add(Ordinary(7, 20));
        ArmLog(h, log);
        view.LoseNextAtomicResponse = true;
        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.DrainAsync());
        Assert.That(view.Rows.ContainsKey(AggregationRowCodec.NumericMaterialisationKey(Key)), Is.True);
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(0));

        log.Add(Ordinary(0, 30));
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(2));
            Assert.That(view.Rows.ContainsKey(AggregationRowCodec.NumericMaterialisationKey(Key)), Is.False,
                "checkpoint retirement cannot strand the skipped event's durable obligation (#4890 composition)");
            Assert.That(view.Rows.ContainsKey("red"), Is.False);
            Assert.That(view.Rows.ContainsKey("blue"), Is.True);
        });
        Assert.That(kind == AggregationKind.Count
            ? LatticeAggregationValue.DecodeInt64(view.Rows["blue"])
            : LatticeAggregationValue.DecodeDouble(view.Rows["blue"]), Is.EqualTo(kind == AggregationKind.Count ? 1 : 7));
    }
}
