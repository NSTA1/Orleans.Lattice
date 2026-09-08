using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue #2312: <c>PersistAsync</c>'s <c>finally</c> block
/// dereferenced <c>state.State</c> unguarded, after the durable write had
/// already committed.
/// <para>
/// Two independent harms, one per test here. First, a committed write reported
/// as failed: the persist lands, the telemetry read throws, and every caller -
/// including <c>FlushPendingCheckpointAsync</c> - observes a failed persist for
/// a write that is durably present. That is the #2220 class arriving through
/// the metrics arm. Second, a destroyed diagnostic: an exception thrown from a
/// <c>finally</c> supersedes the one in flight, so a genuine persist failure on
/// an invalid activation was replaced by the failure of the code measuring how
/// long the persist took.
/// </para>
/// <para>
/// This defect was <b>never observed in the field</b>, and that is not evidence
/// it is unreachable. All 340 recorded traces belong to the sibling read in
/// <c>TryPublishByteFootprintAsync</c> (issue #2264, fixed by #2313): that site
/// throws a frame earlier on the same turn, so it was reached first and the
/// stacks never name this one. The absence was a consequence of the other
/// defect firing first, which is also why fixing that one alone would have left
/// this reachable for the first time.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string TelemetryGuardTreeId = "tree-2312";

    [Test]
    public async Task Persist_completes_normally_when_the_write_invalidates_the_activation()
    {
        var inner = new FakePersistentState<LeafNodeState> { RecordExistsValue = true };
        inner.State.TreeId = TelemetryGuardTreeId;
        var state = new ActivationInvalidatedByWriteState<LeafNodeState>(inner);
        var grain = CreateGrainOverState(state);

        var recordedTags = new List<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafWriteDuration,
            l => l.SetMeasurementEventCallback<double>((_, _, tags, _) =>
            {
                var treeTag = ReadTreeTag(tags);
                if (treeTag != TelemetryGuardTreeId)
                {
                    // A measurement from some other fixture sharing this
                    // process-wide instrument. Ignore it, so a concurrent test
                    // can neither satisfy nor weaken the assertions below.
                    return;
                }

                recordedTags.Add(treeTag);
            }));

        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "sibling-2312"));

        Assert.Multiple(() =>
        {
            Assert.That(inner.WriteCount, Is.EqualTo(1),
                "Test precondition: the durable write must have committed.");
            Assert.That(recordedTags, Has.Count.EqualTo(1),
                "PersistAsync threw from its telemetry finally after the durable "
                + "write had already committed, so the caller - including "
                + "FlushPendingCheckpointAsync - sees a failed persist for a write "
                + "that is durably present (#2312, the #2220 class through the "
                + "metrics arm).");
            Assert.That(recordedTags[0], Is.EqualTo(TelemetryGuardTreeId),
                "The recorded tree tag must be unchanged by the fix: it is read at "
                + "entry, which is the correct time for a measurement describing "
                + "the write just performed.");
        });
    }

    [Test]
    public void Persist_propagates_the_persist_failure_not_the_telemetry_failure()
    {
        var inner = new FakePersistentState<LeafNodeState> { RecordExistsValue = true };
        inner.State.TreeId = TelemetryGuardTreeId;
        var state = new ActivationInvalidatedByWriteState<LeafNodeState>(inner);
        var grain = CreateGrainOverState(state);

        // TimeoutException stands in for a real storage fault and is chosen so
        // it cannot be confused with the InvalidOperationException the invalid
        // activation raises.
        inner.ThrowOnWrite = new TimeoutException("simulated storage failure on WriteStateAsync");

        var thrown = Assert.CatchAsync(
            async () => await grain.SetNextSiblingAsync(GrainId.Create("leaf", "sibling-2312")));

        Assert.That(thrown, Is.TypeOf<TimeoutException>(),
            "The persist failure must reach the caller. An exception thrown from a "
            + "finally supersedes the one in flight, so an unguarded telemetry read "
            + "there discards the only diagnostic that explains the real fault and "
            + "reports a metrics failure in its place (#2312).");
    }

    [Test]
    public async Task Persist_records_the_tree_tag_on_the_healthy_path()
    {
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = true };
        state.State.TreeId = TelemetryGuardTreeId;
        var grain = CreateGrainOverState(state);

        var recordedTags = new List<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafWriteDuration,
            l => l.SetMeasurementEventCallback<double>((_, _, tags, _) =>
            {
                var treeTag = ReadTreeTag(tags);
                if (treeTag == TelemetryGuardTreeId)
                {
                    recordedTags.Add(treeTag);
                }
            }));

        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "sibling-2312"));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(recordedTags, Has.Count.EqualTo(1),
                "A healthy persist must still record exactly one latency measurement.");
        });
    }

    private static string? ReadTreeTag(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
            {
                return tag.Value as string;
            }
        }

        return null;
    }

    /// <summary>
    /// Builds a leaf grain over an arbitrary <see cref="IPersistentState{T}"/>.
    /// The shared <c>CreateGrain</c> helper takes the concrete
    /// <see cref="FakePersistentState{T}"/>, so these tests - which need a
    /// decorator around it - wire the minimal set of collaborators themselves.
    /// </summary>
    private static BPlusLeafGrain CreateGrainOverState(IPersistentState<LeafNodeState> state)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "telemetry-guard-2312"));
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(factory: grainFactory);
        return new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
    }

    /// <summary>
    /// Persistent-state decorator that models the field sequence behind #2312:
    /// the durable write completes, and the activation is invalid by the time
    /// anything reads <see cref="IStorage{TState}.State"/> afterwards.
    /// <para>
    /// Invalidation is applied in a <c>finally</c> around the inner write, so
    /// it holds whether the write succeeded or failed - the two arms of the
    /// defect. Reads made <em>before</em> the write are unaffected, which is
    /// what lets an ordinary public entrypoint set up the mutation it is about
    /// to persist.
    /// </para>
    /// </summary>
    private sealed class ActivationInvalidatedByWriteState<T>(FakePersistentState<T> inner)
        : IPersistentState<T>
        where T : new()
    {
        private bool _invalidated;

        public T State
        {
            get => _invalidated
                ? throw new InvalidOperationException(
                    "Attempt to access an invalid activation: [Activation: bplusleaf/telemetry-guard-2312]")
                : inner.State;
            set => inner.State = value;
        }

        public string Etag => inner.Etag;

        public bool RecordExists => inner.RecordExists;

        public Task ClearStateAsync() => inner.ClearStateAsync();

        public Task ReadStateAsync() => inner.ReadStateAsync();

        public async Task WriteStateAsync()
        {
            try
            {
                await inner.WriteStateAsync();
            }
            finally
            {
                _invalidated = true;
            }
        }
    }
}
