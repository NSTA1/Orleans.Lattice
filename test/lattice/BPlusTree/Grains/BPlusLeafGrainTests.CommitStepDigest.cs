using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the <c>step="digest"</c> label on the
/// <see cref="LatticeMetrics.LeafCommitDuration"/> histogram. The label
/// is recorded once per foreground write that reaches the awaited
/// <c>PublishDigestUpwardAsync</c> hop (single-key
/// <see cref="IBPlusLeafGrain.SetAsync"/> /
/// <see cref="IBPlusLeafGrain.DeleteAsync"/>, and per-leaf
/// <see cref="IBPlusLeafGrain.DeleteRangeAsync"/>); cold / structural
/// digest publishes (split topology, projection-checkpoint flush, saga
/// terminal, compaction reap, cross-shard merge) do <b>not</b> record
/// on the histogram so the per-write attribution stays uncontaminated
/// by one-shot maintenance latency.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Set_with_parent_records_digest_step_on_leaf_commit_duration_once()
    {
        var (grain, _, _) = CreateGrainWithParent(parentId: LeafTestParentId);
        using var recorder = new CommitStepDigestRecorder();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        Assert.That(recorder.DigestStepCount(), Is.EqualTo(1),
            "exactly one step=digest measurement must be recorded per foreground SetAsync that publishes upward");
    }

    [Test]
    public async Task Delete_with_parent_records_digest_step_on_leaf_commit_duration_once()
    {
        var (grain, _, _) = CreateGrainWithParent(parentId: LeafTestParentId);
        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));
        using var recorder = new CommitStepDigestRecorder();

        await grain.DeleteAsync("k0");

        Assert.That(recorder.DigestStepCount(), Is.EqualTo(1),
            "exactly one step=digest measurement must be recorded per foreground DeleteAsync that publishes upward");
    }

    [Test]
    public async Task DeleteRange_with_parent_records_digest_step_once_per_call_not_per_key()
    {
        var (grain, _, _) = CreateGrainWithParent(parentId: LeafTestParentId);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        using var recorder = new CommitStepDigestRecorder();

        var result = await grain.DeleteRangeAsync("a", "z");

        Assert.That(result.Deleted, Is.EqualTo(3),
            "preconditions: the range must cover every previously-set key");
        Assert.That(recorder.DigestStepCount(), Is.EqualTo(1),
            "the per-leaf digest publish for a range delete must record exactly once per call, "
            + "matching the one-shot publication semantics (not once per matched key)");
    }

    [Test]
    public async Task Set_without_parent_records_digest_step_with_zero_publish_cost()
    {
        // Flat-tree leaf (no parent yet): PublishDigestUpwardAsync
        // clears the dirty flag and returns without issuing a cross-grain
        // RPC. The histogram step is still recorded - it measures the
        // *commit-pipeline stage*, including the parent-id resolution
        // and the no-op short-circuit, so operators see a stable label
        // schema across flat-tree and tiered-tree shapes.
        var (grain, _, _) = CreateGrainWithParent(parentId: null);
        using var recorder = new CommitStepDigestRecorder();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        Assert.That(recorder.DigestStepCount(), Is.EqualTo(1),
            "the step=digest measurement is recorded regardless of whether the leaf has a parent; "
            + "the flat-tree case is a zero-cost short-circuit, not an absent stage");
    }

    [Test]
    public async Task Set_records_digest_step_after_wal_apply_observer_steps()
    {
        // Label-schema integrity: a single SetAsync must record one
        // measurement per stage on LeafCommitDuration, and the stages
        // must include digest as a peer of wal / apply / observer.
        var (grain, _, _) = CreateGrainWithParent(parentId: LeafTestParentId);
        using var recorder = new CommitStepDigestRecorder();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.StepCount("wal"), Is.EqualTo(1),
                "exactly one step=wal measurement");
            Assert.That(recorder.StepCount("apply"), Is.EqualTo(1),
                "exactly one step=apply measurement");
            Assert.That(recorder.StepCount("observer"), Is.EqualTo(1),
                "exactly one step=observer measurement");
            Assert.That(recorder.StepCount("digest"), Is.EqualTo(1),
                "exactly one step=digest measurement (the new label)");
        });
    }

    [Test]
    public async Task Multiple_sets_each_record_exactly_one_digest_step()
    {
        var (grain, _, _) = CreateGrainWithParent(parentId: LeafTestParentId);
        using var recorder = new CommitStepDigestRecorder();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        Assert.That(recorder.DigestStepCount(), Is.EqualTo(3),
            "each SetAsync records its own step=digest measurement; the label is per-write, not per-batch");
    }

    [Test]
    public async Task SplitAsync_does_not_record_digest_step_on_leaf_commit_duration()
    {
        // Negative control: the leaf-split path publishes a structural
        // digest update to the parent (BPlusLeafGrain.Split.cs:175) but
        // that publish is deliberately *not* wrapped in
        // RecordCommitStep("digest", ...). The histogram is scoped to
        // the per-write commit pipeline; recording structural one-shots
        // would skew the per-write attribution.
        var state = new FakePersistentState<LeafNodeState>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        sibling.SetNextSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        sibling.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        var grain = CreateGrain(state, siblingStub: sibling, maxLeafKeys: 3);

        // Fill to capacity *before* attaching the recorder so the
        // pre-fill step=digest measurements (per SetAsync) do not
        // contaminate the assertion. The recorder only sees the
        // overflowing fourth SetAsync, which triggers a split.
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        using var recorder = new CommitStepDigestRecorder();
        var splitResult = await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        Assert.That(splitResult, Is.Not.Null,
            "preconditions: the overflowing SetAsync must trigger a split");

        // The overflowing SetAsync records exactly one step=digest from
        // its own foreground CommitSetAsync path. The split's own post-
        // topology digest publish must NOT add a second measurement; if
        // it does, the negative-control assertion fails and the
        // histogram has lost its per-write scope.
        Assert.That(recorder.DigestStepCount(), Is.EqualTo(1),
            "the overflowing SetAsync records one step=digest (its own commit); "
            + "the post-split topology publish must not record a second.");
    }

    /// <summary>
    /// Captures <see cref="LatticeMetrics.LeafCommitDuration"/>
    /// measurements for the lifetime of the recorder and exposes
    /// step-tag aggregates so per-label assertions can be expressed
    /// directly. Subscribes only to the <see cref="LatticeMetrics.Meter"/>
    /// to avoid cross-meter pollution; filtering to the histogram and
    /// step tag is done at read time so the recorder remains usable
    /// as a generic per-test commit-pipeline observer.
    /// </summary>
    private sealed class CommitStepDigestRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, KeyValuePair<string, object?>[] Tags)> _records = new();
        private readonly object _lock = new();

        public CommitStepDigestRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                        l.EnableMeasurementEvents(inst);
                },
            };
            _listener.SetMeasurementEventCallback<double>(OnDouble);
            _listener.Start();
        }

        private void OnDouble(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            lock (_lock)
            {
                _records.Add((instrument.Name, tags.ToArray()));
            }
        }

        public int DigestStepCount() => StepCount("digest");

        public int StepCount(string step)
        {
            lock (_lock)
            {
                var count = 0;
                foreach (var r in _records)
                {
                    if (r.Name != LatticeMetrics.LeafCommitDuration.Name) continue;
                    foreach (var t in r.Tags)
                    {
                        if (t.Key == LatticeMetrics.TagStep && t.Value is string s && s == step)
                        {
                            count++;
                            break;
                        }
                    }
                }
                return count;
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
