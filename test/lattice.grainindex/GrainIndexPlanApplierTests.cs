using NSubstitute;
using Orleans.Lattice.GrainIndex.Tests.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexPlanApplier"/>: the single place a plan becomes
/// tree calls, so the foreground write path and the background outbox drain
/// cannot drift apart in how they write.
/// </summary>
[TestFixture]
public sealed class GrainIndexPlanApplierTests
{
    private const string GrainKey = "alice";

    private static readonly KeyValuePair<string, object?> IndexTag = GrainIndexMetrics.IndexTag("plan-applier-tests");

    private static GrainIndexProjection Projection(int age) =>
        EnrollmentTestIndex.Project(GrainKey, new IndexedTestState { Age = age, Country = "GB" });

    [Test]
    public async Task An_empty_plan_never_reaches_the_tree()
    {
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Between(Projection(30), Projection(30));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, IndexTag, "op", CancellationToken.None);

        Assert.That(tree.ReceivedCalls(), Is.Empty,
            "This short-circuit is what makes re-projecting an unchanged grain free.");
    }

    [Test]
    public async Task A_plan_with_no_tombstones_uses_the_plain_atomic_batch()
    {
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection(30));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, IndexTag, "op", CancellationToken.None);

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            "op",
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_plan_with_tombstones_uses_the_mixed_atomic_batch()
    {
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Between(Projection(17), Projection(18));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, IndexTag, "op", CancellationToken.None);

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<IReadOnlyList<string>>(),
            "op",
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_moved_entry_and_its_tombstone_ride_the_same_batch()
    {
        List<KeyValuePair<string, byte[]>>? upserts = null;
        IReadOnlyList<string>? deletes = null;
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Do<List<KeyValuePair<string, byte[]>>>(u => upserts = u),
                Arg.Do<IReadOnlyList<string>>(d => deletes = d),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await GrainIndexPlanApplier.ApplyAsync(
            tree,
            GrainIndexUpdatePlan.Between(Projection(17), Projection(18)),
            IndexTag,
            "op",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                upserts!.Select(u => u.Key),
                Does.Contain(GrainIndexKeyEncoder.EncodeKey("Age", 18, GrainKey)));
            Assert.That(
                deletes,
                Does.Contain(GrainIndexKeyEncoder.EncodeKey("Age", 17, GrainKey)),
                "Splitting these into a write and a delete would let a concurrent scan see the "
                + "grain at both values, or at neither.");
        });
    }

    [Test]
    public async Task The_cancellation_token_reaches_the_tree()
    {
        using var cts = new CancellationTokenSource();
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection(30));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, IndexTag, "op", cts.Token);

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            "op",
            cts.Token);
    }

    [Test]
    [NonParallelizable]
    public async Task Applying_a_plan_records_its_entry_delta_against_the_index()
    {
        using var recorder = new Observability.InstrumentRecorder();
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection(30));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, IndexTag, "op", CancellationToken.None);

        var recorded = recorder.For(GrainIndexMetrics.EntriesName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].Value, Is.EqualTo((double)plan.EntryDelta));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, "plan-applier-tests"), Is.True);
        });
    }

    [Test]
    [NonParallelizable]
    public async Task A_moved_value_records_no_entry_change_because_the_index_still_holds_the_same_count()
    {
        using var recorder = new Observability.InstrumentRecorder();
        var tree = EnrollmentTrees.Accepting();

        await GrainIndexPlanApplier.ApplyAsync(
            tree,
            GrainIndexUpdatePlan.Between(Projection(17), Projection(18)),
            IndexTag,
            "op",
            CancellationToken.None);

        Assert.That(recorder.For(GrainIndexMetrics.EntriesName), Is.Empty,
            "A value that moved replaces one entry with another, so the index holds no more entries "
            + "than it did and the up-down counter must not move.");
    }

    [Test]
    [NonParallelizable]
    public async Task Withdrawing_a_grain_records_a_negative_entry_delta()
    {
        using var recorder = new Observability.InstrumentRecorder();
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Removing(Projection(30));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, IndexTag, "op", CancellationToken.None);

        var recorded = recorder.For(GrainIndexMetrics.EntriesName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].Value, Is.LessThan(0d));
        });
    }

    [Test]
    [NonParallelizable]
    public async Task An_empty_plan_records_no_entry_change()
    {
        using var recorder = new Observability.InstrumentRecorder();
        var tree = EnrollmentTrees.Accepting();

        await GrainIndexPlanApplier.ApplyAsync(
            tree,
            GrainIndexUpdatePlan.Between(Projection(30), Projection(30)),
            IndexTag,
            "op",
            CancellationToken.None);

        Assert.That(recorder.For(GrainIndexMetrics.EntriesName), Is.Empty);
    }

    [Test]
    [NonParallelizable]
    public void A_failed_batch_records_no_entry_change()
    {
        using var recorder = new Observability.InstrumentRecorder();
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("tree unavailable")));

        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection(30));

        Assert.That(
            async () => await GrainIndexPlanApplier.ApplyAsync(
                tree,
                plan,
                IndexTag,
                "op",
                CancellationToken.None),
            Throws.InvalidOperationException);

        Assert.That(recorder.For(GrainIndexMetrics.EntriesName), Is.Empty,
            "Entries that never landed must not be counted as held.");
    }
}
