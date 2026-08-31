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

    private static GrainIndexProjection Projection(int age) =>
        EnrollmentTestIndex.Project(GrainKey, new IndexedTestState { Age = age, Country = "GB" });

    [Test]
    public async Task An_empty_plan_never_reaches_the_tree()
    {
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Between(Projection(30), Projection(30));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, "op", CancellationToken.None);

        Assert.That(tree.ReceivedCalls(), Is.Empty,
            "This short-circuit is what makes re-projecting an unchanged grain free.");
    }

    [Test]
    public async Task A_plan_with_no_tombstones_uses_the_plain_atomic_batch()
    {
        var tree = EnrollmentTrees.Accepting();
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection(30));

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, "op", CancellationToken.None);

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

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, "op", CancellationToken.None);

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

        await GrainIndexPlanApplier.ApplyAsync(tree, plan, "op", cts.Token);

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            "op",
            cts.Token);
    }
}
