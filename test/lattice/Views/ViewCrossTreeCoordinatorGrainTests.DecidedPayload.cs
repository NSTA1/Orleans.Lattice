using NSubstitute;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewCrossTreeCoordinatorGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task RegisterReadyAsync_a_changed_retry_cannot_replace_a_durably_decided_slice(bool changedTree)
    {
        var h = CreateGrain();
        var first = Substitute.For<ILattice>();
        var replacement = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("view-tree-1").Returns(first);
        h.GrainFactory.GetGrain<ILattice>("view-tree-new").Returns(replacement);
        var attempts = new List<byte[]>();
        var flips = 0;
        first.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                attempts.Add((byte[])call.ArgAt<List<KeyValuePair<string, byte[]>>>(0)[0].Value.Clone());
                return ++flips == 1
                    ? Task.FromException(new TimeoutException("Lost decided joint-flip response."))
                    : Task.CompletedTask;
            });
        var waitSet = new[] { "v1", "v2" };
        await h.Grain.RegisterReadyAsync(Readiness("v1", "view-tree-1", waitSet, Upsert("k", 1)));
        Assert.ThrowsAsync<TimeoutException>(async () =>
            await h.Grain.RegisterReadyAsync(Readiness("v2", "empty-peer", waitSet)));
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.Decided, Is.True);
            Assert.That(h.State.State.Applied, Is.False);
            Assert.That(h.State.WriteCount, Is.GreaterThanOrEqualTo(2), "The payload and decision preceded the interrupted write.");
        });

        var decision = await h.Grain.RegisterReadyAsync(Readiness(
            "v1", changedTree ? "view-tree-new" : "view-tree-1", waitSet, Upsert("k", 2)));
        Assert.Multiple(() =>
        {
            Assert.That(decision.Applied, Is.True);
            Assert.That(h.State.State.Slices["v1"].ViewTreeId, Is.EqualTo("view-tree-1"),
                "A decided operation retains the physical target it committed to.");
            Assert.That(h.State.State.Slices["v1"].Upserts[0].Value, Is.EqualTo(new byte[] { 1 }),
                "Ready admission closed when the durable decision fixed the joint payload.");
            Assert.That(attempts, Has.Count.EqualTo(2));
            Assert.That(attempts.All(value => value.AsSpan().SequenceEqual(new byte[] { 1 })), Is.True,
                "Every retry of the same operation id must carry the exact decided payload.");
        });
        await replacement.DidNotReceive().SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<IReadOnlyList<string>>(),
            Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result()
    {
        var h = CreateGrain();
        var target = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("view-tree-1").Returns(target);
        var attempts = 0;
        target.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (++attempts == 1)
                {
                    h.State.ThrowOnWrite = new TimeoutException("Original completion persistence failed.");
                }
                return Task.CompletedTask;
            });
        await h.Grain.RegisterReadyAsync(Readiness("v1", "view-tree-1", ["v1", "v2"], Upsert("k", 1)));
        Assert.ThrowsAsync<TimeoutException>(async () =>
            await h.Grain.RegisterReadyAsync(Readiness("v2", "empty-peer", ["v1", "v2"])));
        Assert.That(h.State.State.Applied, Is.False, "Failed durability is not a memoized committed response.");
        await h.Grain.RegisterReadyAsync(Readiness("v1", "view-tree-1", ["v1", "v2"], Upsert("k", 9)));
        Assert.That(attempts, Is.EqualTo(2), "Retry the frozen identity rather than certifying an undurable flag.");
        Assert.That(h.State.State.Slices["v1"].Upserts[0].Value, Is.EqualTo(new byte[] { 1 }));
    }
}
