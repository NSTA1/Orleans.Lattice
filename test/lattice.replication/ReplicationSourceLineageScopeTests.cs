namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4707: the source lineage stamp an apply entry is judged by travels on
/// an <see cref="AsyncLocal{T}"/> scope. Every concurrent delivery, drained entry
/// and replay must see only its own stamp, across every await, and a scope must
/// never outlive the flow that entered it. Each test fails if the carrier is
/// shared between flows or leaks back to a caller.
/// </summary>
[TestFixture]
public sealed class ReplicationSourceLineageScopeTests
{
    private static readonly ReplicationSourceLineageStamp StampA = new("site-a", Guid.Parse("aaaaaaaa-0000-4000-8000-000000000001"));
    private static readonly ReplicationSourceLineageStamp StampB = new("site-a", Guid.Parse("bbbbbbbb-0000-4000-8000-000000000002"));

    [Test]
    public async Task Two_concurrent_flows_each_see_their_own_stamp_across_awaits()
    {
        var aEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var bEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<ReplicationSourceLineageStamp?> Flow(ReplicationSourceLineageStamp stamp, TaskCompletionSource mine)
        {
            using var scope = ReplicationSourceLineageScope.Enter(stamp);

            // Both flows hold a live scope before either reads it back.
            mine.SetResult();
            await Task.WhenAll(aEntered.Task, bEntered.Task);
            await Task.Yield();
            return ReplicationSourceLineageScope.Current;
        }

        var a = Task.Run(() => Flow(StampA, aEntered));
        var b = Task.Run(() => Flow(StampB, bEntered));
        var seen = await Task.WhenAll(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(seen[0], Is.EqualTo(StampA));
            Assert.That(seen[1], Is.EqualTo(StampB));
            Assert.That(ReplicationSourceLineageScope.Current, Is.Null, "neither flow's scope reaches the caller");
        });
    }

    [Test]
    public async Task A_scope_entered_inside_an_awaited_call_does_not_reach_the_caller_even_undisposed()
    {
        static async Task EnterAndForgetAsync()
        {
            ReplicationSourceLineageScope.Enter(StampB);
            await Task.Yield();
        }

        using (ReplicationSourceLineageScope.Enter(StampA))
        {
            await EnterAndForgetAsync();
            Assert.That(ReplicationSourceLineageScope.Current, Is.EqualTo(StampA),
                "an async callee's scope change is undone when it returns, so a drained entry's or a replay's scope cannot leak");
        }

        Assert.That(ReplicationSourceLineageScope.Current, Is.Null);
    }

    [Test]
    public void An_unstamped_inner_scope_replaces_a_stamped_outer_one_and_restores_it_on_dispose()
    {
        using var outer = ReplicationSourceLineageScope.Enter(StampA);
        using (ReplicationSourceLineageScope.Enter(null))
        {
            Assert.That(ReplicationSourceLineageScope.Current, Is.Null,
                "an unstamped entry drained inside a stamped delivery is not judged by the delivery's stamp");
        }

        Assert.That(ReplicationSourceLineageScope.Current, Is.EqualTo(StampA));
    }

    [Test]
    public void A_cached_verdict_answers_only_for_the_tree_it_was_reached_for()
    {
        using var scope = ReplicationSourceLineageScope.Enter(StampA);
        scope.Record("tree-1", ReplicationSourceLineageGate.Verdict.RefuseLineage);

        Assert.Multiple(() =>
        {
            Assert.That(scope.TryGetVerdict("tree-1", out var verdict), Is.True);
            Assert.That(verdict, Is.EqualTo(ReplicationSourceLineageGate.Verdict.RefuseLineage));
            Assert.That(scope.TryGetVerdict("tree-2", out _), Is.False, "another tree in the same delivery is checked afresh");
        });
    }

    [Test]
    public void Enter_by_source_and_lineage_stamps_only_when_a_lineage_is_given()
    {
        using (var stamped = ReplicationSourceLineageScope.Enter("site-a", StampA.Lineage, Guid.Empty))
        {
            Assert.Multiple(() =>
            {
                Assert.That(stamped.Stamp, Is.EqualTo(StampA));
                Assert.That(stamped.ObservedFrontierEpoch, Is.EqualTo(Guid.Empty));
                Assert.That(ReplicationSourceLineageScope.Active, Is.SameAs(stamped));
            });
        }

        using var unstamped = ReplicationSourceLineageScope.Enter("site-a", null);
        Assert.That(unstamped.Stamp, Is.Null);
    }
}
