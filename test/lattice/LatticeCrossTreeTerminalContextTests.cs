using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeCrossTreeTerminalContext"/>
/// ambient that carries a cross-tree sub-saga's operation id and participant
/// tree-id set into the per-shard terminal publish helpers, where both are
/// stamped onto the emitted terminal for the receiver-side cross-tree
/// visibility barrier.
/// <para>
/// The setter's three rejection arms were cold. They are the type's safety
/// property rather than an input-validation nicety: a terminal carrying an
/// operation id but an empty participant set would open a barrier that can
/// never be satisfied, so the setter must drop a partial value rather than
/// publish it. Nothing asserted that it did.
/// </para>
/// </summary>
[TestFixture]
public class LatticeCrossTreeTerminalContextTests
{
    private const string OperationId = "op-1";

    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.CrossTreeTerminalRequestContextKey);

    [Test]
    public void Current_is_null_for_a_single_tree_terminal()
    {
        Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
    }

    [Test]
    public void Current_is_null_when_the_context_carries_a_foreign_value()
    {
        RequestContext.Set(LatticeEventConstants.CrossTreeTerminalRequestContextKey, "op-1");

        Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
    }

    [Test]
    public void Current_round_trips_a_complete_value()
    {
        LatticeCrossTreeTerminalContext.Current =
            new CrossTreeTerminalInfo(OperationId, new[] { "a", "b" });

        var current = LatticeCrossTreeTerminalContext.Current;

        Assert.That(current, Is.Not.Null);
        Assert.That(current!.Value.OperationId, Is.EqualTo(OperationId));
        Assert.That(current.Value.Participants, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void Setting_null_removes_the_entry()
    {
        LatticeCrossTreeTerminalContext.Current =
            new CrossTreeTerminalInfo(OperationId, new[] { "a" });
        LatticeCrossTreeTerminalContext.Current = null;

        Assert.That(
            RequestContext.Get(LatticeEventConstants.CrossTreeTerminalRequestContextKey),
            Is.Null);
    }

    [Test]
    public void Setting_an_empty_operation_id_removes_the_entry()
    {
        LatticeCrossTreeTerminalContext.Current =
            new CrossTreeTerminalInfo(OperationId, new[] { "a" });
        LatticeCrossTreeTerminalContext.Current =
            new CrossTreeTerminalInfo(string.Empty, new[] { "a" });

        Assert.That(
            LatticeCrossTreeTerminalContext.Current,
            Is.Null,
            "a terminal with no operation id cannot address a barrier, so it must not be published");
    }

    [Test]
    public void Setting_an_empty_participant_set_removes_the_entry()
    {
        LatticeCrossTreeTerminalContext.Current =
            new CrossTreeTerminalInfo(OperationId, new[] { "a" });
        LatticeCrossTreeTerminalContext.Current =
            new CrossTreeTerminalInfo(OperationId, Array.Empty<string>());

        Assert.That(
            LatticeCrossTreeTerminalContext.Current,
            Is.Null,
            "a barrier over no participants could never be satisfied, so a partial value must be dropped");
    }

    [Test]
    public void With_publishes_both_slots_for_the_lifetime_of_the_scope()
    {
        using (LatticeCrossTreeTerminalContext.With(OperationId, new[] { "a", "b" }))
        {
            var current = LatticeCrossTreeTerminalContext.Current;
            Assert.That(current, Is.Not.Null);
            Assert.That(current!.Value.OperationId, Is.EqualTo(OperationId));
            Assert.That(current.Value.Participants.Count, Is.EqualTo(2));
        }

        Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
    }

    [TestCase(null, new string[] { "a" })]
    [TestCase("", new string[] { "a" })]
    [TestCase(OperationId, new string[0])]
    public void With_an_incomplete_value_clears_the_ambient(string? operationId, string[] participants)
    {
        using (LatticeCrossTreeTerminalContext.With(OperationId, new[] { "seed" }))
        {
            using (LatticeCrossTreeTerminalContext.With(operationId, participants))
            {
                Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
            }

            Assert.That(
                LatticeCrossTreeTerminalContext.Current?.OperationId,
                Is.EqualTo(OperationId),
                "clearing inside a sub-saga must not strip the enclosing value on the way out");
        }
    }

    [Test]
    public void With_a_null_participant_list_clears_the_ambient()
    {
        using (LatticeCrossTreeTerminalContext.With(OperationId, null))
        {
            Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
        }
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_value()
    {
        using (LatticeCrossTreeTerminalContext.With(OperationId, new[] { "a" }))
        {
            using (LatticeCrossTreeTerminalContext.With("op-2", new[] { "b", "c" }))
            {
                Assert.That(LatticeCrossTreeTerminalContext.Current?.OperationId, Is.EqualTo("op-2"));
            }

            Assert.That(LatticeCrossTreeTerminalContext.Current?.OperationId, Is.EqualTo(OperationId));
        }

        Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeCrossTreeTerminalContext.With(OperationId, new[] { "a" });
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_clear_the_outer_value()
    {
        using (LatticeCrossTreeTerminalContext.With(OperationId, new[] { "a" }))
        {
            var inner = LatticeCrossTreeTerminalContext.With("op-2", new[] { "b" });
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeCrossTreeTerminalContext.Current?.OperationId, Is.EqualTo(OperationId));
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeCrossTreeTerminalContext.With(OperationId, new[] { "a" }))
        {
            await Task.Yield();
            Assert.That(LatticeCrossTreeTerminalContext.Current?.OperationId, Is.EqualTo(OperationId));
        }

        await Task.Yield();
        Assert.That(LatticeCrossTreeTerminalContext.Current, Is.Null);
    }
}
