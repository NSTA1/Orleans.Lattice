using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeReplayAdmissionContext"/>, the ambient that
/// declares a call chain's WAL replay admission class so the per-silo replay
/// permit queue can be prioritised rather than merely bounded.
/// <para>
/// Alongside the scope mechanics, this pins the type's central <b>safety</b>
/// claim - that declaring a scope can only ever make a caller more likely to be
/// refused, never less. The absent default is
/// <see cref="LatticeReplayAdmissionClass.Interactive"/>, and no value of the
/// flag admits a caller the interactive bound would have refused; that
/// asymmetry is the reason the seam is safe to expose at all, and nothing
/// asserted it.
/// </para>
/// </summary>
[TestFixture]
public class LatticeReplayAdmissionContextTests
{
    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.ReplayAdmissionBulkRequestContextKey);

    [Test]
    public void Current_defaults_to_Interactive_outside_any_scope()
    {
        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive),
            "a caller that has never heard of this seam must never be the one de-prioritised");
    }

    [Test]
    public void Current_is_Interactive_when_the_context_carries_a_foreign_value()
    {
        RequestContext.Set(LatticeEventConstants.ReplayAdmissionBulkRequestContextKey, "bulk");

        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive));
    }

    [Test]
    public void Interactive_is_the_zero_value_so_an_unset_flag_cannot_mean_Bulk()
    {
        Assert.That((int)LatticeReplayAdmissionClass.Interactive, Is.Zero);
        Assert.That((int)LatticeReplayAdmissionClass.Bulk, Is.EqualTo(1));
    }

    [Test]
    public void BeginBulkScope_declares_Bulk_for_the_lifetime_of_the_scope()
    {
        using (LatticeReplayAdmissionContext.BeginBulkScope())
        {
            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk));
        }

        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive));
    }

    [Test]
    public void BeginBulkScope_removes_the_key_again_after_dispose()
    {
        using (LatticeReplayAdmissionContext.BeginBulkScope())
        {
            // active
        }

        Assert.That(
            RequestContext.Get(LatticeEventConstants.ReplayAdmissionBulkRequestContextKey),
            Is.Null,
            "an outermost scope had no prior class, so disposal must remove the key rather than store false");
    }

    [Test]
    public void There_is_no_scope_that_declares_Interactive()
    {
        // The seam is deliberately one-way: the only scope it offers raises the
        // caller's admission class. A reader looking for a BeginInteractiveScope
        // is looking for a priority boost, and the type documents that none
        // exists - this pins that, so adding one is a deliberate act.
        var scopeFactories = typeof(LatticeReplayAdmissionContext)
            .GetMethods()
            .Where(m => m.IsStatic && typeof(IDisposable).IsAssignableFrom(m.ReturnType))
            .Select(m => m.Name)
            .ToArray();

        Assert.That(scopeFactories, Is.EqualTo(new[] { nameof(LatticeReplayAdmissionContext.BeginBulkScope) }));
    }

    [Test]
    public void Nested_scope_keeps_the_enclosing_bulk_declaration()
    {
        using (LatticeReplayAdmissionContext.BeginBulkScope())
        {
            using (LatticeReplayAdmissionContext.BeginBulkScope())
            {
                Assert.That(
                    LatticeReplayAdmissionContext.Current,
                    Is.EqualTo(LatticeReplayAdmissionClass.Bulk));
            }

            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "an inner fan-out closing must not silently promote the rest of a bulk walk to interactive");
        }

        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive));
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeReplayAdmissionContext.BeginBulkScope();
        scope.Dispose();
        scope.Dispose();

        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive));
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_promote_the_outer_walk()
    {
        using (LatticeReplayAdmissionContext.BeginBulkScope())
        {
            var inner = LatticeReplayAdmissionContext.BeginBulkScope();
            inner.Dispose();
            inner.Dispose();

            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk));
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeReplayAdmissionContext.BeginBulkScope())
        {
            await Task.Yield();
            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk));
        }

        await Task.Yield();
        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive));
    }
}
