using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeCompactionTriggerContext"/>
/// ambient label that carries the compaction trigger
/// (<c>reminder</c> / <c>ratio</c> / <c>size</c> / <c>operator</c>) into the
/// per-leaf compaction instruments.
/// <para>
/// Sibling of <see cref="LatticeCompactionPathContextTests"/>: the two ambients
/// are opened together by the compaction grain and had the same cold arms -
/// the nested restore and the second dispose.
/// </para>
/// </summary>
[TestFixture]
public class LatticeCompactionTriggerContextTests
{
    private const string Reminder = "reminder";
    private const string Operator = "operator";

    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.CompactionTriggerRequestContextKey);

    [Test]
    public void Current_is_null_when_no_scope_is_active()
    {
        Assert.That(LatticeCompactionTriggerContext.Current, Is.Null);
    }

    [Test]
    public void BeginScope_publishes_the_trigger_label()
    {
        using (LatticeCompactionTriggerContext.BeginScope(Reminder))
        {
            Assert.That(LatticeCompactionTriggerContext.Current, Is.EqualTo(Reminder));
        }
    }

    [Test]
    public void BeginScope_removes_the_key_again_after_dispose()
    {
        using (LatticeCompactionTriggerContext.BeginScope(Reminder))
        {
            // active
        }

        Assert.That(LatticeCompactionTriggerContext.Current, Is.Null);
        Assert.That(
            RequestContext.Get(LatticeEventConstants.CompactionTriggerRequestContextKey),
            Is.Null,
            "outside any scope the instruments must fall back to the no-tag shape");
    }

    [Test]
    public void BeginScope_rejects_a_null_label()
    {
        Assert.That(
            () => LatticeCompactionTriggerContext.BeginScope(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_label()
    {
        using (LatticeCompactionTriggerContext.BeginScope(Reminder))
        {
            using (LatticeCompactionTriggerContext.BeginScope(Operator))
            {
                Assert.That(LatticeCompactionTriggerContext.Current, Is.EqualTo(Operator));
            }

            Assert.That(
                LatticeCompactionTriggerContext.Current,
                Is.EqualTo(Reminder),
                "the inner scope must put the outer label back, not clear the tag for the rest of the pass");
        }

        Assert.That(LatticeCompactionTriggerContext.Current, Is.Null);
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeCompactionTriggerContext.BeginScope(Reminder);
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeCompactionTriggerContext.Current, Is.Null);
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_clear_the_outer_label()
    {
        using (LatticeCompactionTriggerContext.BeginScope(Reminder))
        {
            var inner = LatticeCompactionTriggerContext.BeginScope(Operator);
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeCompactionTriggerContext.Current, Is.EqualTo(Reminder));
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeCompactionTriggerContext.BeginScope(Operator))
        {
            await Task.Yield();
            Assert.That(LatticeCompactionTriggerContext.Current, Is.EqualTo(Operator));
        }

        await Task.Yield();
        Assert.That(LatticeCompactionTriggerContext.Current, Is.Null);
    }
}
