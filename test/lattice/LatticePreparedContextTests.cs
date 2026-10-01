using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticePreparedContext"/> ambient
/// prepare-phase flag that stamps <see cref="LatticeMutation.IsPrepared"/> for
/// the duration of a saga's prepare-phase write.
/// <para>
/// The two properties the type documents but nothing asserted are the ones
/// covered here: a nested scope restores the <b>enclosing</b> flag rather than
/// clearing the key outright, and disposal is idempotent. Both are only
/// observable on a second dispose or an inner scope, which is exactly why they
/// stayed cold while the happy path read as covered.
/// </para>
/// </summary>
[TestFixture]
public class LatticePreparedContextTests
{
    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.PreparedRequestContextKey);

    [Test]
    public void Current_defaults_to_false_when_no_scope_is_active()
    {
        Assert.That(LatticePreparedContext.Current, Is.False);
    }

    [Test]
    public void Current_is_false_when_the_context_carries_a_foreign_value()
    {
        // A caller on an older or unrelated build could leave a non-bool under
        // the key; the flag must read as "not prepared" rather than throw.
        RequestContext.Set(LatticeEventConstants.PreparedRequestContextKey, "yes");

        Assert.That(LatticePreparedContext.Current, Is.False);
    }

    [Test]
    public void BeginScope_flips_Current_to_true()
    {
        using (LatticePreparedContext.BeginScope())
        {
            Assert.That(LatticePreparedContext.Current, Is.True);
        }
    }

    [Test]
    public void BeginScope_removes_the_key_again_after_dispose()
    {
        using (LatticePreparedContext.BeginScope())
        {
            // active
        }

        Assert.That(LatticePreparedContext.Current, Is.False);
        Assert.That(
            RequestContext.Get(LatticeEventConstants.PreparedRequestContextKey),
            Is.Null,
            "an outermost scope had no prior value, so disposal must remove the key rather than store false");
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_flag_rather_than_clearing_it()
    {
        using (LatticePreparedContext.BeginScope())
        {
            using (LatticePreparedContext.BeginScope())
            {
                Assert.That(LatticePreparedContext.Current, Is.True);
            }

            Assert.That(
                LatticePreparedContext.Current,
                Is.True,
                "the inner scope must restore the outer scope's flag, not remove the key");
        }

        Assert.That(LatticePreparedContext.Current, Is.False);
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticePreparedContext.BeginScope();
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticePreparedContext.Current, Is.False);
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_clear_the_outer_flag()
    {
        using (LatticePreparedContext.BeginScope())
        {
            var inner = LatticePreparedContext.BeginScope();
            inner.Dispose();
            inner.Dispose();

            Assert.That(
                LatticePreparedContext.Current,
                Is.True,
                "a second dispose must be a no-op; re-running the restore would be harmless here but re-running a removal would not");
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticePreparedContext.BeginScope())
        {
            await Task.Yield();
            Assert.That(LatticePreparedContext.Current, Is.True);
        }

        await Task.Yield();
        Assert.That(LatticePreparedContext.Current, Is.False);
    }
}
