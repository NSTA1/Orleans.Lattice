using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeCommitLogContext"/> ambient
/// that flags a call as driven by the commit-log adapter, so a
/// replication-aware observer can short-circuit rather than re-append its own
/// input back into the WAL.
/// <para>
/// The loop-prevention contract is what makes the idempotent-dispose arm worth
/// asserting: this scope's restore arm is the one that decides whether the flag
/// survives for the rest of an enclosing commit, and a stray second dispose
/// clearing it early would reopen the producer-consumer loop the type exists to
/// close.
/// </para>
/// </summary>
[TestFixture]
public class LatticeCommitLogContextTests
{
    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.CommitLogSourceRequestContextKey);

    [Test]
    public void Current_is_false_for_a_direct_foreground_caller()
    {
        Assert.That(LatticeCommitLogContext.Current, Is.False);
    }

    [Test]
    public void Current_is_false_when_the_context_carries_a_foreign_value()
    {
        RequestContext.Set(LatticeEventConstants.CommitLogSourceRequestContextKey, 1);

        Assert.That(LatticeCommitLogContext.Current, Is.False);
    }

    [Test]
    public void BeginScope_flips_Current_to_true()
    {
        using (LatticeCommitLogContext.BeginScope())
        {
            Assert.That(LatticeCommitLogContext.Current, Is.True);
        }
    }

    [Test]
    public void BeginScope_removes_the_key_again_after_dispose()
    {
        using (LatticeCommitLogContext.BeginScope())
        {
            // active
        }

        Assert.That(LatticeCommitLogContext.Current, Is.False);
        Assert.That(
            RequestContext.Get(LatticeEventConstants.CommitLogSourceRequestContextKey),
            Is.Null,
            "an outermost scope had no prior flag, so disposal must remove the key rather than store false");
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_flag()
    {
        using (LatticeCommitLogContext.BeginScope())
        {
            using (LatticeCommitLogContext.BeginScope())
            {
                Assert.That(LatticeCommitLogContext.Current, Is.True);
            }

            Assert.That(
                LatticeCommitLogContext.Current,
                Is.True,
                "an inner scope closing must not drop the adapter flag for the rest of the enclosing commit");
        }

        Assert.That(LatticeCommitLogContext.Current, Is.False);
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeCommitLogContext.BeginScope();
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeCommitLogContext.Current, Is.False);
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_clear_the_outer_flag()
    {
        using (LatticeCommitLogContext.BeginScope())
        {
            var inner = LatticeCommitLogContext.BeginScope();
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeCommitLogContext.Current, Is.True);
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeCommitLogContext.BeginScope())
        {
            await Task.Yield();
            Assert.That(LatticeCommitLogContext.Current, Is.True);
        }

        await Task.Yield();
        Assert.That(LatticeCommitLogContext.Current, Is.False);
    }
}
