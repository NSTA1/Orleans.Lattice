using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeCompactionPathContext"/>
/// ambient label that carries the compaction walk path (<c>walk</c> for the
/// leaf-chain traversal, <c>dirty-set</c> for the shard-root fast path) into
/// the per-leaf compaction instruments.
/// <para>
/// The nested-restore and idempotent-dispose arms the type documents were both
/// cold: a compaction pass only ever opens one scope in production, so nothing
/// exercised the branch that puts the enclosing label back.
/// </para>
/// </summary>
[TestFixture]
public class LatticeCompactionPathContextTests
{
    private const string Walk = "walk";
    private const string DirtySet = "dirty-set";

    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.CompactionPathRequestContextKey);

    [Test]
    public void Current_is_null_when_no_scope_is_active()
    {
        Assert.That(LatticeCompactionPathContext.Current, Is.Null);
    }

    [Test]
    public void BeginScope_publishes_the_path_label()
    {
        using (LatticeCompactionPathContext.BeginScope(DirtySet))
        {
            Assert.That(LatticeCompactionPathContext.Current, Is.EqualTo(DirtySet));
        }
    }

    [Test]
    public void BeginScope_removes_the_key_again_after_dispose()
    {
        using (LatticeCompactionPathContext.BeginScope(Walk))
        {
            // active
        }

        Assert.That(LatticeCompactionPathContext.Current, Is.Null);
        Assert.That(
            RequestContext.Get(LatticeEventConstants.CompactionPathRequestContextKey),
            Is.Null,
            "outside any scope the instruments must fall back to the no-tag shape");
    }

    [Test]
    public void BeginScope_rejects_a_null_label()
    {
        Assert.That(
            () => LatticeCompactionPathContext.BeginScope(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_label()
    {
        using (LatticeCompactionPathContext.BeginScope(Walk))
        {
            using (LatticeCompactionPathContext.BeginScope(DirtySet))
            {
                Assert.That(LatticeCompactionPathContext.Current, Is.EqualTo(DirtySet));
            }

            Assert.That(
                LatticeCompactionPathContext.Current,
                Is.EqualTo(Walk),
                "the inner scope must put the outer label back, not clear the tag for the rest of the pass");
        }

        Assert.That(LatticeCompactionPathContext.Current, Is.Null);
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeCompactionPathContext.BeginScope(Walk);
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeCompactionPathContext.Current, Is.Null);
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_clear_the_outer_label()
    {
        using (LatticeCompactionPathContext.BeginScope(Walk))
        {
            var inner = LatticeCompactionPathContext.BeginScope(DirtySet);
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeCompactionPathContext.Current, Is.EqualTo(Walk));
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeCompactionPathContext.BeginScope(DirtySet))
        {
            await Task.Yield();
            Assert.That(LatticeCompactionPathContext.Current, Is.EqualTo(DirtySet));
        }

        await Task.Yield();
        Assert.That(LatticeCompactionPathContext.Current, Is.Null);
    }
}
