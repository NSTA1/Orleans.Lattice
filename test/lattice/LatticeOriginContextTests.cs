namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeOriginContext"/> helper that
/// stamps origin-cluster metadata onto mutations via the ambient Orleans
/// <c>RequestContext</c>.
/// </summary>
[TestFixture]
public class LatticeOriginContextTests
{
    [SetUp]
    public void Reset()
    {
        // Clear any ambient value leaking from a previous test on this logical thread.
        LatticeOriginContext.Current = null;
    }

    [Test]
    public void Current_defaults_to_null()
    {
        Assert.That(LatticeOriginContext.Current, Is.Null);
    }

    [Test]
    public void Setting_Current_reads_back_the_same_value()
    {
        LatticeOriginContext.Current = "cluster-a";
        Assert.That(LatticeOriginContext.Current, Is.EqualTo("cluster-a"));
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient_value()
    {
        LatticeOriginContext.Current = "cluster-a";
        LatticeOriginContext.Current = null;
        Assert.That(LatticeOriginContext.Current, Is.Null);
    }

    [Test]
    public void With_sets_the_value_for_the_scope()
    {
        using (LatticeOriginContext.With("cluster-b"))
        {
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("cluster-b"));
        }
    }

    [Test]
    public void With_restores_previous_value_on_dispose()
    {
        LatticeOriginContext.Current = "outer";
        using (LatticeOriginContext.With("inner"))
        {
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("inner"));
        }
        Assert.That(LatticeOriginContext.Current, Is.EqualTo("outer"));
    }

    [Test]
    public void With_null_restores_previous_non_null_value_on_dispose()
    {
        LatticeOriginContext.Current = "outer";
        using (LatticeOriginContext.With(null))
        {
            Assert.That(LatticeOriginContext.Current, Is.Null);
        }
        Assert.That(LatticeOriginContext.Current, Is.EqualTo("outer"));
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        using (LatticeOriginContext.With("a"))
        {
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("a"));
            using (LatticeOriginContext.With("b"))
            {
                Assert.That(LatticeOriginContext.Current, Is.EqualTo("b"));
            }
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("a"));
        }
        Assert.That(LatticeOriginContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        LatticeOriginContext.Current = "outer";
        var scope = LatticeOriginContext.With("inner");

        scope.Dispose();
        Assert.That(LatticeOriginContext.Current, Is.EqualTo("outer"));

        // Second dispose must not re-apply the restore — otherwise it would
        // overwrite any value set after the first dispose returned.
        LatticeOriginContext.Current = "after";
        scope.Dispose();
        Assert.That(LatticeOriginContext.Current, Is.EqualTo("after"));
    }

    [Test]
    public async Task Current_flows_across_async_await_boundary()
    {
        using (LatticeOriginContext.With("flowing"))
        {
            await Task.Yield();
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("flowing"));
        }
    }
}
