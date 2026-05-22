namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeDeltaContext"/> helper that
/// stamps the author's pre-merge delta onto mutations via the ambient
/// Orleans <c>RequestContext</c>.
/// </summary>
[TestFixture]
public class LatticeDeltaContextTests
{
    [SetUp]
    public void Reset()
    {
        LatticeDeltaContext.Current = null;
    }

    [Test]
    public void Current_defaults_to_null()
    {
        Assert.That(LatticeDeltaContext.Current, Is.Null);
    }

    [Test]
    public void Setting_Current_reads_back_the_same_payload()
    {
        var payload = new byte[] { 1, 2, 3 };
        LatticeDeltaContext.Current = payload;
        Assert.That(LatticeDeltaContext.Current, Is.EqualTo(payload));
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient_value()
    {
        LatticeDeltaContext.Current = new byte[] { 1 };
        LatticeDeltaContext.Current = null;
        Assert.That(LatticeDeltaContext.Current, Is.Null);
    }

    [Test]
    public void With_sets_the_payload_for_the_scope()
    {
        var payload = new byte[] { 9 };
        using (LatticeDeltaContext.With(payload))
        {
            Assert.That(LatticeDeltaContext.Current, Is.EqualTo(payload));
        }
        Assert.That(LatticeDeltaContext.Current, Is.Null);
    }

    [Test]
    public void With_restores_previous_value_on_dispose()
    {
        LatticeDeltaContext.Current = new byte[] { 1 };
        using (LatticeDeltaContext.With(new byte[] { 2 }))
        {
            Assert.That(LatticeDeltaContext.Current, Is.EqualTo(new byte[] { 2 }));
        }
        Assert.That(LatticeDeltaContext.Current, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        using (LatticeDeltaContext.With(new byte[] { 1 }))
        {
            using (LatticeDeltaContext.With(new byte[] { 2 }))
            {
                Assert.That(LatticeDeltaContext.Current, Is.EqualTo(new byte[] { 2 }));
            }
            Assert.That(LatticeDeltaContext.Current, Is.EqualTo(new byte[] { 1 }));
        }
        Assert.That(LatticeDeltaContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        LatticeDeltaContext.Current = new byte[] { 1 };
        var scope = LatticeDeltaContext.With(new byte[] { 2 });

        scope.Dispose();
        Assert.That(LatticeDeltaContext.Current, Is.EqualTo(new byte[] { 1 }));

        // Second dispose must not re-apply the restore - otherwise it would
        // overwrite any value set after the first dispose returned.
        LatticeDeltaContext.Current = new byte[] { 3 };
        scope.Dispose();
        Assert.That(LatticeDeltaContext.Current, Is.EqualTo(new byte[] { 3 }));
    }

    [Test]
    public async Task Current_flows_across_async_await_boundary()
    {
        using (LatticeDeltaContext.With(new byte[] { 7 }))
        {
            await Task.Yield();
            Assert.That(LatticeDeltaContext.Current, Is.EqualTo(new byte[] { 7 }));
        }
    }

    [Test]
    public void With_throws_on_null_payload()
    {
        Assert.That(
            () => LatticeDeltaContext.With(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Current_is_independent_of_LatticeOriginContext()
    {
        // Confirms the two ambient helpers use distinct RequestContext keys
        // and do not alias each other.
        LatticeOriginContext.Current = "cluster-a";
        Assert.That(LatticeDeltaContext.Current, Is.Null);

        LatticeDeltaContext.Current = new byte[] { 1 };
        Assert.That(LatticeOriginContext.Current, Is.EqualTo("cluster-a"));

        // Cleanup
        LatticeOriginContext.Current = null;
    }
}