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
    public void Setting_Current_reads_back_the_same_carry()
    {
        var payload = new byte[] { 1, 2, 3 };
        LatticeDeltaContext.Current = ("lwr", payload);
        var got = LatticeDeltaContext.Current;
        Assert.That(got, Is.Not.Null);
        Assert.That(got!.Value.Kind, Is.EqualTo("lwr"));
        Assert.That(got.Value.Payload, Is.EqualTo(payload));
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient_value()
    {
        LatticeDeltaContext.Current = ("lwr", new byte[] { 1 });
        LatticeDeltaContext.Current = null;
        Assert.That(LatticeDeltaContext.Current, Is.Null);
    }

    [Test]
    public void With_sets_the_carry_for_the_scope()
    {
        var payload = new byte[] { 9 };
        using (LatticeDeltaContext.With("ors", payload))
        {
            var got = LatticeDeltaContext.Current;
            Assert.That(got, Is.Not.Null);
            Assert.That(got!.Value.Kind, Is.EqualTo("ors"));
            Assert.That(got.Value.Payload, Is.EqualTo(payload));
        }
        Assert.That(LatticeDeltaContext.Current, Is.Null);
    }

    [Test]
    public void With_restores_previous_value_on_dispose()
    {
        LatticeDeltaContext.Current = ("outer", new byte[] { 1 });
        using (LatticeDeltaContext.With("inner", new byte[] { 2 }))
        {
            Assert.That(LatticeDeltaContext.Current!.Value.Kind, Is.EqualTo("inner"));
        }
        Assert.That(LatticeDeltaContext.Current!.Value.Kind, Is.EqualTo("outer"));
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        using (LatticeDeltaContext.With("a", new byte[] { 1 }))
        {
            using (LatticeDeltaContext.With("b", new byte[] { 2 }))
            {
                Assert.That(LatticeDeltaContext.Current!.Value.Kind, Is.EqualTo("b"));
            }
            Assert.That(LatticeDeltaContext.Current!.Value.Kind, Is.EqualTo("a"));
        }
        Assert.That(LatticeDeltaContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        LatticeDeltaContext.Current = ("outer", new byte[] { 1 });
        var scope = LatticeDeltaContext.With("inner", new byte[] { 2 });

        scope.Dispose();
        Assert.That(LatticeDeltaContext.Current!.Value.Kind, Is.EqualTo("outer"));

        // Second dispose must not re-apply the restore - otherwise it would
        // overwrite any value set after the first dispose returned.
        LatticeDeltaContext.Current = ("after", new byte[] { 3 });
        scope.Dispose();
        Assert.That(LatticeDeltaContext.Current!.Value.Kind, Is.EqualTo("after"));
    }

    [Test]
    public async Task Current_flows_across_async_await_boundary()
    {
        using (LatticeDeltaContext.With("flowing", new byte[] { 7 }))
        {
            await Task.Yield();
            Assert.That(LatticeDeltaContext.Current!.Value.Kind, Is.EqualTo("flowing"));
        }
    }

    [Test]
    public void With_throws_on_null_kind()
    {
        Assert.That(
            () => LatticeDeltaContext.With(null!, new byte[] { 1 }),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void With_throws_on_empty_kind()
    {
        Assert.That(
            () => LatticeDeltaContext.With(string.Empty, new byte[] { 1 }),
            Throws.ArgumentException);
    }

    [Test]
    public void With_throws_on_null_payload()
    {
        Assert.That(
            () => LatticeDeltaContext.With("lwr", null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Current_is_independent_of_LatticeOriginContext()
    {
        // Confirms the two ambient helpers use distinct RequestContext keys
        // and do not alias each other.
        LatticeOriginContext.Current = "cluster-a";
        Assert.That(LatticeDeltaContext.Current, Is.Null);

        LatticeDeltaContext.Current = ("lwr", new byte[] { 1 });
        Assert.That(LatticeOriginContext.Current, Is.EqualTo("cluster-a"));

        // Cleanup
        LatticeOriginContext.Current = null;
    }
}
