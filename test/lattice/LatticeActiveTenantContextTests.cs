namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeActiveTenantContext"/> ambient helper
/// that stamps the caller's active <see cref="TenantId"/> onto the Orleans
/// <c>RequestContext</c> for propagation to the silo. Mirrors
/// <see cref="LatticeCredentialContextTests"/> and
/// <see cref="LatticeOriginContextTests"/>.
/// </summary>
[TestFixture]
public class LatticeActiveTenantContextTests
{
    [SetUp]
    public void Reset()
    {
        // Clear any ambient value leaking from a previous test on this logical thread.
        LatticeActiveTenantContext.Current = null;
    }

    [Test]
    public void Current_defaults_to_null()
    {
        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
    }

    [Test]
    public void IsActive_defaults_to_false()
    {
        Assert.That(LatticeActiveTenantContext.IsActive, Is.False);
    }

    [Test]
    public void Setting_Current_reads_back_the_same_tenant()
    {
        var tenant = TenantId.Parse("contoso");
        LatticeActiveTenantContext.Current = tenant;

        Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(tenant));
        Assert.That(LatticeActiveTenantContext.IsActive, Is.True);
    }

    [Test]
    public void Setting_Current_to_the_default_tenant_reads_back_the_default_tenant()
    {
        LatticeActiveTenantContext.Current = TenantId.Default;

        Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Default));
        Assert.That(LatticeActiveTenantContext.IsActive, Is.True);
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient_value()
    {
        LatticeActiveTenantContext.Current = TenantId.Parse("contoso");
        LatticeActiveTenantContext.Current = null;

        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
        Assert.That(LatticeActiveTenantContext.IsActive, Is.False);
    }

    [Test]
    public void Setting_Current_to_the_no_tenant_value_clears_the_ambient_value()
    {
        LatticeActiveTenantContext.Current = TenantId.Parse("contoso");
        LatticeActiveTenantContext.Current = default(TenantId);

        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
        Assert.That(LatticeActiveTenantContext.IsActive, Is.False);
    }

    [Test]
    public void With_sets_the_tenant_for_the_scope()
    {
        var tenant = TenantId.Parse("contoso");
        using (LatticeActiveTenantContext.With(tenant))
        {
            Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(tenant));
        }

        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
    }

    [Test]
    public void With_null_clears_the_ambient_tenant_for_the_scope()
    {
        LatticeActiveTenantContext.Current = TenantId.Parse("outer");
        using (LatticeActiveTenantContext.With(null))
        {
            Assert.That(LatticeActiveTenantContext.Current, Is.Null);
        }

        Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("outer")));
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        using (LatticeActiveTenantContext.With(TenantId.Parse("a")))
        {
            Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("a")));
            using (LatticeActiveTenantContext.With(TenantId.Parse("b")))
            {
                Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("b")));
            }

            Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("a")));
        }

        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        LatticeActiveTenantContext.Current = TenantId.Parse("outer");
        var scope = LatticeActiveTenantContext.With(TenantId.Parse("inner"));

        scope.Dispose();
        Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("outer")));

        // Second dispose must not re-apply the restore - otherwise it would
        // overwrite any value set after the first dispose returned.
        LatticeActiveTenantContext.Current = TenantId.Parse("after");
        scope.Dispose();
        Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("after")));
    }

    [Test]
    public async Task Current_flows_across_async_await_boundary()
    {
        using (LatticeActiveTenantContext.With(TenantId.Parse("flowing")))
        {
            await Task.Yield();
            Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("flowing")));
        }
    }
}
