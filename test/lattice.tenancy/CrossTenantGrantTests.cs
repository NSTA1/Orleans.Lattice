namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="CrossTenantGrant"/>.</summary>
public sealed class CrossTenantGrantTests
{
    [Test]
    public void Create_populates_every_field()
    {
        var grant = CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.Read);

        Assert.Multiple(() =>
        {
            Assert.That(grant.Grantee, Is.EqualTo("sub-a"));
            Assert.That(grant.GranteeKind, Is.EqualTo(TenantGranteeKind.Subject));
            Assert.That(grant.Scope, Is.EqualTo("tree-x"));
            Assert.That(grant.Operations, Is.EqualTo(TenantGrantOperations.Read));
        });
    }

    [Test]
    public void Create_null_grantee_throws()
    {
        Assert.That(
            () => CrossTenantGrant.Create(null!, TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.Read),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Create_null_scope_throws()
    {
        Assert.That(
            () => CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, null!, TenantGrantOperations.Read),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GrantId_is_independent_of_the_operation_set()
    {
        var read = CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.Read);
        var readWrite = CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.ReadWrite);

        Assert.That(read.GrantId, Is.EqualTo(readWrite.GrantId));
    }

    [Test]
    public void GrantId_differs_by_grantee_kind()
    {
        var subject = CrossTenantGrant.Create("acme", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.Read);
        var tenant = CrossTenantGrant.Create("acme", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read);

        Assert.That(subject.GrantId, Is.Not.EqualTo(tenant.GrantId));
    }

    [Test]
    public void GrantId_differs_by_scope()
    {
        var scopeX = CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.Read);
        var scopeY = CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-y", TenantGrantOperations.Read);

        Assert.That(scopeX.GrantId, Is.Not.EqualTo(scopeY.GrantId));
    }

    [Test]
    public void GrantId_does_not_confuse_grantee_and_scope_boundaries()
    {
        // Distinct (grantee, scope) pairs that would collide under naive
        // concatenation must not share a grant id thanks to the unit separator.
        var left = CrossTenantGrant.Create("a", TenantGranteeKind.Subject, "bc", TenantGrantOperations.Read);
        var right = CrossTenantGrant.Create("ab", TenantGranteeKind.Subject, "c", TenantGrantOperations.Read);

        Assert.That(left.GrantId, Is.Not.EqualTo(right.GrantId));
    }
}
