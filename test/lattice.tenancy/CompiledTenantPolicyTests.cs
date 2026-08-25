using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledTenantPolicy"/>: compilation of the
/// subject-to-tenants index, the per-tenant admin set and status, and the
/// tenant-grantee grant index, plus the empty-snapshot and zero-allocation
/// lookup behaviour.
/// </summary>
public sealed class CompiledTenantPolicyTests
{
    [Test]
    public void Empty_snapshot_has_no_tenants_or_subjects()
    {
        var snapshot = CompiledTenantPolicy.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TenantCount, Is.EqualTo(0));
            Assert.That(snapshot.SubjectCount, Is.EqualTo(0));
            Assert.That(snapshot.ResolveAllowedTenants("anyone"), Is.Empty);
            Assert.That(snapshot.TryGetTenant("acme", out _), Is.False);
        });
    }

    [Test]
    public void Compile_with_no_records_returns_the_shared_empty_snapshot()
    {
        var snapshot = CompiledTenantPolicy.Compile([]);

        Assert.That(snapshot, Is.SameAs(CompiledTenantPolicy.Empty));
    }

    [Test]
    public void Compile_null_records_throws()
    {
        Assert.That(() => CompiledTenantPolicy.Compile(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveAllowedTenants_null_subject_throws()
    {
        Assert.That(() => CompiledTenantPolicy.Empty.ResolveAllowedTenants(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveAllowedTenants_returns_the_shared_empty_array_for_an_unknown_subject()
    {
        var snapshot = CompiledTenantPolicy.Compile([Record("acme", admins: ["alice"])]);

        var first = snapshot.ResolveAllowedTenants("nobody");
        var second = snapshot.ResolveAllowedTenants("nobody");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Empty);
            // The warm miss path is allocation-free: the same shared empty array.
            Assert.That(first, Is.SameAs(second));
        });
    }

    [Test]
    public void Compile_indexes_a_subject_across_multiple_tenants_in_tenant_order()
    {
        var snapshot = CompiledTenantPolicy.Compile(
        [
            Record("beta", admins: ["alice"]),
            Record("acme", admins: ["alice", "bob"]),
        ]);

        var alice = snapshot.ResolveAllowedTenants("alice");
        var bob = snapshot.ResolveAllowedTenants("bob");

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TenantCount, Is.EqualTo(2));
            Assert.That(snapshot.SubjectCount, Is.EqualTo(2));
            Assert.That(alice, Is.EqualTo(new[] { TenantId.Parse("acme"), TenantId.Parse("beta") }));
            Assert.That(bob, Is.EqualTo(new[] { TenantId.Parse("acme") }));
        });
    }

    [Test]
    public void Compile_captures_tenant_status_and_admin_membership()
    {
        var snapshot = CompiledTenantPolicy.Compile(
        [
            Record("acme", TenantStatus.Suspended, admins: ["alice"]),
        ]);

        Assert.That(snapshot.TryGetTenant("acme", out var tenant), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(tenant!.Status, Is.EqualTo(TenantStatus.Suspended));
            Assert.That(tenant.IsAdmin("alice"), Is.True);
            Assert.That(tenant.IsAdmin("bob"), Is.False);
        });
    }

    [Test]
    public void Compile_indexes_only_tenant_grantee_grants_by_grantee_tenant()
    {
        var snapshot = CompiledTenantPolicy.Compile(
        [
            Record(
                "acme",
                admins: ["alice"],
                grants:
                [
                    TenantGrant("beta", "orders", TenantGrantOperations.Read),
                    // A subject-grantee grant must not appear in the tenant-grant index.
                    CrossTenantGrant.Create("carol", TenantGranteeKind.Subject, "orders", TenantGrantOperations.ReadWrite),
                ]),
        ]);

        Assert.That(snapshot.TryGetTenant("acme", out var tenant), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(tenant!.TryGetTenantGrants("beta", out var betaGrants), Is.True);
            Assert.That(betaGrants!, Has.Length.EqualTo(1));
            Assert.That(betaGrants![0].Scope, Is.EqualTo("orders"));
            Assert.That(tenant.TryGetTenantGrants("carol", out _), Is.False);
        });
    }

    [Test]
    public void Compile_skips_the_uninitialised_tenant_id()
    {
        // A default-constructed record has a null tenant-id value and must be skipped.
        var snapshot = CompiledTenantPolicy.Compile([new TenantRecord()]);

        Assert.That(snapshot, Is.SameAs(CompiledTenantPolicy.Empty));
    }
}
