namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantEnumerationFilter"/>, the seam that prunes a
/// tree-id enumeration to the trees the active tenant owns. A filter that
/// silently passes everything through is what let a tenant caller enumerate every
/// other tenant's tree ids from the cluster-state catalog, so the "returns
/// everything" case is pinned explicitly as a failure rather than left implicit.
/// </summary>
[TestFixture]
public sealed class TenantEnumerationFilterTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static readonly string[] Mixed =
    [
        "t/acme/orders",
        "t/acme/customers",
        "t/beta/orders",
        "t/beta/secrets",
        "legacy-tree",
        "_lattice_trees",
        "sys-auth-policy",
    ];

    [Test]
    public void IsActive_is_true_so_choke_points_consult_it()
    {
        Assert.That(new TenantEnumerationFilter().IsActive, Is.True);
    }

    [Test]
    public void Filter_keeps_only_the_tenants_own_trees()
    {
        var filtered = new TenantEnumerationFilter().Filter(Acme, Mixed);

        Assert.Multiple(() =>
        {
            Assert.That(filtered, Does.Contain("t/acme/orders"));
            Assert.That(filtered, Does.Contain("t/acme/customers"));
            Assert.That(filtered, Does.Not.Contain("t/beta/orders"),
                "another tenant's tree id must never appear in this tenant's enumeration");
            Assert.That(filtered, Does.Not.Contain("t/beta/secrets"));
        });
    }

    [Test]
    public void Filter_excludes_a_bare_legacy_tree_from_a_non_default_tenant()
    {
        // A bare id is adopted by the reserved default tenant, so it is not acme's.
        Assert.That(new TenantEnumerationFilter().Filter(Acme, Mixed), Does.Not.Contain("legacy-tree"));
    }

    [Test]
    public void Filter_keeps_a_bare_legacy_tree_for_the_default_tenant()
    {
        var filtered = new TenantEnumerationFilter().Filter(TenantId.Default, Mixed);

        Assert.Multiple(() =>
        {
            Assert.That(filtered, Does.Contain("legacy-tree"));
            Assert.That(filtered, Does.Not.Contain("t/acme/orders"),
                "the default tenant owns the bare namespace, not every tenant's namespace");
        });
    }

    [Test]
    public void Filter_keeps_platform_owned_trees()
    {
        // Platform trees are not tenant data; the catalog's own system-tree switch
        // and the per-entry authorization check govern them, so pruning them here
        // would change what an operator sees for reasons unrelated to tenancy.
        var filtered = new TenantEnumerationFilter().Filter(Acme, Mixed);

        Assert.Multiple(() =>
        {
            Assert.That(filtered, Does.Contain("_lattice_trees"));
            Assert.That(filtered, Does.Contain("sys-auth-policy"));
        });
    }

    [Test]
    public void Filter_with_the_no_tenant_value_yields_no_tenant_trees()
    {
        var filtered = new TenantEnumerationFilter().Filter(default, Mixed);

        Assert.Multiple(() =>
        {
            Assert.That(filtered, Does.Not.Contain("t/acme/orders"));
            Assert.That(filtered, Does.Not.Contain("t/beta/orders"));
            Assert.That(filtered, Does.Not.Contain("legacy-tree"));
            Assert.That(filtered, Does.Contain("sys-auth-policy"), "platform ids are still governed elsewhere");
        });
    }

    [Test]
    public void Filter_never_returns_the_input_unchanged_for_a_mixed_enumeration()
    {
        var filtered = new TenantEnumerationFilter().Filter(Beta, Mixed);

        Assert.That(filtered.Count, Is.LessThan(Mixed.Length),
            "a pass-through filter is the exact defect this seam exists to prevent");
    }

    [Test]
    public void Filter_does_not_mutate_the_supplied_list()
    {
        var input = new List<string>(Mixed);

        new TenantEnumerationFilter().Filter(Acme, input);

        Assert.That(input, Has.Count.EqualTo(Mixed.Length));
    }

    [Test]
    public void Filter_rejects_a_null_enumeration()
    {
        Assert.That(() => new TenantEnumerationFilter().Filter(Acme, null!), Throws.ArgumentNullException);
    }
}
