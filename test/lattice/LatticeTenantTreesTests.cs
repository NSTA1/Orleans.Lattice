namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantTrees"/>: the reserved <c>t/</c>
/// segment prefix, composing a segmented tree id from a tenant and an
/// unqualified name, the tenant-scoped prefix check, and parsing the owning
/// tenant back out (including round-trip compose/parse).
/// </summary>
[TestFixture]
public sealed class LatticeTenantTreesTests
{
    [Test]
    public void SegmentPrefix_is_the_reserved_tenant_namespace()
    {
        Assert.That(LatticeTenantTrees.SegmentPrefix, Is.EqualTo("t/"));
    }

    [Test]
    public void Compose_builds_the_segmented_tree_id()
    {
        var id = LatticeTenantTrees.Compose(TenantId.Parse("contoso"), "orders");

        Assert.That(id, Is.EqualTo("t/contoso/orders"));
    }

    [Test]
    public void Compose_preserves_slashes_in_the_local_name()
    {
        var id = LatticeTenantTrees.Compose(TenantId.Parse("contoso"), "orders/eu");

        Assert.That(id, Is.EqualTo("t/contoso/orders/eu"));
    }

    [Test]
    public void Compose_no_tenant_value_throws_argument()
    {
        Assert.That(
            () => LatticeTenantTrees.Compose(default, "orders"),
            Throws.ArgumentException);
    }

    [TestCase(null)]
    [TestCase("")]
    public void Compose_null_or_empty_name_throws_argument(string? name)
    {
        Assert.That(
            () => LatticeTenantTrees.Compose(TenantId.Parse("contoso"), name!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void IsTenantScoped_is_true_for_a_tenant_tree_id()
    {
        Assert.That(LatticeTenantTrees.IsTenantScoped("t/contoso/orders"), Is.True);
    }

    [TestCase("orders")]
    [TestCase("_lattice_trees")]
    [TestCase("sys-auth-users")]
    [TestCase("view-orders")]
    [TestCase("t")]
    public void IsTenantScoped_is_false_for_a_non_tenant_tree_id(string treeId)
    {
        Assert.That(LatticeTenantTrees.IsTenantScoped(treeId), Is.False);
    }

    [Test]
    public void IsTenantScoped_null_throws_argument_null()
    {
        Assert.That(() => LatticeTenantTrees.IsTenantScoped(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetTenant_extracts_the_owning_tenant()
    {
        var ok = LatticeTenantTrees.TryGetTenant("t/contoso/orders", out var tenant);

        Assert.That(ok, Is.True);
        Assert.That(tenant, Is.EqualTo(TenantId.Parse("contoso")));
    }

    [Test]
    public void TryGetTenant_ignores_slashes_after_the_tenant_segment()
    {
        var ok = LatticeTenantTrees.TryGetTenant("t/contoso/orders/eu", out var tenant);

        Assert.That(ok, Is.True);
        Assert.That(tenant.Value, Is.EqualTo("contoso"));
    }

    [TestCase("orders", TestName = "non_tenant")]
    [TestCase("t/contoso", TestName = "missing_name_segment")]
    [TestCase("t/contoso/", TestName = "empty_name_segment")]
    [TestCase("t//orders", TestName = "empty_tenant_segment")]
    [TestCase("t/-bad/orders", TestName = "invalid_tenant_grammar")]
    [TestCase("t/CONTOSO/orders", TestName = "uppercase_tenant")]
    public void TryGetTenant_returns_false_for_a_malformed_id(string treeId)
    {
        var ok = LatticeTenantTrees.TryGetTenant(treeId, out var tenant);

        Assert.That(ok, Is.False);
        Assert.That(tenant, Is.EqualTo(default(TenantId)));
    }

    [Test]
    public void TryGetTenant_null_throws_argument_null()
    {
        Assert.That(() => LatticeTenantTrees.TryGetTenant(null!, out _), Throws.ArgumentNullException);
    }

    [TestCase("contoso", "orders")]
    [TestCase("t0", "a")]
    [TestCase("default", "legacy")]
    public void Compose_then_TryGetTenant_round_trips_the_tenant(string tenantId, string name)
    {
        var tenant = TenantId.Parse(tenantId);

        var composed = LatticeTenantTrees.Compose(tenant, name);
        var ok = LatticeTenantTrees.TryGetTenant(composed, out var parsed);

        Assert.That(ok, Is.True);
        Assert.That(parsed, Is.EqualTo(tenant));
        Assert.That(LatticeTenantTrees.IsTenantScoped(composed), Is.True);
    }

    [Test]
    public void GetOwner_null_throws_argument_null()
    {
        Assert.That(() => LatticeTenantTrees.GetOwner(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void GetOwner_tenant_scoped_id_is_owned_by_that_tenant()
    {
        var ownership = LatticeTenantTrees.GetOwner("t/contoso/orders");

        Assert.That(ownership.IsTenantOwned, Is.True);
        Assert.That(ownership.Tenant, Is.EqualTo(TenantId.Parse("contoso")));
    }

    [Test]
    public void GetOwner_bare_legacy_id_is_adopted_by_the_default_tenant()
    {
        var ownership = LatticeTenantTrees.GetOwner("orders");

        Assert.That(ownership.IsTenantOwned, Is.True);
        Assert.That(ownership.Tenant, Is.EqualTo(TenantId.Default));
    }

    [TestCase("view-orders", TestName = "GetOwner_view_tree_is_adopted_by_the_default_tenant")]
    [TestCase("tag-orders", TestName = "GetOwner_tag_index_tree_is_adopted_by_the_default_tenant")]
    public void GetOwner_bare_derived_tree_is_adopted_by_the_default_tenant(string treeId)
    {
        var ownership = LatticeTenantTrees.GetOwner(treeId);

        Assert.That(ownership.IsTenantOwned, Is.True);
        Assert.That(ownership.Tenant, Is.EqualTo(TenantId.Default));
    }

    [TestCase("_lattice_trees", TestName = "GetOwner_system_internal_tree_is_platform_owned")]
    [TestCase("_lattice_replog_orders", TestName = "GetOwner_replog_tree_is_platform_owned")]
    [TestCase("sys-auth-users", TestName = "GetOwner_system_data_tree_is_platform_owned")]
    [TestCase("sys-membership-nodes", TestName = "GetOwner_membership_tree_is_platform_owned")]
    public void GetOwner_system_id_is_platform_owned(string treeId)
    {
        var ownership = LatticeTenantTrees.GetOwner(treeId);

        Assert.That(ownership.IsPlatformOwned, Is.True);
        Assert.That(ownership.Tenant, Is.EqualTo(default(TenantId)));
    }

    [TestCase("t/contoso", TestName = "GetOwner_missing_name_segment_is_platform_owned")]
    [TestCase("t/contoso/", TestName = "GetOwner_empty_name_segment_is_platform_owned")]
    [TestCase("t//orders", TestName = "GetOwner_empty_tenant_segment_is_platform_owned")]
    [TestCase("t/-bad/orders", TestName = "GetOwner_invalid_tenant_grammar_is_platform_owned")]
    [TestCase("t/CONTOSO/orders", TestName = "GetOwner_uppercase_tenant_is_platform_owned")]
    public void GetOwner_malformed_tenant_id_is_platform_owned(string treeId)
    {
        // A malformed id in the reserved t/ namespace is not a bare legacy id, so
        // it is not adopted by the default tenant; it is uncreatable and must
        // never leak into any tenant's view.
        var ownership = LatticeTenantTrees.GetOwner(treeId);

        Assert.That(ownership.IsPlatformOwned, Is.True);
    }

    [Test]
    public void GetOwner_bare_id_that_merely_embeds_t_slash_is_default_tenant()
    {
        // Only a leading "t/" is the reserved prefix; an embedded one is a normal
        // bare id and is adopted by the default tenant.
        var ownership = LatticeTenantTrees.GetOwner("orders/t/eu");

        Assert.That(ownership.IsTenantOwned, Is.True);
        Assert.That(ownership.Tenant, Is.EqualTo(TenantId.Default));
    }

    // ----- LocalName: the seam that keeps classification working after composition -----

    [TestCase("t/acme/orders", "orders")]
    [TestCase("t/acme/view-orders", "view-orders")]
    [TestCase("t/acme/tag-bytag", "tag-bytag")]
    [TestCase("t/globex/a/b/c", "a/b/c")]
    public void LocalName_strips_the_tenant_segment(string treeId, string expected)
        => Assert.That(LatticeTenantTrees.LocalName(treeId), Is.EqualTo(expected));

    [TestCase("orders")]
    [TestCase("view-orders")]
    [TestCase("sys-auth-policy")]
    [TestCase("_lattice_registry")]
    [TestCase("orders/t/eu")]
    public void LocalName_returns_a_non_tenant_id_unchanged(string treeId)
        => Assert.That(LatticeTenantTrees.LocalName(treeId), Is.SameAs(treeId));

    [TestCase("t/")]
    [TestCase("t//orders")]
    [TestCase("t/acme")]
    [TestCase("t/acme/")]
    [TestCase("t/NOT VALID/orders")]
    public void LocalName_returns_a_malformed_tenant_id_unchanged(string treeId)
        => Assert.That(
            LatticeTenantTrees.LocalName(treeId),
            Is.SameAs(treeId),
            "a malformed t/ id names no tenant, so there is no local name to expose");

    [Test]
    public void LocalName_is_consistent_with_Compose()
    {
        var composed = LatticeTenantTrees.Compose(TenantId.Parse("acme"), "view-orders");

        Assert.That(LatticeTenantTrees.LocalName(composed), Is.EqualTo("view-orders"));
    }

    [Test]
    public void LocalName_null_throws_argument_null()
        => Assert.That(() => LatticeTenantTrees.LocalName((string)null!), Throws.ArgumentNullException);

    // ----- Allocation behaviour: these run on every read and write -----

    [Test]
    public void LocalName_span_overload_slices_without_copying()
    {
        // The classification paths (is this a view / a tag index / reserved) run
        // on every data-plane operation, so they must slice the local name rather
        // than materialise it. Proven structurally: the returned span overlaps the
        // input's own memory, which a copy could not.
        const string composed = "t/acme/view-orders";
        var local = LatticeTenantTrees.LocalName(composed.AsSpan());

        Assert.That(local.ToString(), Is.EqualTo("view-orders"));
        Assert.That(
            composed.AsSpan().Overlaps(local),
            Is.True,
            "the local name must be a slice of the input, not a copy");
    }

    [Test]
    public void LocalName_returns_the_same_reference_for_a_non_tenant_id()
    {
        // Every id on a tenancy-off cluster takes this path, so it must not copy.
        const string bare = "orders";

        Assert.That(LatticeTenantTrees.LocalName(bare), Is.SameAs(bare));
    }

    [Test]
    public void Compose_accepts_a_span_so_a_slice_needs_no_intermediate_string()
    {
        var composed = "t/acme/view-orders".AsSpan();
        var local = LatticeTenantTrees.LocalName(composed);

        Assert.That(
            LatticeTenantTrees.Compose(TenantId.Parse("globex"), local),
            Is.EqualTo("t/globex/view-orders"));
    }

    [Test]
    public void Compose_span_rejects_an_empty_name()
        => Assert.That(
            () => LatticeTenantTrees.Compose(TenantId.Parse("acme"), ReadOnlySpan<char>.Empty),
            Throws.ArgumentException);
}
