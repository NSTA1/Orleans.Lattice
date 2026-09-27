using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Named regression tests for the app tree namespace composition invariant (epic
/// #2235, item F2). An installed app's trees are named
/// <c>a/{app}/{tree}</c> (<see cref="LatticeConstants.AppTreePrefix"/>), and
/// tenancy must stay the <em>outer</em> axis: with tenancy off (the default
/// tenant) the name resolves bare, and under tenant <c>T</c> it composes to
/// <c>t/T/a/{app}/{tree}</c>.
/// </summary>
/// <remarks>
/// That behaviour holds only while <c>a/</c> is absent from the reserved /
/// already-qualified set in <see cref="LatticeTenantResolution"/>. Adding it there
/// would pass every app tree through uncomposed and collapse every tenant's copy
/// of an app onto one shared keyspace, with no other test failing. These tests
/// fail the moment that happens.
/// </remarks>
[TestFixture]
public sealed class LatticeTenantResolutionAppTreePrefixNotReservedTests
{
    private const string AppTree = "a/crm/contacts";
    private static readonly TenantId Contoso = TenantId.Parse("contoso");

    [Test]
    public void AppTreePrefix_is_the_a_slash_segment()
    {
        Assert.That(LatticeConstants.AppTreePrefix, Is.EqualTo("a/"));
        Assert.That(AppTree, Does.StartWith(LatticeConstants.AppTreePrefix));
    }

    [Test]
    public void AppTreePrefix_does_not_overlap_any_reserved_or_qualified_prefix()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeConstants.AppTreePrefix, Does.Not.StartWith(LatticeTenantTrees.SegmentPrefix));
            Assert.That(LatticeConstants.AppTreePrefix, Does.Not.StartWith(LatticeConstants.SystemTreePrefix));
            Assert.That(LatticeConstants.AppTreePrefix, Does.Not.StartWith(LatticeConstants.SystemDataTreePrefix));
            Assert.That(LatticeTenantTrees.IsTenantScoped(AppTree), Is.False);
        });
    }

    [Test]
    public void AppTreePrefix_is_not_reserved_so_a_confined_tenant_composes_an_app_tree()
    {
        // A reserved or already-qualified name is returned uncomposed. An app tree
        // must instead fall through to composition, and must not be refused as a
        // namespace escape the way a confined tenant's 'sys-' name is.
        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Contoso, AppTree);

        Assert.That(effective, Is.Not.EqualTo(AppTree));
        Assert.That(effective, Does.StartWith(LatticeTenantTrees.SegmentPrefix));
    }

    [Test]
    public void AppTreePrefix_composition_with_the_default_tenant_returns_the_bare_app_tree_unchanged()
    {
        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Default, AppTree);

        Assert.That(effective, Is.SameAs(AppTree));
    }

    [Test]
    public void AppTreePrefix_composition_with_a_tenant_yields_the_tenant_scoped_app_tree()
    {
        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Contoso, AppTree);

        Assert.That(effective, Is.EqualTo("t/contoso/a/crm/contacts"));
    }

    [Test]
    public void AppTreePrefix_composition_keeps_tenant_as_the_outer_axis()
    {
        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Contoso, AppTree);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantTrees.TryGetTenant(effective, out var owner), Is.True);
            Assert.That(owner, Is.EqualTo(Contoso));
            Assert.That(LatticeTenantTrees.LocalName(effective), Is.EqualTo(AppTree));
        });
    }

    [Test]
    public void AppTreePrefix_composition_gives_two_tenants_distinct_trees_for_the_same_app_tree()
    {
        var contoso = LatticeTenantResolution.ComposeEffectiveTreeId(Contoso, AppTree);
        var fabrikam = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("fabrikam"), AppTree);

        Assert.That(contoso, Is.Not.EqualTo(fabrikam));
    }

    [Test]
    public void AppTreePrefix_composition_does_not_double_compose_an_already_tenant_scoped_app_tree()
    {
        const string composed = "t/contoso/a/crm/contacts";

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Contoso, composed);

        Assert.That(effective, Is.SameAs(composed));
    }

    [Test]
    public void AppTreePrefix_composition_does_not_double_compose_a_tenant_scoped_app_tree_under_the_default_tenant()
    {
        const string composed = "t/contoso/a/crm/contacts";

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Default, composed);

        Assert.That(effective, Is.SameAs(composed));
    }

    [Test]
    public void AppTreePrefix_composition_with_no_tenant_value_throws_access_denied()
    {
        Assert.That(
            () => LatticeTenantResolution.ComposeEffectiveTreeId(default, AppTree),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public async Task AppTreePrefix_composition_through_the_resolver_seam_matches_direct_composition()
    {
        var tenancyOff = new FakeTenantContextResolver(TenantId.Default);
        var tenancyOn = new FakeTenantContextResolver(Contoso, resolvesSynchronously: false);

        var bare = await LatticeTenantResolution.ResolveEffectiveTreeIdAsync(tenancyOff, AppTree);
        var scoped = await LatticeTenantResolution.ResolveEffectiveTreeIdAsync(tenancyOn, AppTree);

        Assert.Multiple(() =>
        {
            Assert.That(bare, Is.SameAs(AppTree));
            Assert.That(scoped, Is.EqualTo("t/contoso/a/crm/contacts"));
        });
    }
}
