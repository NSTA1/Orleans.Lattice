using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Tenant confinement of role bindings (D4): a binding may name a cluster group or a tenant group of the
/// installing tenant, never another tenant's group or any other id in the reserved <c>t/</c> namespace.
/// </summary>
public sealed partial class AppRoleCompilerTests
{
    [Test]
    public void A_binding_to_the_installing_tenants_own_group_compiles_to_a_rule_on_that_tenants_app_tree()
    {
        var result = AppRoleCompiler.Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Acme, [Bind("writer", "t/acme/editors")], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.TenantMismatchBindings, Is.Empty);
        var rule = result.Rules.Single();
        Assert.That(rule.Subject, Is.EqualTo(LatticeSubjectSelector.Group("t/acme/editors")));
        Assert.That(rule.Scope, Is.EqualTo(LatticeScope.Tree("t/acme/a/notes/docs")));
        Assert.That(rule.RuleId, Does.StartWith(AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse(Slug))));
    }

    [Test]
    public void A_binding_to_another_tenants_group_fails_compilation_with_the_typed_mismatch_and_emits_no_rules()
    {
        var foreign = Bind("writer", "t/globex/editors");

        var result = AppRoleCompiler.Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs")), Role("reader", LatticeOperation.Read, TreeScope("docs"))),
            Acme,
            [Bind("reader", "g-a"), foreign],
            Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty, "no partially granted rule set, not even for the valid binding");
        Assert.That(result.TenantMismatchBindings, Is.EqualTo(new[] { foreign }));
        Assert.That(result.Excesses, Is.Empty);
        Assert.That(result.UnknownRoleBindings, Is.Empty);
    }

    [Test]
    public void A_cluster_group_binding_compiles_identically_whatever_the_tenant()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));

        var acme = AppRoleCompiler.Compile(manifest, Acme, [Bind("writer", "g-a")], Ceiling(ReadWrite));
        var off = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite));

        Assert.That(acme.Succeeded, Is.True);
        Assert.That(off.Succeeded, Is.True);
        Assert.That(acme.TenantMismatchBindings, Is.Empty);
        Assert.That(off.TenantMismatchBindings, Is.Empty);
        Assert.That(acme.Rules.Single().Subject, Is.EqualTo(LatticeSubjectSelector.Group("g-a")));
        Assert.That(off.Rules.Single().Subject, Is.EqualTo(LatticeSubjectSelector.Group("g-a")));
    }

    [Test]
    public void With_tenancy_off_every_tenant_group_binding_is_a_mismatch()
    {
        var binding = Bind("writer", "t/acme/editors");

        var result = AppRoleCompiler.Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"))), TenantId.Default, [binding], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.TenantMismatchBindings, Is.EqualTo(new[] { binding }));
    }

    [TestCase("t/default/editors")]
    [TestCase("t/acme/")]
    [TestCase("t/acme/Editors")]
    [TestCase("t/acme/a/b")]
    [TestCase("t/")]
    [TestCase("t/acme")]
    public void A_malformed_or_reserved_id_in_the_tenant_namespace_is_a_mismatch(string groupId)
    {
        var binding = Bind("writer", groupId);

        var result = AppRoleCompiler.Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Acme, [binding], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.TenantMismatchBindings, Is.EqualTo(new[] { binding }));
        Assert.That(result.Rules, Is.Empty);
    }

    [Test]
    public void A_mismatched_binding_to_an_undeclared_role_is_reported_in_both_lists()
    {
        var binding = Bind("ghost", "t/globex/editors");

        var result = AppRoleCompiler.Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Acme, [binding], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.TenantMismatchBindings, Is.EqualTo(new[] { binding }));
        Assert.That(result.UnknownRoleBindings, Is.EqualTo(new[] { binding }));
    }

    [Test]
    public void Mismatches_are_reported_alongside_ceiling_excesses_in_binding_order()
    {
        var first = Bind("writer", "t/globex/a");
        var second = Bind("reader", "t/initech/b");

        var result = AppRoleCompiler.Compile(
            Manifest(Role("writer", ReadWrite | LatticeOperation.Delete, TreeScope("docs")), Role("reader", LatticeOperation.Read, TreeScope("docs"))),
            Acme,
            [first, second],
            Ceiling(ReadWrite));

        Assert.That(result.Excesses.Single().Kind, Is.EqualTo(AppCeilingExcessKind.Operations));
        Assert.That(result.TenantMismatchBindings, Is.EqualTo(new[] { first, second }));
    }

    [Test]
    public void A_successful_compilation_reports_no_tenant_mismatches()
    {
        var result = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-a"));

        Assert.That(result.TenantMismatchBindings, Is.Empty);
    }

    [TestCase("g-a", "acme", true)]
    [TestCase("readers", "default", true)]
    [TestCase("tenant/acme/x", "acme", true)]
    [TestCase("T/acme/x", "acme", true)]
    [TestCase("t/acme/editors", "acme", true)]
    [TestCase("t/acme/x.y_z-1", "acme", true)]
    [TestCase("t/acme/editors", "globex", false)]
    [TestCase("t/acmex/editors", "acme", false)]
    [TestCase("t/acm/editors", "acme", false)]
    [TestCase("t/acme/editors", "default", false)]
    [TestCase("t/default/editors", "default", false)]
    [TestCase("t/default/editors", "acme", false)]
    [TestCase("t/acme/", "acme", false)]
    [TestCase("t/", "acme", false)]
    public void IsBindableGroup_admits_cluster_groups_and_the_installing_tenants_groups_only(string groupId, string tenant, bool expected)
    {
        var installTenant = tenant == TenantId.DefaultId ? TenantId.Default : TenantId.Parse(tenant);

        Assert.That(AppRoleCompiler.IsBindableGroup(installTenant, groupId), Is.EqualTo(expected));
    }

    [Test]
    public void IsBindableGroup_refuses_a_tenant_group_for_the_uninitialised_tenant()
    {
        Assert.That(AppRoleCompiler.IsBindableGroup(default, "t/acme/editors"), Is.False);
        Assert.That(AppRoleCompiler.IsBindableGroup(default, "g-a"), Is.True);
    }

    [Test]
    public void Rule_ids_for_a_tenant_group_binding_are_stable_and_distinct_from_a_cluster_group_binding()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));
        string IdFor(string group) => AppRoleCompiler.Compile(manifest, Acme, [Bind("writer", group)], Ceiling(ReadWrite)).Rules.Single().RuleId;

        Assert.That(IdFor("t/acme/editors"), Is.EqualTo(IdFor("t/acme/editors")));
        Assert.That(IdFor("t/acme/editors"), Is.Not.EqualTo(IdFor("editors")));
    }
}
