using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppRoleCompilerTests
{
    [Test]
    public void Compile_fails_an_over_requesting_role_with_the_excess_operation_bits_and_no_rules()
    {
        var manifest = Manifest(Role("writer", ReadWrite | LatticeOperation.Admin, TreeScope("docs")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty);
        Assert.That(result.Excesses, Is.EqualTo(new[]
        {
            new AppCeilingExcess("writer", AppCeilingExcessKind.Operations, LatticeOperation.Admin, null),
        }));
    }

    [Test]
    public void Compile_lists_every_excess_across_roles_in_manifest_order_rather_than_clamping()
    {
        var manifest = Manifest(
            [Tree("docs", adopted: "legacy-docs"), Tree("audit")],
            Role("writer", ReadWrite | LatticeOperation.Delete, TreeScope("docs"), TreeScope("audit")),
            Role("peer", LatticeOperation.Read | LatticeOperation.Backup, TreeScope("items", app: "other")),
            Role("reader", LatticeOperation.Read, TreeScope("audit")));

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("writer", "g-a"), Bind("peer", "g-b"), Bind("reader", "g-c")], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty);
        Assert.That(result.Excesses, Is.EqualTo(new[]
        {
            new AppCeilingExcess("writer", AppCeilingExcessKind.Operations, LatticeOperation.Delete, null),
            new AppCeilingExcess("writer", AppCeilingExcessKind.Scope, ReadWrite | LatticeOperation.Delete, LatticeScope.Tree("legacy-docs")),
            new AppCeilingExcess("peer", AppCeilingExcessKind.Operations, LatticeOperation.Backup, null),
            new AppCeilingExcess("peer", AppCeilingExcessKind.Scope, LatticeOperation.Read | LatticeOperation.Backup, LatticeScope.Tree("a/other/items")),
        }));
    }

    [Test]
    public void Compile_checks_an_unbound_role_against_the_ceiling()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")), Role("admin", LatticeOperation.Admin, TreeScope("docs")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty);
        Assert.That(result.Excesses.Single().RoleName, Is.EqualTo("admin"));
        Assert.That(result.UnboundRoles, Is.EqualTo(new[] { "admin" }));
    }

    [Test]
    public void Compile_accepts_structural_own_namespace_scopes_without_any_exception()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs"), TreeScope("audit", app: Slug), PrefixScope("docs", "p/")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules.Select(r => r.Scope.TreeId).Distinct(), Is.EquivalentTo(new[] { "a/notes/docs", "a/notes/audit" }));
    }

    [Test]
    public void Compile_fails_an_adopted_tree_without_an_approved_exception()
    {
        var manifest = Manifest([Tree("docs", adopted: "legacy-docs")], Role("writer", ReadWrite, TreeScope("docs")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Excesses, Is.EqualTo(new[]
        {
            new AppCeilingExcess("writer", AppCeilingExcessKind.Scope, ReadWrite, LatticeScope.Tree("legacy-docs")),
        }));
    }

    [Test]
    public void Compile_grants_an_adopted_tree_covered_by_an_approved_exception()
    {
        var manifest = Manifest([Tree("docs", adopted: "legacy-docs")], Role("writer", ReadWrite, TreeScope("docs"), KeyScope("docs", "k")));

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite, LatticeScope.Tree("legacy-docs")));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules.Select(r => r.Scope), Is.EquivalentTo(new[] { LatticeScope.Tree("legacy-docs"), LatticeScope.Key("legacy-docs", "k") }));
    }

    [Test]
    public void Compile_fails_another_apps_tree_without_an_approved_exception()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, PrefixScope("items", "shared/", app: "other")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Excesses, Is.EqualTo(new[]
        {
            new AppCeilingExcess("peer", AppCeilingExcessKind.Scope, LatticeOperation.Read, LatticeScope.Prefix("a/other/items", "shared/")),
        }));
    }

    [Test]
    public void Compile_grants_another_apps_tree_covered_by_an_approved_exception()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, PrefixScope("items", "shared/", app: "other")));

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, LatticeScope.Prefix("a/other/items", "shared/")));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules.Single().Scope, Is.EqualTo(LatticeScope.Prefix("a/other/items", "shared/")));
    }

    [TestCase(LatticeScopeKind.Tree, null, LatticeScopeKind.Tree, null, true)]
    [TestCase(LatticeScopeKind.Tree, null, LatticeScopeKind.Key, "k", true)]
    [TestCase(LatticeScopeKind.Tree, null, LatticeScopeKind.Prefix, "p/", true)]
    [TestCase(LatticeScopeKind.Prefix, "p/", LatticeScopeKind.Prefix, "p/", true)]
    [TestCase(LatticeScopeKind.Prefix, "p/", LatticeScopeKind.Prefix, "p/deeper/", true)]
    [TestCase(LatticeScopeKind.Prefix, "p/", LatticeScopeKind.Key, "p/k", true)]
    [TestCase(LatticeScopeKind.Prefix, "p/", LatticeScopeKind.Tree, null, false)]
    [TestCase(LatticeScopeKind.Prefix, "p/", LatticeScopeKind.Prefix, "q/", false)]
    [TestCase(LatticeScopeKind.Prefix, "p/deeper/", LatticeScopeKind.Prefix, "p/", false)]
    [TestCase(LatticeScopeKind.Key, "k", LatticeScopeKind.Key, "k", true)]
    [TestCase(LatticeScopeKind.Key, "k", LatticeScopeKind.Key, "k2", false)]
    [TestCase(LatticeScopeKind.Key, "k", LatticeScopeKind.Prefix, "k", false)]
    [TestCase(LatticeScopeKind.Key, "k", LatticeScopeKind.Tree, null, false)]
    public void An_exception_covers_a_requested_scope_on_the_same_tree_that_it_contains(
        LatticeScopeKind exceptionKind, string? exceptionKey, LatticeScopeKind requestedKind, string? requestedKey, bool covered)
    {
        var template = new AppScopeTemplate { Tree = "items", App = AppSlug.Parse("other"), Kind = requestedKind, KeyOrPrefix = requestedKey };
        var manifest = Manifest(Role("peer", LatticeOperation.Read, template));
        var exception = new LatticeScope(exceptionKind, "a/other/items", exceptionKey);

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, exception));

        Assert.That(result.Succeeded, Is.EqualTo(covered));
        Assert.That(result.Rules, Has.Count.EqualTo(covered ? 1 : 0));
    }

    [TestCase("a/other/other-items")]
    [TestCase("a/other")]
    [TestCase("A/OTHER/ITEMS")]
    [TestCase(LatticeScope.ClusterWideTreeId)]
    public void An_exception_on_a_different_tree_or_the_cluster_wide_sentinel_covers_nothing(string exceptionTree)
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, LatticeScope.Tree(exceptionTree)));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Excesses.Single().Kind, Is.EqualTo(AppCeilingExcessKind.Scope));
    }

    [Test]
    public void Compile_reports_a_repeated_uncovered_scope_once_per_role()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other"), TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read));

        Assert.That(result.Excesses, Has.Count.EqualTo(1));
    }

    [Test]
    public void Compile_ignores_null_exception_scope_entries()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, null!, LatticeScope.Tree("a/other/items")));

        Assert.That(result.Succeeded, Is.True);
    }
}
