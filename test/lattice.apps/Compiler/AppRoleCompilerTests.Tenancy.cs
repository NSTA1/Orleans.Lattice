using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppRoleCompilerTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void Tenancy_off_keeps_the_bare_structural_app_tree()
    {
        var result = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-a"));

        Assert.That(result.Rules.Single().Scope, Is.EqualTo(LatticeScope.Tree("a/notes/docs")));
    }

    [Test]
    public void Tenancy_on_composes_the_tenant_as_the_outer_axis_of_every_resolved_tree()
    {
        var manifest = Manifest(
            [Tree("docs"), Tree("audit", adopted: "legacy-audit")],
            Role("writer", ReadWrite, TreeScope("docs"), PrefixScope("audit", "log/"), KeyScope("docs", "k")),
            Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));
        var ceiling = Ceiling(ReadWrite, LatticeScope.Tree("legacy-audit"), LatticeScope.Tree("a/other/items"));

        var result = AppRoleCompiler.Compile(manifest, Acme, [Bind("writer", "g-a"), Bind("peer", "g-a")], ceiling);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules.Select(r => r.Scope), Is.EquivalentTo(new[]
        {
            LatticeScope.Tree("t/acme/a/notes/docs"),
            LatticeScope.Prefix("t/acme/legacy-audit", "log/"),
            LatticeScope.Key("t/acme/a/notes/docs", "k"),
            LatticeScope.Tree("t/acme/a/other/items"),
        }));
    }

    [Test]
    public void Tenancy_on_reports_an_uncovered_scope_in_the_tenant_local_vocabulary()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(manifest, Acme, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read));

        Assert.That(result.Excesses.Single().Scope, Is.EqualTo(LatticeScope.Tree("a/other/items")));
    }

    [Test]
    public void A_tenant_qualified_exception_scope_never_matches()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(
            manifest, Acme, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, LatticeScope.Tree("t/acme/a/other/items")));

        Assert.That(result.Succeeded, Is.False);
    }

    [Test]
    public void Rule_ids_differ_per_tenant_and_are_stable_within_a_tenant()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));
        var ceiling = Ceiling(ReadWrite);
        string IdFor(TenantId tenant) => AppRoleCompiler.Compile(manifest, tenant, [Bind("writer", "g-a")], ceiling).Rules.Single().RuleId;

        Assert.That(IdFor(Acme), Is.EqualTo(IdFor(Acme)));
        Assert.That(IdFor(Acme), Is.Not.EqualTo(IdFor(TenantId.Default)));
        Assert.That(IdFor(Acme), Is.Not.EqualTo(IdFor(TenantId.Parse("globex"))));
    }

    [Test]
    public void A_parsed_default_tenant_compiles_identically_to_tenancy_off()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));

        var parsed = AppRoleCompiler.Compile(manifest, TenantId.Parse(TenantId.DefaultId), [Bind("writer", "g-a")], Ceiling(ReadWrite));
        var off = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite));

        Assert.That(parsed.Rules, Is.EqualTo(off.Rules));
    }
}
