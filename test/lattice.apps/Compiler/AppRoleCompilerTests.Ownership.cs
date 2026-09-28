using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppRoleCompilerTests
{
    private static readonly LatticeScope OtherItemsException = LatticeScope.Tree("a/other/items");

    [Test]
    public void Compile_refuses_a_covered_cross_app_scope_when_no_owner_snapshot_is_supplied()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, OtherItemsException));

        Assert.That(result.Succeeded, Is.False, "the named app is not an installed owner, so the scope fails closed");
        Assert.That(result.Excesses.Single(), Is.EqualTo(
            new AppCeilingExcess("peer", AppCeilingExcessKind.Scope, LatticeOperation.Read, OtherItemsException)));
        Assert.That(result.Rules, Is.Empty);
    }

    [Test]
    public void Compile_refuses_a_cross_app_scope_whose_tree_is_owned_by_a_different_app()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));
        var owners = AppTreeOwnerSnapshot.Create([new("a/other/items", AppSlug.Parse("squatter"))]);

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, OtherItemsException), owners);

        Assert.That(result.Succeeded, Is.False);
    }

    [Test]
    public void Compile_refuses_a_cross_app_scope_whose_owner_is_installed_only_in_another_tenant()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(manifest, Acme, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, OtherItemsException), OtherOwnsItems);

        Assert.That(result.Succeeded, Is.False, "the snapshot is keyed by the tenant-composed id, so the default tenant's owner does not count in acme");
    }

    [Test]
    public void Compile_grants_a_cross_app_scope_whose_tree_the_named_app_owns()
    {
        var manifest = Manifest(Role("peer", LatticeOperation.Read, TreeScope("items", app: "other")));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("peer", "g-a")], Ceiling(LatticeOperation.Read, OtherItemsException), OtherOwnsItems);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules.Single().Scope, Is.EqualTo(OtherItemsException));
    }

    [Test]
    public void Owner_snapshot_does_not_affect_structural_or_adopted_scopes()
    {
        var manifest = Manifest(
            [Tree("docs"), Tree("audit", adopted: "legacy-audit")],
            Role("writer", ReadWrite, TreeScope("docs"), TreeScope("audit")));

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("writer", "g-a")], Ceiling(ReadWrite, LatticeScope.Tree("legacy-audit")), AppTreeOwnerSnapshot.None);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules, Has.Count.EqualTo(2));
    }
}
