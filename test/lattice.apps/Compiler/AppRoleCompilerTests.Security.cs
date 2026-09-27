using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppRoleCompilerTests
{
    // The compiler is the single seam every app grant funnels through, so it must refuse a
    // grant outside ordinary data trees whatever the manifest or the ceiling says. These
    // manifests bypass the validator on purpose: the guard must not depend on it.
    private static AppManifest UnvalidatedAdoption(string adoptedTreeId, LatticeOperation operations) => new()
    {
        Identity = new() { Slug = AppSlug.Parse(Slug), Version = AppVersion.Parse("1.0.0") },
        Trees = [Tree("docs", adoptedTreeId)],
        Roles = [Role("reader", operations, TreeScope("docs"))],
        Subscriptions = [],
        McpTools = [],
    };

    [TestCase("*")]
    [TestCase("sys-app-registry")]
    [TestCase("sys-auth-policy")]
    [TestCase("_lattice_trees")]
    [TestCase("t/acme/contacts")]
    [TestCase("t/x")]
    public void Compile_refuses_an_adopted_non_data_tree_even_when_an_exception_approves_it(string adoptedTreeId)
    {
        var manifest = UnvalidatedAdoption(adoptedTreeId, LatticeOperation.Read);
        var ceiling = Ceiling(LatticeOperation.Read, LatticeScope.Tree(adoptedTreeId));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Default, [Bind("reader", "g")], ceiling);

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty);
        Assert.That(result.Excesses.Single().Kind, Is.EqualTo(AppCeilingExcessKind.Scope));
    }

    [Test]
    public void Compile_refuses_an_adopted_tenant_qualified_tree_under_a_non_default_tenant()
    {
        var manifest = UnvalidatedAdoption("t/other/contacts", LatticeOperation.Read);
        var ceiling = Ceiling(LatticeOperation.Read, LatticeScope.Tree("t/other/contacts"));

        var result = AppRoleCompiler.Compile(manifest, TenantId.Parse("acme"), [Bind("reader", "g")], ceiling);

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty);
    }

    [TestCase(LatticeOperation.AppInstall)]
    [TestCase(LatticeOperation.Telemetry)]
    public void Compile_never_emits_a_scopeless_capability_even_when_the_ceiling_allows_it(LatticeOperation capability)
    {
        var manifest = new AppManifest
        {
            Identity = new() { Slug = AppSlug.Parse(Slug), Version = AppVersion.Parse("1.0.0") },
            Trees = [Tree("docs")],
            Roles = [Role("reader", LatticeOperation.Read | capability, TreeScope("docs"))],
            Subscriptions = [],
            McpTools = [],
        };

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("reader", "g")], Ceiling(LatticeOperation.Read | capability));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty);
        var excess = result.Excesses.Single();
        Assert.That(excess.Kind, Is.EqualTo(AppCeilingExcessKind.Operations));
        Assert.That(excess.Operations, Is.EqualTo(capability));
    }

    [Test]
    public void Compile_ignores_an_exception_naming_the_cluster_wide_sentinel_for_a_structural_scope()
    {
        // Structural own-namespace scopes never consult exceptions, so a "*" exception is inert.
        var manifest = Manifest([Tree("docs")], Role("reader", LatticeOperation.Read, TreeScope("docs")));

        var result = AppRoleCompiler.Compile(
            manifest, TenantId.Default, [Bind("reader", "g")], Ceiling(LatticeOperation.Read, LatticeScope.ClusterWide()));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules.Select(r => r.Scope.TreeId), Is.EqualTo(new[] { "a/notes/docs" }));
    }
}
