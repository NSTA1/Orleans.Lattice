using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Pins the tool gate's scope resolution to the role compiler's: for the same manifest and
/// tenant, every resolved scope equals the scope of the rule the compiler emits.
/// </summary>
[TestFixture]
public sealed class AppRoleScopeResolverTests
{
    private static readonly AppSlug Notes = AppSlug.Parse("notes");
    private static readonly AppSlug Tasks = AppSlug.Parse("tasks");

    private static AppManifest MixedManifest() => AppMcpTestData.Manifest(
        Notes,
        AppMcpTestData.V1,
        [
            AppMcpTestData.Role(
                "mixed",
                LatticeOperation.Read,
                AppMcpTestData.TreeScope("notes"),
                AppMcpTestData.TreeScope("board", Tasks),
                AppMcpTestData.TreeScope("legacy"),
                new AppScopeTemplate { Tree = "notes", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "p/" }),
        ],
        [],
        [new AppTreeDeclaration { Name = "notes" }, new AppTreeDeclaration { Name = "legacy", AdoptedTreeId = "old-notes" }]);

    [TestCase("default")]
    [TestCase("acme")]
    public void Resolve_matches_the_role_compiler_for_structural_other_app_adopted_and_prefix_scopes(string tenantId)
    {
        var tenant = TenantId.Parse(tenantId);
        var manifest = MixedManifest();
        var ceiling = new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.Read,
            ApprovedExceptionScopes = [LatticeScope.Tree("a/tasks/board"), LatticeScope.Tree("old-notes")],
        };
        var owners = AppTreeOwnerSnapshot.Create([
            new(tenant.IsDefault ? "a/tasks/board" : $"t/{tenantId}/a/tasks/board", Tasks),
        ]);
        var compiled = AppRoleCompiler.Compile(manifest, tenant, [AppRoleBinding.Create("mixed", "g")], ceiling, owners);
        Assert.That(compiled.Succeeded, Is.True);

        var resolved = AppRoleScopeResolver.Resolve(Notes, manifest.Roles[0], manifest.Trees, tenant);

        Assert.That(resolved, Is.EquivalentTo(compiled.Rules.Select(r => r.Scope)));
    }

    [Test]
    public void Resolve_composes_a_non_default_tenant_into_the_tree_id()
    {
        var manifest = MixedManifest();

        var resolved = AppRoleScopeResolver.Resolve(Notes, manifest.Roles[0], manifest.Trees, TenantId.Parse("acme"));

        Assert.That(resolved[0].TreeId, Is.EqualTo("t/acme/a/notes/notes"));
    }

    [Test]
    public void Resolve_leaves_the_default_tenant_uncomposed()
    {
        var manifest = MixedManifest();

        var resolved = AppRoleScopeResolver.Resolve(Notes, manifest.Roles[0], manifest.Trees, TenantId.Default);

        Assert.That(resolved.Select(s => s.TreeId), Is.EqualTo(new[] { "a/notes/notes", "a/tasks/board", "old-notes", "a/notes/notes" }));
    }

    [Test]
    public void Resolve_drops_a_scope_whose_tenant_composition_fails_closed()
    {
        var role = AppMcpTestData.Role("r", LatticeOperation.Read, AppMcpTestData.TreeScope("notes"));

        Assert.That(AppRoleScopeResolver.Resolve(Notes, role, [], default), Is.Empty);
    }

    [Test]
    public void Resolve_skips_an_absent_scope_template_without_dropping_its_neighbours()
    {
        // The template array is manifest-supplied, so a hole in it must cost that one scope and
        // not the rest of the role - a role silently resolved to nothing would widen nothing,
        // but a role that threw would take the whole tool surface down with it.
        var role = new AppRoleDeclaration
        {
            Name = "holed",
            Operations = LatticeOperation.Read,
            Scopes = [AppMcpTestData.TreeScope("notes"), null!, AppMcpTestData.TreeScope("board", Tasks)],
        };

        var resolved = AppRoleScopeResolver.Resolve(Notes, role, [new AppTreeDeclaration { Name = "notes" }], TenantId.Default);

        Assert.That(resolved.Select(s => s.TreeId), Is.EqualTo(new[] { "a/notes/notes", "a/tasks/board" }));
    }
}
