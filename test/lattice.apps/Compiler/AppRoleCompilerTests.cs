using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed partial class AppRoleCompilerTests
{
    private const string Slug = "notes";

    private static readonly LatticeOperation ReadWrite = LatticeOperation.Read | LatticeOperation.Write;

    /// <summary>The installed app <c>other</c> owns its <c>items</c> tree (default tenant).</summary>
    private static readonly AppTreeOwnerSnapshot OtherOwnsItems =
        AppTreeOwnerSnapshot.Create([new("a/other/items", AppSlug.Parse("other"))]);

    private static AppRoleDeclaration Role(string name, LatticeOperation operations, params AppScopeTemplate[] scopes) =>
        new() { Name = name, Operations = operations, Scopes = scopes };

    private static AppScopeTemplate TreeScope(string tree, string? app = null) =>
        new() { Tree = tree, App = app is null ? null : AppSlug.Parse(app) };

    private static AppScopeTemplate KeyScope(string tree, string key) =>
        new() { Tree = tree, Kind = LatticeScopeKind.Key, KeyOrPrefix = key };

    private static AppScopeTemplate PrefixScope(string tree, string prefix, string? app = null) =>
        new() { Tree = tree, App = app is null ? null : AppSlug.Parse(app), Kind = LatticeScopeKind.Prefix, KeyOrPrefix = prefix };

    private static AppTreeDeclaration Tree(string name, string? adopted = null) =>
        new() { Name = name, AdoptedTreeId = adopted };

    private static AppManifest Manifest(AppTreeDeclaration[] trees, params AppRoleDeclaration[] roles)
    {
        var manifest = new AppManifest
        {
            Identity = new() { Slug = AppSlug.Parse(Slug), Version = AppVersion.Parse("1.0.0") },
            Trees = trees,
            Roles = roles,
            Subscriptions = [],
            McpTools = [],
        };
        var validation = AppManifestValidator.Validate(manifest);
        Assert.That(validation.IsValid, Is.True, () => string.Join("; ", validation.Errors));
        return manifest;
    }

    private static AppManifest Manifest(params AppRoleDeclaration[] roles) => Manifest([Tree("docs"), Tree("audit")], roles);

    private static AppRoleBinding Bind(string role, string group) => AppRoleBinding.Create(role, group);

    private static AppCapabilityCeiling Ceiling(LatticeOperation allowed, params LatticeScope[] exceptions) =>
        AppCapabilityCeiling.Structural(allowed) with { ApprovedExceptionScopes = exceptions };

    private static AppRuleCompilation Compile(AppManifest manifest, params AppRoleBinding[] bindings) =>
        AppRoleCompiler.Compile(manifest, TenantId.Default, bindings, Ceiling(LatticeAuthOperations.All));

    [Test]
    public void Compile_expands_each_binding_across_every_scope_of_its_flat_role()
    {
        var manifest = Manifest(
            Role("writer", ReadWrite, TreeScope("docs"), PrefixScope("audit", "log/")),
            Role("reader", LatticeOperation.Read, KeyScope("docs", "readme")));

        var result = Compile(manifest, Bind("writer", "g-a"), Bind("writer", "g-b"), Bind("reader", "g-a"));

        Assert.That(result.Succeeded, Is.True);
        var projected = result.Rules
            .Select(r => (r.Subject.Id, r.Scope.Kind, r.Scope.TreeId, r.Scope.KeyOrPrefix, r.Operations))
            .OrderBy(t => t.Id, StringComparer.Ordinal).ThenBy(t => t.TreeId, StringComparer.Ordinal).ThenBy(t => t.Kind)
            .ToArray();
        Assert.That(projected, Is.EqualTo(new[]
        {
            ("g-a", LatticeScopeKind.Prefix, "a/notes/audit", (string?)"log/", ReadWrite),
            ("g-a", LatticeScopeKind.Tree, "a/notes/docs", (string?)null, ReadWrite),
            ("g-a", LatticeScopeKind.Key, "a/notes/docs", (string?)"readme", LatticeOperation.Read),
            ("g-b", LatticeScopeKind.Prefix, "a/notes/audit", (string?)"log/", ReadWrite),
            ("g-b", LatticeScopeKind.Tree, "a/notes/docs", (string?)null, ReadWrite),
        }));
        Assert.That(result.Excesses, Is.Empty);
        Assert.That(result.UnknownRoleBindings, Is.Empty);
        Assert.That(result.UnboundRoles, Is.Empty);
    }

    [Test]
    public void Compile_emits_unconditional_allow_rules_for_group_subjects_only()
    {
        var result = Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"), TreeScope("audit"))),
            Bind("writer", "g-a"), Bind("writer", "g-b"));

        Assert.That(result.Rules, Has.Count.EqualTo(4));
        Assert.That(result.Rules.Select(r => r.Subject.Kind), Is.All.EqualTo(LatticeSubjectSelectorKind.Group));
        Assert.That(result.Rules.Select(r => r.Subject.Id).Distinct(), Is.EquivalentTo(new[] { "g-a", "g-b" }));
        Assert.That(result.Rules.Select(r => r.Effect), Is.All.EqualTo(LatticeEffect.Allow));
        Assert.That(result.Rules.Select(r => r.Condition), Is.All.Null);
    }

    [Test]
    public void Compile_orders_rules_by_ordinal_rule_id()
    {
        var result = Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"), TreeScope("audit")), Role("reader", LatticeOperation.Read, TreeScope("docs"))),
            Bind("writer", "g-z"), Bind("reader", "g-a"), Bind("writer", "g-a"));

        var ids = result.Rules.Select(r => r.RuleId).ToArray();
        Assert.That(ids, Is.EqualTo(ids.OrderBy(i => i, StringComparer.Ordinal).ToArray()));
    }

    [Test]
    public void Compile_is_idempotent_across_repeated_compilation_of_the_same_input()
    {
        AppRuleCompilation Once() => Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs"), PrefixScope("audit", "log/")), Role("reader", LatticeOperation.Read, KeyScope("docs", "k"))),
            Bind("writer", "g-a"), Bind("reader", "g-b"));

        var first = Once();
        var second = Once();

        Assert.That(second.Rules, Is.EqualTo(first.Rules));
        Assert.That(AppRoleCompiler.ComputeDiff(AppSlug.Parse(Slug), second.Rules, first.Rules).IsEmpty, Is.True);
    }

    [Test]
    public void Compile_deduplicates_a_repeated_binding()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));

        var once = Compile(manifest, Bind("writer", "g-a"));
        var twice = Compile(manifest, Bind("writer", "g-a"), Bind("writer", "g-a"));

        Assert.That(twice.Rules, Is.EqualTo(once.Rules));
    }

    [Test]
    public void Compile_reports_an_unbound_role_as_a_diagnostic_and_emits_nothing_for_it()
    {
        var result = Compile(
            Manifest(Role("writer", ReadWrite, TreeScope("docs")), Role("reader", LatticeOperation.Read, TreeScope("docs")), Role("auditor", LatticeOperation.Read, TreeScope("audit"))),
            Bind("writer", "g-a"));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.UnboundRoles, Is.EqualTo(new[] { "reader", "auditor" }));
        Assert.That(result.Rules, Has.Count.EqualTo(1));
        Assert.That(result.Rules[0].RuleId, Does.StartWith("app:notes:writer:"));
    }

    [Test]
    public void Compile_fails_when_a_binding_names_an_undeclared_role()
    {
        var unknown = Bind("admin", "g-x");

        var result = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))), Bind("writer", "g-a"), unknown);

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Rules, Is.Empty);
        Assert.That(result.UnknownRoleBindings, Is.EqualTo(new[] { unknown }));
        Assert.That(result.Excesses, Is.Empty);
        Assert.That(result.UnboundRoles, Is.Empty);
    }

    [Test]
    public void Compile_with_no_bindings_succeeds_with_an_empty_set()
    {
        var result = Compile(Manifest(Role("writer", ReadWrite, TreeScope("docs"))));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Rules, Is.Empty);
        Assert.That(result.UnboundRoles, Is.EqualTo(new[] { "writer" }));
    }

    [Test]
    public void Compile_rejects_null_arguments()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));
        var ceiling = Ceiling(ReadWrite);

        Assert.That(() => AppRoleCompiler.Compile(null!, TenantId.Default, [], ceiling), Throws.ArgumentNullException);
        Assert.That(() => AppRoleCompiler.Compile(manifest, TenantId.Default, null!, ceiling), Throws.ArgumentNullException);
        Assert.That(() => AppRoleCompiler.Compile(manifest, TenantId.Default, [], null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Compile_rejects_an_unattributed_tenant()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));

        Assert.That(
            () => AppRoleCompiler.Compile(manifest, default, [], Ceiling(ReadWrite)),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void Compile_rejects_a_manifest_without_a_slug()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs"))) with
        {
            Identity = new() { Slug = default, Version = AppVersion.Parse("1.0.0") },
        };

        Assert.That(
            () => AppRoleCompiler.Compile(manifest, TenantId.Default, [], Ceiling(ReadWrite)),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("manifest"));
    }

    [Test]
    public void Compile_rejects_a_null_binding_and_an_empty_group_id()
    {
        var manifest = Manifest(Role("writer", ReadWrite, TreeScope("docs")));
        var ceiling = Ceiling(ReadWrite);

        Assert.That(() => AppRoleCompiler.Compile(manifest, TenantId.Default, [null!], ceiling), Throws.ArgumentException);
        Assert.That(
            () => AppRoleCompiler.Compile(manifest, TenantId.Default, [new() { RoleName = "writer", GroupId = "" }], ceiling),
            Throws.ArgumentException);
        Assert.That(
            () => AppRoleCompiler.Compile(manifest, TenantId.Default, [new() { RoleName = "unknown", GroupId = "" }], ceiling),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("bindings"));
    }
}
