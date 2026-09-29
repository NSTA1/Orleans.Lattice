using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>Builders for manifests, registry records, snapshots and tools used across the fixtures.</summary>
internal static class AppMcpTestData
{
    public static readonly AppVersion V1 = AppVersion.Parse("1.0.0");
    public static readonly AppVersion V2 = AppVersion.Parse("2.0.0");

    public static AppSlug Slug(string value) => AppSlug.Parse(value);

    public static AppRoleDeclaration Role(string name, LatticeOperation operations, params AppScopeTemplate[] scopes) =>
        new() { Name = name, Operations = operations, Scopes = scopes };

    public static AppScopeTemplate TreeScope(string tree, AppSlug? app = null) => new() { Tree = tree, App = app };

    public static AppMcpToolDeclaration ToolDecl(string name, string role) =>
        new() { Name = name, Description = $"The {name} tool.", Role = role };

    public static AppManifest Manifest(
        AppSlug slug,
        AppVersion version,
        AppRoleDeclaration[] roles,
        AppMcpToolDeclaration[] tools,
        AppTreeDeclaration[]? trees = null) =>
        new()
        {
            Identity = new AppIdentity { Slug = slug, Version = version },
            Trees = trees ?? [new AppTreeDeclaration { Name = "notes" }],
            Roles = roles,
            Subscriptions = [],
            McpTools = tools,
        };

    /// <summary>A one-role manifest whose role reads the app's own <c>notes</c> tree, declaring the given tools.</summary>
    public static AppManifest ReaderManifest(AppSlug slug, AppVersion version, params string[] toolNames) =>
        Manifest(
            slug,
            version,
            [Role("reader", LatticeOperation.Read, TreeScope("notes"))],
            toolNames.Select(n => ToolDecl(n, "reader")).ToArray());

    public static AppRegistryRecord Record(
        TenantId tenant,
        AppSlug slug,
        AppVersion version,
        AppRegistryLifecycleState state = AppRegistryLifecycleState.Enabled,
        AppVersion? ceilingVersion = null,
        IReadOnlyList<AppRoleBinding>? bindings = null) =>
        new()
        {
            Isolation = new AppIsolationContext { Tenant = tenant, ClusterId = "test-cluster" },
            Slug = slug,
            Version = version,
            Provenance = new AppProvenance(),
            Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.Write),
            CeilingVersion = ceilingVersion ?? version,
            RoleBindings = bindings ?? DefaultBindings,
            State = state,
            Revision = 1,
        };

    /// <summary>The group the default test bindings bind to <paramref name="role"/>.</summary>
    public static string GroupFor(string role) => "g-" + role;

    /// <summary>The default bindings: each of the fixtures' role names bound to its own group.</summary>
    public static readonly AppRoleBinding[] DefaultBindings =
    [
        AppRoleBinding.Create("reader", GroupFor("reader")),
        AppRoleBinding.Create("writer", GroupFor("writer")),
        AppRoleBinding.Create("editor", GroupFor("editor")),
    ];

    /// <summary>Compiles a registry snapshot through the real (internal) snapshot factory.</summary>
    public static CompiledAppRegistrySnapshot Snapshot(long epoch, params AppRegistryRecord[] records)
        => CompiledAppRegistrySnapshot.Compile(records, epoch);

    public static McpServerTool Tool(string name, string result = "ok") =>
        McpServerTool.Create(() => result, new McpServerToolCreateOptions { Name = name, Description = $"impl {name}" });

    public static AppSourceResult Resolved(AppManifest manifest) =>
        AppSourceResult.Resolved(manifest, new AppProvenance(), Substitute.For<IAppActivationHandle>());
}
