using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Parity between the surfaces that answer "does this caller hold app role R?": the app MCP tool surface offers
/// a tool exactly when <see cref="AppRoleGrantEvaluator"/> - the evaluation the app workspace reports - says the
/// caller holds the tool's role, across a binding matrix. Every case also gives the caller broad rights of its
/// own, which must never add a role (#3902).
/// </summary>
[TestFixture]
public sealed class AppRoleGrantEvaluatorParityTests
{
    private static readonly AppSlug Notes = AppMcpTestData.Slug("notes");

    private static AppManifest ThreeRoleManifest() =>
        AppMcpTestData.Manifest(
            Notes,
            AppMcpTestData.V1,
            [
                AppMcpTestData.Role("reader", LatticeOperation.Read, AppMcpTestData.TreeScope("notes")),
                AppMcpTestData.Role("writer", LatticeOperation.Write, AppMcpTestData.TreeScope("notes")),
                AppMcpTestData.Role("editor", LatticeOperation.Read | LatticeOperation.Write, AppMcpTestData.TreeScope("notes"), AppMcpTestData.TreeScope("drafts")),
            ],
            [AppMcpTestData.ToolDecl("read", "reader"), AppMcpTestData.ToolDecl("write", "writer"), AppMcpTestData.ToolDecl("edit", "editor")],
            [new AppTreeDeclaration { Name = "notes" }, new AppTreeDeclaration { Name = "drafts" }]);

    private static readonly Dictionary<string, string> ToolRoles = new(StringComparer.Ordinal)
    {
        ["notes_read"] = "reader",
        ["notes_write"] = "writer",
        ["notes_edit"] = "editor",
    };

    private static readonly object[] BindingMatrix =
    [
        new object[] { "none", Array.Empty<string>() },
        new object[] { "reader", new[] { "reader" } },
        new object[] { "writer", new[] { "writer" } },
        new object[] { "editor", new[] { "editor" } },
        new object[] { "reader-and-editor", new[] { "reader", "editor" } },
        new object[] { "another-subject-bound", Array.Empty<string>() },
    ];

    private static void Apply(string bindingCase, IEnumerable<string> roles, AppMcpTestHost host)
    {
        // Broad rights of the caller's own on every tree the app declares: capability that confers no role.
        host.Gate
            .Grant("alice", "a/notes/notes", LatticeOperation.Read | LatticeOperation.Write)
            .Grant("alice", "a/notes/drafts", LatticeOperation.Read | LatticeOperation.Write);
        host.Membership.Join("alice", "cluster-admins");
        if (bindingCase == "another-subject-bound")
            host.Bind("bob", "editor");
        foreach (var role in roles)
            host.Bind("alice", role);
    }

    [TestCaseSource(nameof(BindingMatrix))]
    public async Task The_tool_gate_offers_exactly_the_tools_of_the_roles_the_evaluator_reports_held(string bindingCase, string[] bound)
    {
        var host = new AppMcpTestHost()
            .Provide(Notes, AppMcpTestData.Tool("read"), AppMcpTestData.Tool("write"), AppMcpTestData.Tool("edit"))
            .Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        host.Source.Add(ThreeRoleManifest());
        Apply(bindingCase, bound, host);

        var advertised = (await host.AdvertisedAsync()).Where(ToolRoles.ContainsKey).Select(t => ToolRoles[t]).ToHashSet();
        var evaluator = new AppRoleGrantEvaluator(host.Projection, host.Source);
        using var credential = LatticeCredentialContext.With(new LatticeCredential("t", principalId: "alice"));
        var alice = await host.Membership.ResolveCurrentAsync();
        var evaluation = await evaluator.EvaluateAsync(TenantId.Default, Notes, alice, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(evaluation, Is.Not.Null);
            Assert.That(evaluation!.HeldRoles, Is.EquivalentTo(advertised));
            Assert.That(evaluation.HeldRoles, Is.EquivalentTo(bound), "a role is held exactly when the caller is bound to it");
            Assert.That(evaluation.HasGrant, Is.EqualTo(advertised.Count > 0));
            Assert.That(host.Gate.Requests, Is.Empty, "neither surface consults the caller's own rules");
        });
    }

    [Test]
    public async Task The_evaluator_reports_no_install_for_a_disabled_app_the_tool_gate_also_ignores()
    {
        var host = new AppMcpTestHost()
            .Provide(Notes, AppMcpTestData.Tool("read"))
            .Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1, AppRegistryLifecycleState.Disabled));
        host.Source.Add(ThreeRoleManifest());
        host.Bind("alice");

        var evaluator = new AppRoleGrantEvaluator(host.Projection, host.Source);

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
        Assert.That(await evaluator.EvaluateAsync(TenantId.Default, Notes, new LatticeSubject("alice", ["g-reader"]), CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task App_tools_activate_from_the_install_source_when_a_second_source_offers_the_slug()
    {
        var manifest = AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search");
        var set = new AppSourceSet([new TestCatalogSource("feed-a").Publish(manifest), new TestCatalogSource("feed-b").Publish(manifest)]);
        var record = AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1) with { Provenance = new AppProvenance { Source = "feed-b" } };
        var projection = new FakeAppRegistryProjection(AppMcpTestData.Snapshot(1, record));
        var source = new AppMcpToolSource(
            [new AppMcpToolProvider(Notes, [AppMcpTestData.Tool("search")])],
            NullLogger<AppMcpToolSource>.Instance,
            projection,
            set);

        var catalog = await source.GetCatalogAsync(CancellationToken.None);

        Assert.That(catalog.Failures, Is.Empty, () => string.Join("; ", catalog.Failures.Select(f => f.Failure)));
        Assert.That(catalog.GetTenantApps(TenantId.Default).Single().Activation.Tools, Has.Length.EqualTo(1));
    }
}
