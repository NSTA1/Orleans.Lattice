using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Regression coverage for the extraction of the shared app-role evaluation: the app MCP tool surface offers a
/// tool exactly when <see cref="AppRoleGrantEvaluator"/> reports the caller holds the tool's role, across a
/// grant matrix, so the tool gate and the app workspace cannot drift apart.
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

    private static readonly string[] GrantMatrix =
    [
        "none", "read-notes", "write-notes", "read-write-notes", "read-write-drafts", "other-subject", "filtered-everywhere",
    ];

    private static void Apply(string grantCase, GrantingAccessGate gate)
    {
        switch (grantCase)
        {
            case "read-notes": gate.Grant("alice", "a/notes/notes", LatticeOperation.Read); break;
            case "write-notes": gate.Grant("alice", "a/notes/notes", LatticeOperation.Write); break;
            case "read-write-notes": gate.Grant("alice", "a/notes/notes", LatticeOperation.Read | LatticeOperation.Write); break;
            case "read-write-drafts": gate.Grant("alice", "a/notes/drafts", LatticeOperation.Read | LatticeOperation.Write); break;
            case "other-subject": gate.Grant("bob", "a/notes/notes", LatticeOperation.Read | LatticeOperation.Write); break;
            case "filtered-everywhere": gate.Override = static _ => LatticeAccessDecision.Filtered(static _ => false); break;
        }
    }
    [TestCaseSource(nameof(GrantMatrix))]
    public async Task The_tool_gate_offers_exactly_the_tools_of_the_roles_the_evaluator_reports_held(string grantCase)
    {
        var host = new AppMcpTestHost()
            .Provide(Notes, AppMcpTestData.Tool("read"), AppMcpTestData.Tool("write"), AppMcpTestData.Tool("edit"))
            .Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        host.Source.Add(ThreeRoleManifest());
        Apply(grantCase, host.Gate);

        var advertised = (await host.AdvertisedAsync()).Where(ToolRoles.ContainsKey).Select(t => ToolRoles[t]).ToHashSet();
        var evaluator = new AppRoleGrantEvaluator(host.Projection, host.Source, host.Gate);
        var evaluation = await evaluator.EvaluateAsync(TenantId.Default, Notes, new LatticeSubject("alice"), CancellationToken.None);

        Assert.That(evaluation, Is.Not.Null);
        Assert.That(evaluation!.HeldRoles, Is.EquivalentTo(advertised));
        Assert.That(evaluation.HasGrant, Is.EqualTo(advertised.Count > 0));
    }

    [Test]
    public async Task The_evaluator_reports_no_install_for_a_disabled_app_the_tool_gate_also_ignores()
    {
        var host = new AppMcpTestHost()
            .Provide(Notes, AppMcpTestData.Tool("read"))
            .Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1, AppRegistryLifecycleState.Disabled));
        host.Source.Add(ThreeRoleManifest());
        host.Gate.Grant("alice", "a/notes/notes", LatticeOperation.Read);

        var evaluator = new AppRoleGrantEvaluator(host.Projection, host.Source, host.Gate);

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
        Assert.That(await evaluator.EvaluateAsync(TenantId.Default, Notes, new LatticeSubject("alice"), CancellationToken.None), Is.Null);
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
            set,
            new GrantingAccessGate());

        var catalog = await source.GetCatalogAsync(CancellationToken.None);

        Assert.That(catalog.Failures, Is.Empty, () => string.Join("; ", catalog.Failures.Select(f => f.Failure)));
        Assert.That(catalog.GetTenantApps(TenantId.Default).Single().Activation.Tools, Has.Length.EqualTo(1));
    }
}
