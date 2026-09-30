using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// The workspace describes an app's UI bridge as the bridge itself admits it (issue #4020): the grants the
/// operator consented to that the installed manifest still requests. The Explorer's frame broker gates the
/// non-data operations (<c>context.read</c>, <c>context.user</c>, <c>nav.sync</c>, <c>ui.notify</c>) on this
/// set alone, so offering the bare request would grant them without consent.
/// </summary>
[TestFixture]
public sealed class LatticeAppWorkspaceBridgeTests
{
    [Test]
    public async Task The_described_bridge_is_the_consented_grants_the_manifest_still_requests()
    {
        var harness = Harness(
            consented: AppUiBridgeRequest.Create(
            [
                new AppUiBridgeGrant(AppUiBridgeOperations.ContextRead),
                new AppUiBridgeGrant(AppUiBridgeOperations.DataRead, "contacts"),
                new AppUiBridgeGrant(AppUiBridgeOperations.DataWrite),
                new AppUiBridgeGrant(AppUiBridgeOperations.UiNotify),
            ]),
            UiTestManifests.Bridge(AppUiBridgeOperations.ContextRead),
            UiTestManifests.Bridge(AppUiBridgeOperations.ContextUser),
            UiTestManifests.Bridge(AppUiBridgeOperations.NavSync),
            UiTestManifests.Bridge(AppUiBridgeOperations.DataRead),
            UiTestManifests.Bridge(AppUiBridgeOperations.DataWrite, "contacts"));

        var described = await harness.Workspace.DescribeMyAppAsync(AppsControlHarness.Slug);

        Assert.That(Grants(described), Is.EqualTo(new[]
        {
            // Requested and consented.
            (AppUiBridgeOperations.ContextRead, (string?)null),
            // Requested for every tree, consented for one: narrowed to that tree.
            (AppUiBridgeOperations.DataRead, "contacts"),
            // Requested for one tree, consented for every tree: the request.
            (AppUiBridgeOperations.DataWrite, "contacts"),
            // context.user and nav.sync were requested but never consented; ui.notify was
            // consented but is no longer requested. None is offered.
        }));
    }

    [Test]
    public async Task No_recorded_consent_describes_no_bridge_grant()
    {
        var harness = Harness(
            consented: null,
            UiTestManifests.Bridge(AppUiBridgeOperations.ContextRead),
            UiTestManifests.Bridge(AppUiBridgeOperations.NavSync));

        var described = await harness.Workspace.DescribeMyAppAsync(AppsControlHarness.Slug);

        Assert.Multiple(() =>
        {
            Assert.That(described?.Ui, Is.Not.Null, "the premise: the app ships a UI");
            Assert.That(Grants(described), Is.Empty);
        });
    }

    private static WorkspaceHarness Harness(AppUiBridgeRequest? consented, params AppUiBridgeDeclaration[] requested)
    {
        var harness = new WorkspaceHarness();
        harness.Sources = new AppSourceSet(
        [
            new TestCatalogSource(InImageAppSource.SourceKey)
                .Publish(UiTestManifests.WithUi(AppsControlHarness.Manifest(), requested))
                .WithUiAssets(),
        ]);
        return harness.Publish(WorkspaceHarness.Record() with { ConsentedBridge = consented }).GrantReader();
    }

    private static (string Operation, string? Tree)[] Grants(WorkspaceAppDescriptor? described) =>
        [.. described!.Ui!.Bridge.Select(grant => (grant.Operation, grant.Tree))];
}
