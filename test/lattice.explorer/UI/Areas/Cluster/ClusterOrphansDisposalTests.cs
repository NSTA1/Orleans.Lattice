using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>Capability replies cannot resume an orphan-operation watch after its page has left.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOrphansDisposalTests : ClusterTestContext
{
    [Test]
    public async Task Capability_reply_after_leaving_does_not_resume_a_disposed_watch()
    {
        var reply = new TaskCompletionSource<LatticeTreeAdminCapabilities>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.ProbeCapabilitiesAsync("orders", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                started.SetResult();
                return reply.Task;
            });
        var cut = RenderAt("/cluster/orphans?tree=orders");
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await DisposeComponentsAsync();

        await cut.InvokeAsync(() => reply.SetResult(new LatticeTreeAdminCapabilities
        {
            TreeId = "orders",
            Schema = new LatticeSchemaCapabilities { TreeId = "orders" },
            CanViewDiagnostics = true,
        }));

        Assert.That(await LeftPage.FaultAsync(Renderer), Is.Null,
            "resuming the disposed watch throws ObjectDisposedException and ends the Blazor circuit");
        await Tracked.Operations.DidNotReceive().ListOperationsAsync(
            Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>());
    }
}
