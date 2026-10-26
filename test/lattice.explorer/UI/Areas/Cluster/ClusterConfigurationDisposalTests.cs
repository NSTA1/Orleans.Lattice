using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterConfigurationDisposalTests : ClusterTestContext
{
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Save_page_left_before_reply_posts_no_toast(bool retention, bool fails)
    {
        const string treeId = "orders";
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = false;
        Admin.GetHistoryRetentionAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(new TreeHistoryRetention { TreeId = treeId });
        Admin.SetTreeConfigAsync(treeId, Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                started = true;
                await gate.Task;
                return new TreeConfigurationReport { TreeId = treeId, Exists = true };
            });
        Admin.SetHistoryRetentionAsync(treeId, Arg.Any<TreeHistoryRetentionMode?>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                started = true;
                await gate.Task;
                return new TreeHistoryRetention { TreeId = treeId };
            });
        var cut = Render<ClusterTreeConfiguration>(p => p
            .Add(x => x.TreeId, treeId)
            .Add(x => x.Capabilities, new LatticeTreeAdminCapabilities
            {
                TreeId = treeId, Schema = new LatticeSchemaCapabilities { TreeId = treeId },
                CanViewDiagnostics = true, CanAdministerTree = true,
            }));
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(2)));
        if (!retention)
        {
            cut.Find("form[aria-label='Change configuration'] select").Change("off");
        }

        var save = cut.Find(retention
            ? "form[aria-label='Change history retention']"
            : "form[aria-label='Change configuration']").SubmitAsync();
        cut.WaitUntil(() => Assert.That(started, Is.True));
        await DisposeComponentsAsync();
        await cut.InvokeAsync(() =>
        {
            if (fails)
            {
                gate.SetException(new InvalidOperationException("Old save failed"));
            }
            else
            {
                gate.SetResult();
            }
        });
        await save;

        Assert.That(Toasts, Is.Empty);
        Assert.That(await LeftPage.FaultAsync(Renderer), Is.Null);
    }
}
