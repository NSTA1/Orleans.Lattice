using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// A backup page left while it is still reading (issue #4011): the reply resumes after the
/// page is disposed, and the page must stop quietly. It used to declare the address not
/// found when the backup turned out not to exist, which the router then applied to
/// whichever page the caller had moved on to.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupsPageDisposalTests : BackupsTestContext
{
    [Test]
    public async Task A_backup_page_left_while_it_is_described_declares_nothing_not_found()
    {
        var gate = new TaskCompletionSource<BackupChainDescription?>();
        var described = 0;
        Backups.Describe = _ =>
        {
            described++;
            return gate.Task;
        };
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderAt<BackupPage>("backups/gone");
        cut.WaitUntil(() => Assert.That(described, Is.EqualTo(1)));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(() => gate.SetResult(null));

        Assert.Multiple(() =>
        {
            Assert.That(LeftPage.Fault(Renderer), Is.Null, "the circuit would end");
            Assert.That(notFound, Is.Zero, "a page that has been left declares nothing not found");
        });
    }
}
