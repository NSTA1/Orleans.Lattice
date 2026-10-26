using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>A bulk load stops issuing client-driven writes when its page is left.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterBulkLoadDisposalTests : ClusterTestContext
{
    private const string TreeId = "orders";

    [TestCase("begin")]
    [TestCase("first-chunk")]
    [TestCase("last-chunk")]
    public async Task Reply_after_leaving_does_not_issue_another_write(string pauseAt)
    {
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken issuedToken = default;
        async Task PauseAsync(string stage, CancellationToken token)
        {
            if (stage == pauseAt)
            {
                issuedToken = token;
                started.SetResult();
                await reply.Task;
            }
        }

        Admin.BeginBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(async call =>
        {
            await PauseAsync("begin", call.Arg<CancellationToken>());
            return new TreeBulkLoadSession { TreeId = TreeId, OperationId = call.ArgAt<string>(1) };
        });
        Admin.AppendBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<long>(), Arg.Any<IReadOnlyList<DataEntry>>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                var index = call.Arg<long>();
                await PauseAsync(index == 0 ? "first-chunk" : "last-chunk", call.Arg<CancellationToken>());
                return new TreeBulkLoadChunkAck
                {
                    TreeId = TreeId, OperationId = call.ArgAt<string>(1), ChunkIndex = index, NextChunkIndex = index + 1,
                    AcceptedEntryCount = call.Arg<IReadOnlyList<DataEntry>>().Count,
                };
            });
        Admin.CommitBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(call =>
            new TreeBulkLoadResult { TreeId = TreeId, OperationId = call.ArgAt<string>(1), TotalLiveKeys = 257 });
        var cut = RenderAt("/cluster/trees/orders/tools");
        cut.WaitForElement("textarea").Change(string.Join('\n', Enumerable.Range(0, 257).Select(index => $"k{index:0000}=v")));
        cut.Find("form[aria-label='Compose a bulk load']").Submit();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Bulk load..."), Is.True));
        Button(cut, "Bulk load...").Click();
        cut.WaitForElement(".lt-confirm input").Input(TreeId);
        var submit = cut.Find("form.lt-confirm").SubmitAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var issuedWrites = Writes();

        await DisposeComponentsAsync();
        Assert.That(issuedToken.IsCancellationRequested, Is.True, "the facade was asked to cancel the in-flight request");
        await cut.InvokeAsync(reply.SetResult);
        await submit;

        Assert.That(Writes(), Is.EqualTo(issuedWrites), "a transport ignoring cancellation must not drive the next chunk or commit");
    }

    private int Writes() => Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name is
        nameof(ILatticeTreeAdmin.BeginBulkLoadAsync) or
        nameof(ILatticeTreeAdmin.AppendBulkLoadAsync) or
        nameof(ILatticeTreeAdmin.CommitBulkLoadAsync));
}
