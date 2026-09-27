namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

public sealed partial class RepoContextAnnIndexLoadResumeTests
{
    [Test]
    public async Task Readiness_reads_the_actual_handle_gate_before_and_after_build_and_disposal()
    {
        var store = new FaultOnceScanStore();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var source = SeededSource();
        using var handle = NewHandle(source, store, RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space), reporter);
        var before = handle.DescribeReadiness();
        Assert.Multiple(() =>
        {
            Assert.That(before.CanServe, Is.False);
            Assert.That(before.Blocker, Is.EqualTo("ann_not_serving"));
        });
        await handle.EnsureBuiltAsync(Ct);
        var built = handle.DescribeReadiness();
        Assert.Multiple(() =>
        {
            Assert.That(built.CanServe, Is.True);
            Assert.That(built.AnnCanServe, Is.True);
            Assert.That(built.Progress!.Value.VectorsIndexed, Is.GreaterThan(0));
        });
        handle.Dispose();
        Assert.That(handle.DescribeReadiness().CanServe, Is.False);
    }
}
