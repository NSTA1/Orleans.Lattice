using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class NoOpReplogSinkTests
{
    [Test]
    public async Task WriteAsync_completes_synchronously()
    {
        IReplogSink sink = new NoOpReplogSink();
        var task = sink.WriteAsync("t", CancellationToken.None);
        Assert.That(task.IsCompletedSuccessfully, Is.True);
        await task;
    }

    [Test]
    public async Task WriteAsync_accepts_empty_tree_id()
    {
        IReplogSink sink = new NoOpReplogSink();
        await sink.WriteAsync(string.Empty, CancellationToken.None);
        Assert.Pass();
    }
}
