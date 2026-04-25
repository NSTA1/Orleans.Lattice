using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class NoOpReplogSinkTests
{
    [Test]
    public async Task WriteAsync_completes_synchronously()
    {
        IReplogSink sink = new NoOpReplogSink();
        var task = sink.WriteAsync(new ReplogEntry { TreeId = "t", Key = "k" }, CancellationToken.None);
        Assert.That(task.IsCompletedSuccessfully, Is.True);
        await task;
    }

    [Test]
    public async Task WriteAsync_accepts_default_entry()
    {
        IReplogSink sink = new NoOpReplogSink();
        await sink.WriteAsync(default, CancellationToken.None);
        Assert.Pass();
    }
}
