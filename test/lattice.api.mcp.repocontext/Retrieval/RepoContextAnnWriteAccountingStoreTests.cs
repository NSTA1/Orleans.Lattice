using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

[TestFixture]
public sealed class RepoContextAnnWriteAccountingStoreTests
{
    [Test]
    public async Task Write_counts_utf8_key_and_value_bytes_including_failed_submissions()
    {
        var inner = new InMemoryVectorIndexStore();
        var store = new RepoContextAnnWriteAccountingStore(inner);
        KeyValuePair<string, byte[]>[] batch = [new("key", [1, 2]), new("\u00e9", [3])];
        await store.WriteAsync(batch);
        inner.WriteFault = new InvalidOperationException("failed");
        Assert.That(async () => await store.WriteAsync(batch), Throws.InvalidOperationException);
        await store.DeleteAsync(["key"]);
        await store.DeletePrefixAsync("prefix");
        Assert.Multiple(() =>
        {
            Assert.That(store.RecordsSubmitted, Is.EqualTo(4));
            Assert.That(store.PayloadBytesSubmitted, Is.EqualTo(16));
            Assert.That(inner.PayloadBytesWritten, Is.EqualTo(8));
            Assert.That(store.DeletesSubmitted, Is.EqualTo(1));
            Assert.That(store.PrefixDeletesSubmitted, Is.EqualTo(1));
            Assert.That(inner.Keys, Does.Not.Contain("key"));
        });
    }

    [Test]
    public void Reads_and_resumed_scans_forward_original_tasks_and_exact_arguments()
    {
        var inner = Substitute.For<IVectorIndexStore>();
        var store = new RepoContextAnnWriteAccountingStore(inner);
        var ct = TestContext.CurrentContext.CancellationToken;
        var point = Task.FromResult<byte[]?>([1]);
        var many = Task.FromResult<IReadOnlyDictionary<string, byte[]>>(new Dictionary<string, byte[]>());
        string[] keys = ["key"];
        var scan = Substitute.For<IAsyncEnumerable<KeyValuePair<string, byte[]>>>();
        inner.ReadAsync("key", ct).Returns(point);
        inner.ReadManyAsync(keys, ct).Returns(many);
        inner.ScanAsync("prefix", ct).Returns(scan);
        inner.ScanAsync("prefix", "cursor", ct).Returns(scan);
        Assert.Multiple(() =>
        {
            Assert.That(store.ReadAsync("key", ct), Is.SameAs(point));
            Assert.That(store.ReadManyAsync(keys, ct), Is.SameAs(many));
            Assert.That(store.ScanAsync("prefix", ct), Is.SameAs(scan));
            Assert.That(store.ScanAsync("prefix", "cursor", ct), Is.SameAs(scan));
            Assert.That(store.RecordsSubmitted, Is.Zero);
            Assert.That(store.PayloadBytesSubmitted, Is.Zero);
        });
    }
}
