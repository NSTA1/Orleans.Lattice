using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression tests for <see cref="CancellationToken"/> support on the
/// public <see cref="ILattice"/> surface. Verifies that a pre-cancelled token
/// causes every public method to throw <see cref="OperationCanceledException"/>
/// before the orchestrator performs any work.
/// </summary>
[TestFixture]
public class CancellationTokenIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private ILattice NewTree() =>
        _cluster.GrainFactory.GetGrain<ILattice>($"ct-{Guid.NewGuid():N}");

    private static CancellationToken Cancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts.Token;
    }

    [Test]
    public void GetAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.GetAsync("k", Cancelled()));
    }

    [Test]
    public void SetAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.SetAsync("k", Bytes("v"), Cancelled()));
    }

    [Test]
    public void DeleteAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.DeleteAsync("k", Cancelled()));
    }

    [Test]
    public void DeleteRangeAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.DeleteRangeAsync("a", "z", Cancelled()));
    }

    [Test]
    public void CountAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.CountAsync(Cancelled()));
    }

    [Test]
    public void CountPerShardAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.CountPerShardAsync(Cancelled()));
    }

    [Test]
    public void GetManyAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.GetManyAsync(new List<string> { "a", "b" }, Cancelled()));
    }

    [Test]
    public void SetManyAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.SetManyAsync(
                new List<KeyValuePair<string, byte[]>> { new("a", Bytes("A")) },
                Cancelled()));
    }

    [Test]
    public void SetManyAtomicAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.SetManyAtomicAsync(
                new List<KeyValuePair<string, byte[]>> { new("a", Bytes("A")) },
                Cancelled()));
    }

    [Test]
    public void BulkLoadAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.BulkLoadAsync(
                new List<KeyValuePair<string, byte[]>> { new("a", Bytes("A")) },
                Cancelled()));
    }

    [Test]
    public void KeysAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in tree.KeysAsync(cancellationToken: Cancelled()))
            { /* should never reach */ }
        });
    }

    [Test]
    public void EntriesAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in tree.EntriesAsync(cancellationToken: Cancelled()))
            { /* should never reach */ }
        });
    }

    [Test]
    public async Task KeysAsync_stops_mid_stream_when_token_cancelled()
    {
        var tree = NewTree();
        for (int i = 0; i < 50; i++)
            await tree.SetAsync($"k{i:D3}", Bytes($"v{i}"));

        using var cts = new CancellationTokenSource();
        var observed = 0;

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in tree.KeysAsync(cancellationToken: cts.Token))
            {
                observed++;
                if (observed == 1) cts.Cancel();
            }
        });

        Assert.That(observed, Is.GreaterThanOrEqualTo(1));
        Assert.That(observed, Is.LessThan(50));
    }

    [Test]
    public void OpenKeyCursorAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await tree.OpenKeyCursorAsync(cancellationToken: Cancelled()));
    }

    [Test]
    public async Task NextKeysAsync_throws_when_token_already_cancelled()
    {
        var tree = NewTree();
        await tree.SetAsync("a", Bytes("A"));
        var cursorId = await tree.OpenKeyCursorAsync();
        try
        {
            Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await tree.NextKeysAsync(cursorId, 10, Cancelled()));
        }
        finally
        {
            await tree.CloseCursorAsync(cursorId);
        }
    }

    [Test]
    public async Task Non_cancelled_token_behaves_as_before()
    {
        // Regression: adding CT parameters must not change default behaviour.
        var tree = NewTree();
        await tree.SetAsync("a", Bytes("A"));
        await tree.SetAsync("b", Bytes("B"));

        Assert.That(await tree.CountAsync(CancellationToken.None), Is.EqualTo(2));
        Assert.That(await tree.GetAsync("a", CancellationToken.None), Is.EqualTo(Bytes("A")));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(cancellationToken: CancellationToken.None))
            keys.Add(k);
        Assert.That(keys, Is.EquivalentTo(new[] { "a", "b" }));
    }
}
