using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Tests;

[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateChangeFeedRevocationTests
{
    private AuthApiStateClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new AuthApiStateClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [TestCase(true)]
    [TestCase(false)]
    public async Task Observe_revoked_grant_stops_buffered_and_future_changes(bool buffered)
    {
        var treeId = $"feed-revoked-{buffered}";
        var tree = await _fixture.RegisterTreeAsync(treeId);
        await SetGrantAsync(treeId, LatticeScope.Tree(treeId), LatticeEffect.Allow);
        string cursor;
        using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
        {
            cursor = await StateObserveTailCursor.CaptureAsync(_fixture.SiloServices, treeId);
            await tree.SetAsync("public/first", new byte[] { 1 });
            if (buffered)
            {
                await tree.SetAsync("private/buffered", new byte[] { 2 });
            }
        }

        using (AuthApiStateClusterFixture.AsSubject("reader"))
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
        {
            await using var feed = _fixture.Observer.ObserveAsync(
                new StateObserveRequest { TreeId = treeId, ContinuationToken = cursor }, cts.Token)
                .GetAsyncEnumerator();
            Assert.That(await feed.MoveNextAsync(), Is.True);
            Assert.That(feed.Current.Key, Is.EqualTo("public/first"));

            await SetGrantAsync(treeId, LatticeScope.Tree(treeId), LatticeEffect.Deny);
            Assert.That((await _fixture.Query.GetEntryAsync(treeId, "public/first")).Status,
                Is.EqualTo(StateQueryStatus.TreeNotFound), "fresh reads must already observe the revocation");

            using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
            {
                await tree.SetAsync("private/after-revocation", new byte[] { 3 });
            }

            Assert.ThrowsAsync<KeyNotFoundException>(async () => await feed.MoveNextAsync());
        }
    }

    [Test]
    public async Task Observe_narrowed_grant_replaces_the_buffered_page_filter()
    {
        const string treeId = "feed-narrowed";
        var tree = await _fixture.RegisterTreeAsync(treeId);
        await SetGrantAsync(treeId, LatticeScope.Tree(treeId), LatticeEffect.Allow);
        string cursor;
        using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
        {
            cursor = await StateObserveTailCursor.CaptureAsync(_fixture.SiloServices, treeId);
            await tree.SetAsync("public/first", new byte[] { 1 });
            await tree.SetAsync("private/buffered", new byte[] { 2 });
        }

        using (AuthApiStateClusterFixture.AsSubject("reader"))
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
        {
            await using var feed = _fixture.Observer.ObserveAsync(
                new StateObserveRequest { TreeId = treeId, ContinuationToken = cursor }, cts.Token)
                .GetAsyncEnumerator();
            Assert.That(await feed.MoveNextAsync(), Is.True);
            Assert.That(feed.Current.Key, Is.EqualTo("public/first"));

            await SetGrantAsync(treeId, LatticeScope.Prefix(treeId, "public/"), LatticeEffect.Allow);
            Assert.That((await _fixture.Query.GetEntryAsync(treeId, "private/buffered")).Status,
                Is.EqualTo(StateQueryStatus.KeyNotFound));
            using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
            {
                await tree.SetAsync("private/after-narrowing", new byte[] { 3 });
                await tree.SetAsync("public/after-narrowing", new byte[] { 4 });
            }

            Assert.That(await feed.MoveNextAsync(), Is.True);
            Assert.That(feed.Current.Key, Is.EqualTo("public/after-narrowing"));
        }
    }

    private Task SetGrantAsync(string treeId, LatticeScope scope, LatticeEffect effect) =>
        _fixture.GrantAsync(new LatticeAuthorizationRule(
            $"reader-{treeId}", LatticeSubjectSelector.User("reader"), scope,
            LatticeOperation.Read | LatticeOperation.RangeRead, effect));
}
