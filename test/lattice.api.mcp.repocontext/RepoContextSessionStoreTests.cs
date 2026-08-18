using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextSessionStore"/>, the read-merge-write persistence
/// behind per-session reuse bookkeeping. They prove a delivered unit round-trips
/// through the dedicated session tree, that a second call folds into (rather than
/// clobbers) the first, that bookkeeping is scoped strictly per <c>(repoId, sessionId)</c>
/// with no cross-session leakage, and that blank or empty inputs fail closed.
/// </summary>
[TestFixture]
public sealed class RepoContextSessionStoreTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static (RepoContextSessionStore Store, Dictionary<string, byte[]> Map) NewStore()
    {
        var map = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var tree = Substitute.For<ILattice>();
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<byte[]?>(
                map.TryGetValue(ci.ArgAt<string>(0), out var value) ? value : null));
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                map[ci.ArgAt<string>(0)] = ci.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Session).Returns(tree);
        return (new RepoContextSessionStore(grainFactory, Serializer), map);
    }

    private static IReadOnlyList<string> Receipts(RepoContextSessionRecord record)
        => record.Receipts.Values().Select(System.Text.Encoding.UTF8.GetString)
            .OrderBy(static s => s, StringComparer.Ordinal).ToArray();

    [Test]
    public async Task LoadAsync_returns_null_when_the_session_has_recorded_nothing()
    {
        var (store, _) = NewStore();

        Assert.That(await store.LoadAsync(RepoId, "s", CancellationToken.None), Is.Null);
    }

    [TestCase("", "s")]
    [TestCase("acme", "")]
    public async Task LoadAsync_fails_closed_on_a_blank_identifier(string repoId, string sessionId)
    {
        var (store, _) = NewStore();

        Assert.That(await store.LoadAsync(repoId, sessionId, CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task RecordAsync_then_LoadAsync_round_trips_receipts_and_possession()
    {
        var (store, _) = NewStore();

        await store.RecordAsync(RepoId, "s", ["r1", "r2"], ["src/A.cs\u0000h"], CancellationToken.None);
        var record = await store.LoadAsync(RepoId, "s", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(record, Is.Not.Null);
            Assert.That(record!.SessionId, Is.EqualTo("s"));
            Assert.That(record.RepoId, Is.EqualTo(RepoId));
            Assert.That(Receipts(record), Is.EqualTo(new[] { "r1", "r2" }));
            Assert.That(record.Possession.Contains(System.Text.Encoding.UTF8.GetBytes("src/A.cs\u0000h")), Is.True);
        });
    }

    [Test]
    public async Task RecordAsync_folds_a_later_call_into_the_earlier_record()
    {
        var (store, _) = NewStore();

        await store.RecordAsync(RepoId, "s", ["r1"], [], CancellationToken.None);
        await store.RecordAsync(RepoId, "s", ["r2"], [], CancellationToken.None);
        var record = await store.LoadAsync(RepoId, "s", CancellationToken.None);

        Assert.That(Receipts(record!), Is.EqualTo(new[] { "r1", "r2" }),
            "The store merges into the existing record rather than overwriting it.");
    }

    [Test]
    public async Task RecordAsync_scopes_bookkeeping_strictly_per_session_id()
    {
        var (store, _) = NewStore();

        await store.RecordAsync(RepoId, "s1", ["only-s1"], [], CancellationToken.None);
        var other = await store.LoadAsync(RepoId, "s2", CancellationToken.None);

        Assert.That(other, Is.Null, "A different session must never observe another session's deliveries.");
    }

    [Test]
    public async Task RecordAsync_scopes_bookkeeping_strictly_per_repo_id()
    {
        var (store, _) = NewStore();

        await store.RecordAsync("repo-a", "s", ["only-a"], [], CancellationToken.None);
        var other = await store.LoadAsync("repo-b", "s", CancellationToken.None);

        Assert.That(other, Is.Null, "The same session id under a different repo is a distinct scope.");
    }

    [Test]
    public async Task RecordAsync_is_a_no_op_when_nothing_was_delivered()
    {
        var (store, map) = NewStore();

        await store.RecordAsync(RepoId, "s", [], [], CancellationToken.None);

        Assert.That(map, Is.Empty, "An empty delivery must not create a record.");
    }

    [TestCase("", "s")]
    [TestCase("acme", "")]
    public async Task RecordAsync_fails_closed_on_a_blank_identifier(string repoId, string sessionId)
    {
        var (store, map) = NewStore();

        await store.RecordAsync(repoId, sessionId, ["r"], [], CancellationToken.None);

        Assert.That(map, Is.Empty);
    }

    [Test]
    public void RecordAsync_rejects_null_collections()
    {
        var (store, _) = NewStore();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await store.RecordAsync(RepoId, "s", null!, [], CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.RecordAsync(RepoId, "s", [], null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextSessionStore(null!, Serializer), Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextSessionStore(Substitute.For<IGrainFactory>(), null!),
                Throws.ArgumentNullException);
        });
    }
}
