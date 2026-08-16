namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextVectorCache"/>: a warm decoded-candidate
/// set is returned on a hit, kept per <c>(repoId, space)</c>, dropped precisely by
/// <see cref="RepoContextVectorCache.Invalidate(string)"/>, aged out by the TTL
/// backstop, and never cached from a gather that raced a concurrent invalidation.
/// A non-positive TTL disables the cache so every lookup misses.
/// </summary>
[TestFixture]
public sealed class RepoContextVectorCacheTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space = new("m", 4, VectorNormalization.UnitL2);
    private static readonly EmbeddingSpaceTag OtherSpace = new("m", 8, VectorNormalization.UnitL2);

    private static IReadOnlyList<RepoContextVectorCandidate> Candidates(string id = "v1")
        => new[] { new RepoContextVectorCandidate(id, "repo/acme/file/A.cs", new[] { 1f, 0f, 0f, 0f }, Space) };

    private static RepoContextVectorCache Create(FakeTimeProvider clock, TimeSpan ttl)
        => new(clock, new RepoContextIndexingOptions { VectorCacheTtl = ttl });

    [Test]
    public void TryGet_misses_for_an_unknown_repo()
    {
        var cache = Create(new FakeTimeProvider(), TimeSpan.FromSeconds(30));

        var hit = cache.TryGet(RepoId, Space, out var candidates);

        Assert.Multiple(() =>
        {
            Assert.That(hit, Is.False, "A repo that was never gathered is a miss.");
            Assert.That(candidates, Is.Empty, "A miss yields an empty set, never null.");
        });
    }

    [Test]
    public void Store_then_TryGet_returns_the_cached_set()
    {
        var cache = Create(new FakeTimeProvider(), TimeSpan.FromSeconds(30));
        var stored = Candidates();

        var generation = cache.CaptureGeneration(RepoId);
        cache.Store(RepoId, Space, stored, generation);

        Assert.Multiple(() =>
        {
            Assert.That(cache.TryGet(RepoId, Space, out var got), Is.True, "A stored set is a hit.");
            Assert.That(got, Is.SameAs(stored), "The hit hands back the stored reference without copying.");
        });
    }

    [Test]
    public void TryGet_for_a_different_space_misses()
    {
        var cache = Create(new FakeTimeProvider(), TimeSpan.FromSeconds(30));

        var generation = cache.CaptureGeneration(RepoId);
        cache.Store(RepoId, Space, Candidates(), generation);

        Assert.That(cache.TryGet(RepoId, OtherSpace, out _), Is.False,
            "A set cached under one embedding space does not answer a query in another.");
    }

    [Test]
    public void Invalidate_drops_the_cached_entry()
    {
        var cache = Create(new FakeTimeProvider(), TimeSpan.FromSeconds(30));

        var generation = cache.CaptureGeneration(RepoId);
        cache.Store(RepoId, Space, Candidates(), generation);
        cache.Invalidate(RepoId);

        Assert.That(cache.TryGet(RepoId, Space, out _), Is.False,
            "Invalidation drops the repository's cached set immediately.");
    }

    [Test]
    public void TryGet_after_the_ttl_elapses_misses()
    {
        var clock = new FakeTimeProvider { UtcNow = DateTimeOffset.UnixEpoch };
        var cache = Create(clock, TimeSpan.FromSeconds(30));

        var generation = cache.CaptureGeneration(RepoId);
        cache.Store(RepoId, Space, Candidates(), generation);

        clock.UtcNow = DateTimeOffset.UnixEpoch.AddSeconds(31);

        Assert.That(cache.TryGet(RepoId, Space, out _), Is.False,
            "A set older than the TTL is stale and re-gathered.");
    }

    [Test]
    public void TryGet_just_before_the_ttl_still_hits()
    {
        var clock = new FakeTimeProvider { UtcNow = DateTimeOffset.UnixEpoch };
        var cache = Create(clock, TimeSpan.FromSeconds(30));

        var generation = cache.CaptureGeneration(RepoId);
        cache.Store(RepoId, Space, Candidates(), generation);

        clock.UtcNow = DateTimeOffset.UnixEpoch.AddSeconds(29);

        Assert.That(cache.TryGet(RepoId, Space, out _), Is.True,
            "A set younger than the TTL is still trusted.");
    }

    [Test]
    public void Store_with_a_superseded_generation_is_dropped()
    {
        var cache = Create(new FakeTimeProvider(), TimeSpan.FromSeconds(30));

        // Simulate a gather that captured the generation, then a write landing (which
        // invalidates and advances the generation) before the gather stored its now
        // stale result.
        var generation = cache.CaptureGeneration(RepoId);
        cache.Invalidate(RepoId);
        cache.Store(RepoId, Space, Candidates(), generation);

        Assert.That(cache.TryGet(RepoId, Space, out _), Is.False,
            "A store whose generation was superseded by an invalidation is dropped, never cached.");
    }

    [Test]
    public void TryGet_with_a_non_positive_ttl_always_misses()
    {
        var cache = Create(new FakeTimeProvider(), TimeSpan.Zero);

        var generation = cache.CaptureGeneration(RepoId);
        cache.Store(RepoId, Space, Candidates(), generation);

        Assert.That(cache.TryGet(RepoId, Space, out _), Is.False,
            "A zero TTL disables the cache: every lookup misses, reproducing the uncached path.");
    }

    [Test]
    public void Ctor_rejects_null_arguments()
    {
        var options = new RepoContextIndexingOptions();
        Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextVectorCache(null!, options), Throws.ArgumentNullException);
            Assert.That(() => new RepoContextVectorCache(new FakeTimeProvider(), null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Methods_reject_a_null_repo_id()
    {
        var cache = Create(new FakeTimeProvider(), TimeSpan.FromSeconds(30));
        Assert.Multiple(() =>
        {
            Assert.That(() => cache.TryGet(null!, Space, out _), Throws.ArgumentNullException);
            Assert.That(() => cache.CaptureGeneration(null!), Throws.ArgumentNullException);
            Assert.That(() => cache.Store(null!, Space, Candidates(), 0), Throws.ArgumentNullException);
            Assert.That(() => cache.Store(RepoId, Space, null!, 0), Throws.ArgumentNullException);
            Assert.That(() => cache.Invalidate(null!), Throws.ArgumentNullException);
        });
    }

    private sealed class FakeTimeProvider : TimeProvider
    {
        public DateTimeOffset UtcNow { get; set; } = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => UtcNow;
    }
}
