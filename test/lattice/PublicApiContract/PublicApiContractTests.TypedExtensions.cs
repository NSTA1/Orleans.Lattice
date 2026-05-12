using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    /// <summary>Test record used by typed extension tests.</summary>
    private sealed record TypedTestUser(string Name, int Age);

    // ── Typed GetAsync / SetAsync (JSON-default) ────────────────────────

    [Test]
    public async Task TypedSetAsync_then_TypedGetAsync_roundtrips_via_JSON_default()
    {
        var treeId = "pac-typed-roundtrip-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        await tree.SetAsync("u1", new TypedTestUser("alice", 30));
        var user = await tree.GetAsync<TypedTestUser>("u1");

        Assert.That(user, Is.Not.Null);
        Assert.That(user!.Name, Is.EqualTo("alice"));
        Assert.That(user.Age, Is.EqualTo(30));
    }

    [Test]
    public async Task TypedGetAsync_returns_default_for_missing_key()
    {
        var treeId = "pac-typed-missing-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var user = await tree.GetAsync<TypedTestUser>("absent");
        Assert.That(user, Is.Null);
    }

    [Test]
    public async Task TypedSetAsync_with_explicit_serializer_uses_it()
    {
        var treeId = "pac-typed-explicit-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var serializer = JsonLatticeSerializer<TypedTestUser>.Default;
        await tree.SetAsync("u", new TypedTestUser("bob", 22), serializer);
        var user = await tree.GetAsync<TypedTestUser>("u", serializer);

        Assert.That(user!.Name, Is.EqualTo("bob"));
    }

    [Test]
    public async Task TypedSetAsync_with_null_serializer_throws()
    {
        var treeId = "pac-typed-nullser-set-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.SetAsync("k", new TypedTestUser("x", 1), serializer: null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task TypedGetAsync_with_null_serializer_throws()
    {
        var treeId = "pac-typed-nullser-get-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.GetAsync<TypedTestUser>("k", serializer: null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ── Typed SetAsync with TTL ─────────────────────────────────────────

    [Test]
    public async Task TypedSetAsync_with_ttl_writes_with_expiry()
    {
        var treeId = "pac-typed-ttl-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        await tree.SetAsync("u", new TypedTestUser("eve", 40), TimeSpan.FromMinutes(5));
        var user = await tree.GetAsync<TypedTestUser>("u");

        Assert.That(user!.Name, Is.EqualTo("eve"));
    }

    // ── Typed GetWithVersionAsync ───────────────────────────────────────

    [Test]
    public async Task TypedGetWithVersionAsync_returns_value_and_version()
    {
        var treeId = "pac-typed-version-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("u", new TypedTestUser("ada", 36));

        var versioned = await tree.GetWithVersionAsync<TypedTestUser>("u");

        Assert.That(versioned.Value, Is.Not.Null);
        Assert.That(versioned.Value!.Name, Is.EqualTo("ada"));
        Assert.That(versioned.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task TypedGetWithVersionAsync_returns_zero_version_for_missing_key()
    {
        var treeId = "pac-typed-version-miss-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var versioned = await tree.GetWithVersionAsync<TypedTestUser>("absent");

        Assert.That(versioned.Value, Is.Null);
        Assert.That(versioned.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    // ── Typed SetIfVersionAsync ─────────────────────────────────────────

    [Test]
    public async Task TypedSetIfVersionAsync_succeeds_when_version_matches()
    {
        var treeId = "pac-typed-cas-ok-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("u", new TypedTestUser("alice", 30));

        var current = await tree.GetWithVersionAsync<TypedTestUser>("u");
        var ok = await tree.SetIfVersionAsync("u", new TypedTestUser("alice2", 31), current.Version);
        Assert.That(ok, Is.True);
    }

    [Test]
    public async Task TypedSetIfVersionAsync_fails_when_version_does_not_match()
    {
        var treeId = "pac-typed-cas-stale-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("u", new TypedTestUser("alice", 30));

        var ok = await tree.SetIfVersionAsync("u", new TypedTestUser("alice2", 31), HybridLogicalClock.Zero);
        Assert.That(ok, Is.False);
    }

    // ── Typed GetOrSetAsync ─────────────────────────────────────────────

    [Test]
    public async Task TypedGetOrSetAsync_returns_default_when_key_was_newly_written()
    {
        var treeId = "pac-typed-getorset-new-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var existing = await tree.GetOrSetAsync("u", new TypedTestUser("first", 1));
        Assert.That(existing, Is.Null);

        var written = await tree.GetAsync<TypedTestUser>("u");
        Assert.That(written!.Name, Is.EqualTo("first"));
    }

    [Test]
    public async Task TypedGetOrSetAsync_returns_existing_value_when_key_is_present()
    {
        var treeId = "pac-typed-getorset-exists-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("u", new TypedTestUser("incumbent", 99));

        var existing = await tree.GetOrSetAsync("u", new TypedTestUser("late", 1));
        Assert.That(existing, Is.Not.Null);
        Assert.That(existing!.Name, Is.EqualTo("incumbent"));
    }

    // ── Typed batch ─────────────────────────────────────────────────────

    [Test]
    public async Task TypedSetManyAsync_writes_multiple_entries()
    {
        var treeId = "pac-typed-setmany-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var entries = new List<KeyValuePair<string, TypedTestUser>>
        {
            new("u1", new TypedTestUser("a", 1)),
            new("u2", new TypedTestUser("b", 2)),
        };
        await tree.SetManyAsync(entries);

        Assert.That((await tree.GetAsync<TypedTestUser>("u1"))!.Name, Is.EqualTo("a"));
        Assert.That((await tree.GetAsync<TypedTestUser>("u2"))!.Name, Is.EqualTo("b"));
    }

    [Test]
    public async Task TypedGetManyAsync_returns_deserialized_dictionary()
    {
        var treeId = "pac-typed-getmany-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("u1", new TypedTestUser("a", 1));
        await tree.SetAsync("u2", new TypedTestUser("b", 2));

        var dict = await tree.GetManyAsync<TypedTestUser>(new List<string> { "u1", "u2", "absent" });
        Assert.That(dict, Has.Count.EqualTo(2));
        Assert.That(dict["u1"].Name, Is.EqualTo("a"));
        Assert.That(dict["u2"].Name, Is.EqualTo("b"));
    }

    [Test]
    public async Task TypedSetManyAtomicAsync_writes_all_entries()
    {
        var treeId = "pac-typed-setmanyatomic-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var entries = new List<KeyValuePair<string, TypedTestUser>>
        {
            new("u1", new TypedTestUser("a", 1)),
            new("u2", new TypedTestUser("b", 2)),
        };
        await tree.SetManyAtomicAsync(entries);

        Assert.That((await tree.GetAsync<TypedTestUser>("u1"))!.Age, Is.EqualTo(1));
        Assert.That((await tree.GetAsync<TypedTestUser>("u2"))!.Age, Is.EqualTo(2));
    }

    [Test]
    public async Task TypedSetManyAtomicAsync_with_opId_is_idempotent()
    {
        var treeId = "pac-typed-setmanyatomic-opid-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        var entries = new List<KeyValuePair<string, TypedTestUser>>
        {
            new("u1", new TypedTestUser("a", 1)),
        };
        var opId = "op-" + Guid.NewGuid().ToString("N");

        await tree.SetManyAtomicAsync(entries, opId);
        // Re-submit with the same opId - should be an idempotent no-op.
        Assert.That(
            async () => await tree.SetManyAtomicAsync(entries, opId),
            Throws.Nothing);
    }

    [Test]
    public async Task TypedSetManyAtomicAsync_with_null_entries_throws()
    {
        var treeId = "pac-typed-setmanyatomic-null-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.SetManyAtomicAsync<TypedTestUser>(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ── Typed BulkLoad ──────────────────────────────────────────────────

    [Test]
    public async Task TypedBulkLoadAsync_loads_entries()
    {
        var treeId = "pac-typed-bulk-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var entries = new List<KeyValuePair<string, TypedTestUser>>
        {
            new("u1", new TypedTestUser("a", 1)),
            new("u2", new TypedTestUser("b", 2)),
        };
        await tree.BulkLoadAsync(entries);

        Assert.That((await tree.GetAsync<TypedTestUser>("u1"))!.Name, Is.EqualTo("a"));
        Assert.That((await tree.GetAsync<TypedTestUser>("u2"))!.Name, Is.EqualTo("b"));
    }

    // ── Typed enumeration ───────────────────────────────────────────────

    [Test]
    public async Task TypedScanEntriesAsync_yields_deserialized_entries_in_order()
    {
        var treeId = "pac-typed-scan-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("a", new TypedTestUser("aname", 1));
        await tree.SetAsync("b", new TypedTestUser("bname", 2));

        var collected = new List<KeyValuePair<string, TypedTestUser>>();
        await foreach (var entry in tree.ScanEntriesAsync<TypedTestUser>())
        {
            collected.Add(entry);
        }

        Assert.That(collected.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
        Assert.That(collected[0].Value.Name, Is.EqualTo("aname"));
        Assert.That(collected[1].Value.Name, Is.EqualTo("bname"));
    }

    [Test]
    public async Task TypedScanEntriesAsync_with_explicit_serializer_uses_it()
    {
        var treeId = "pac-typed-scan-explicit-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("a", new TypedTestUser("first", 1));

        var serializer = JsonLatticeSerializer<TypedTestUser>.Default;
        var collected = new List<KeyValuePair<string, TypedTestUser>>();
        await foreach (var entry in tree.ScanEntriesAsync(serializer))
        {
            collected.Add(entry);
        }
        Assert.That(collected, Has.Count.EqualTo(1));
        Assert.That(collected[0].Value.Name, Is.EqualTo("first"));
    }
}
