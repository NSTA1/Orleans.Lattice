using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for empty-leaf reclaim running concurrently with writes.
/// <para>
/// The reclaim tests alongside this fixture all drive a quiescent tree: they
/// empty a range, run a pass, and inspect what is left. That is the shape the
/// feature was written and reviewed against, and it is exactly the shape that
/// cannot see its most serious failure mode. A fold is not one atomic step but
/// a sequence of grain calls - probe, descend, unroute, unlink, clear - and the
/// leaf mutation surface is <c>[AlwaysInterleave]</c>, so a write can be
/// routed, logged, applied and acknowledged in the middle of that sequence. On
/// a quiescent tree no write ever does, so nothing fails; every one of those
/// tests passed against an implementation that would erase such a write.
/// </para>
/// <para>
/// The loss is silent and permanent. After the fold the key routes to the
/// predecessor, whose projection checkpoint is already past the offset the
/// erased write occupies, so no replay re-materialises it: the caller was told
/// the write succeeded and the row is gone. These tests therefore assert
/// against a projection rebuilt from the write-ahead log rather than against
/// the live cache, because a cache read would report the row present right up
/// until the moment the tree was restarted.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafReclaimConcurrencyIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<(ILattice Router, IShardRootGrain Shard)> CreateSingleShardTreeAsync(string treeName)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry
        {
            ShardCount = 1,
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
        });
        return (_cluster.GrainFactory.GetGrain<ILattice>(treeName),
                _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0"));
    }

    private async Task<List<GrainId>> WalkChainAsync(IShardRootGrain shard)
    {
        var chain = new List<GrainId>();
        var leafId = await shard.GetLeftmostLeafIdAsync();

        while (leafId is { } id && chain.Count < 5_000)
        {
            chain.Add(id);
            leafId = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        return chain;
    }

    private async Task SeedAsync(ILattice router, int count)
    {
        for (var i = 0; i < count; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
    }

    /// <summary>
    /// Discards every leaf's in-memory projection and rebuilds it from the
    /// write-ahead log, which is what a restart would do.
    /// <para>
    /// This is the assertion that matters for a lost write, and the reason a
    /// plain read-back is not enough. A write erased by a fold is still in the
    /// predecessor's cache for as long as that activation lives, so a live read
    /// reports success; only a rebuild asks the durable log what the tree
    /// actually holds, and only the rebuild shows the row is gone.
    /// </para>
    /// </summary>
    private async Task RebuildEveryProjectionAsync(IShardRootGrain shard)
    {
        foreach (var leafId in await WalkChainAsync(shard))
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).RebuildProjectionFromWalAsync();
    }

    /// <summary>
    /// Takes every leaf in the chain cold and lets the runtime bring it back
    /// on the next call, which is what an ordinary idle collection does in a
    /// live deployment.
    /// <para>
    /// This is deliberately NOT
    /// <see cref="RebuildEveryProjectionAsync"/>. The admin rebuild sets the
    /// projection checkpoint to -1 and replays the whole log from offset 0;
    /// a natural reactivation replays from the persisted checkpoint instead.
    /// They are different code paths, so a defect visible through one cannot
    /// be assumed visible through the other - and only this one is reachable
    /// without an operator, which is what decides whether a read-visibility
    /// defect is an admin-only curiosity or something every cold leaf can do.
    /// </para>
    /// <para>
    /// The per-silo <c>LeafCacheGrain</c> in front of each leaf is a separate
    /// grain and is deliberately left alone: taking only the leaf cold is
    /// precisely the asymmetry that makes cache-versus-leaf divergence
    /// observable.
    /// </para>
    /// </summary>
    private async Task DeactivateEveryLeafAsync(IShardRootGrain shard)
    {
        foreach (var leafId in await WalkChainAsync(shard))
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).ForceDeactivateAsync();

        // DeactivateOnIdle is scheduled for after the current grain turn, so
        // the collection is not observable synchronously.
        await Task.Delay(500);
    }

    /// <summary>Finds an empty, non-head leaf: the shape a pass would fold.</summary>
    private async Task<(GrainId LeafId, LeafKeyRange Range)> FindReclaimCandidateAsync(IShardRootGrain shard)
    {
        var chain = await WalkChainAsync(shard);

        // Skip the head: it owns everything below the first separator and is
        // never a candidate, because it has no predecessor to inherit its
        // range.
        for (var i = 1; i < chain.Count; i++)
        {
            var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i]);
            if (await leaf.CountAsync() != 0) continue;

            var range = await leaf.GetKeyRangeAsync();
            if (range.LowKeyInclusive is null) continue;

            return (chain[i], range);
        }

        Assert.Fail("precondition: the emptied range must have left at least one empty non-head leaf to fold");
        return default;
    }

    /// <summary>Returns a key that routes into the given leaf's declared range.</summary>
    private static string KeyInRange(LeafKeyRange range)
    {
        // The low bound is inclusive, so it is always in range, and it is the
        // one key guaranteed to exist whatever the bounds happen to be.
        return range.LowKeyInclusive!;
    }

    /// <summary>
    /// One leaf as the diagnostic sees it: its position in the sibling chain,
    /// the span it declares, and whether routing still reaches it.
    /// <para>
    /// The last of those is the one no other check in this fixture can see. A
    /// leaf is reachable two independent ways - by walking the sibling chain,
    /// and by descending the internal nodes on a key - and a fold changes them
    /// at different moments. A leaf that is still chained and still declaring a
    /// span, but which a descent on its own low bound no longer lands on, is
    /// invisible to a chain walk, invisible to a span-tiling check, and is
    /// exactly the state in which a write to its range is handed to a leaf that
    /// does not declare the key.
    /// </para>
    /// </summary>
    private readonly record struct ChainLeaf(
        GrainId Id,
        string? Low,
        string? High,
        GrainId? RoutedOnOwnLow)
    {
        /// <summary>
        /// Delegates to the production predicate rather than restating it. The
        /// replay filter admits a row on the key-span axis via exactly this
        /// call, so a reimplementation here could drift from the behaviour the
        /// test is characterising and quietly report the wrong coverage.
        /// </summary>
        public bool Covers(string key) => SplitBoundary.Owns(key, Low, High);

        public string Span => $"[{Low ?? "-inf"}, {High ?? "+inf"})";

        /// <summary>
        /// True when this leaf is in the chain but a routing descent on its own
        /// low bound lands somewhere else, so no key can reach it any more.
        /// </summary>
        public bool IsRoutingOrphan =>
            Low is not null && RoutedOnOwnLow is not null && RoutedOnOwnLow != Id;
    }

    /// <summary>
    /// Renders a leaf chain as ordered declared spans, marking any leaf that
    /// routing no longer reaches.
    /// </summary>
    private static string DescribeChain(List<ChainLeaf> chain) =>
        string.Join(" -> ", chain.Select(
            l => l.IsRoutingOrphan ? $"{l.Span}!UNROUTED" : l.Span));

    /// <summary>
    /// Captures the chain as ordered spans plus, for each leaf, where a routing
    /// descent on that leaf's own low bound actually lands.
    /// </summary>
    private async Task<List<ChainLeaf>> SnapshotChainAsync(IShardRootGrain shard)
    {
        var snapshot = new List<ChainLeaf>();

        foreach (var leafId in await WalkChainAsync(shard))
        {
            var range = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).GetKeyRangeAsync();

            // Descending on the leaf's own low bound is the question "does
            // routing still consider this leaf to own its own range?". The head
            // has no low bound to descend on, so it is left unprobed rather
            // than probed with a null that means something else.
            var routed = range.LowKeyInclusive is { } low
                ? await shard.GetLeafIdForKeyAsync(low)
                : null;

            snapshot.Add(new ChainLeaf(leafId, range.LowKeyInclusive, range.HighKeyExclusive, routed));
        }

        return snapshot;
    }

    /// <summary>
    /// States, for each lost key, which leaf in the chain declared its span -
    /// or, if none did, the declared bounds on either side of it.
    /// <para>
    /// This is the discriminator the pass/fail result cannot carry, and it has
    /// to separate three mechanisms, not two. A lost key that a leaf does
    /// declare means the row was written somewhere other than the leaf that
    /// owns it: a stale-routing defect. A lost key that no leaf declares means
    /// the key fell outside every span - but that has two quite different
    /// causes, and they present identically:
    /// </para>
    /// <para>
    /// A key strictly interior to the gap is a genuine routing/span
    /// disagreement, where a fold left a range unowned. A key exactly equal to
    /// the predecessor's high bound is an inclusive/exclusive off-by-one in
    /// the widen - the span was taken as exclusive where routing takes it as
    /// inclusive, so precisely one key, the boundary key, belongs to nobody.
    /// The second is not a race at all, and no amount of reordering the fold
    /// will ever fix it; being sent back to the fold ordering by a message
    /// that cannot tell them apart is a guaranteed wasted cycle.
    /// </para>
    /// </summary>
    /// <summary>
    /// Separates the two read paths the shard root offers to the same leaf, so
    /// a lost key can be attributed to the right layer without a test-only
    /// probe or any access to internals.
    /// <para>
    /// <c>GetAsync</c> is served by <c>LeafCacheGrain</c>
    /// (<c>ShardRootGrain.TraverseForReadAsync</c> ends
    /// <c>ResolveLeafCacheGrain(leafId).GetAsync(key)</c>), while
    /// <c>GetWithVersionAsync</c> goes straight to the leaf
    /// (<c>TraverseForReadWithVersionAsync</c> ends
    /// <c>ResolveLeafGrain(leafId).GetWithVersionAsync(key)</c>). A row the
    /// direct path returns and the cached path does not is a read-cache
    /// coherence failure, not a lost write - and no chain, tiling, coverage or
    /// routing invariant can see the difference.
    /// </para>
    /// <para>
    /// The version also settles tombstoning without a fourth classifier
    /// branch: <see cref="VersionedValue.Value"/> is null for an absent OR a
    /// tombstoned row, and the HLC is <c>Zero</c> in both cases, so a non-null
    /// value proves the row is live and servable at the leaf.
    /// </para>
    /// </summary>
    private static async Task<string> DescribeReadPathsAsync(ILattice router, IShardRootGrain shard, string key)
    {
        var direct = await shard.GetWithVersionAsync(key);

        // A second cached read AFTER the first. LeafCacheGrain.GetAsync opens
        // with RefreshAsync, so if the first read's refresh repaired the view
        // this one succeeds - which distinguishes a cache that is permanently
        // wrong from one that is merely a refresh behind.
        var cachedRetry = await router.GetAsync(key);

        return $"read paths: cached (LeafCacheGrain) -> {(cachedRetry is null ? "null" : "HOLDS THE ROW")}"
            + $", direct-to-leaf (GetWithVersionAsync) -> "
            + $"{(direct.Value is null ? "null" : "HOLDS THE ROW")} (version {direct.Version})";
    }

    /// <summary>
    /// Reports what the leaf actually holds - the lost row's timestamp, any
    /// rows on the same leaf that are newer, the version it publishes, and
    /// its same-silo revision cookie - so the read divergence can be read off
    /// measurements rather than inferred.
    /// <para>
    /// Passing an empty <see cref="Orleans.Lattice.VersionVector"/> makes the
    /// delta's <c>callerClock</c> zero, so it returns everything the leaf
    /// holds and this probe reads the leaf's true contents rather than a
    /// filtered view of them.
    /// </para>
    /// <para>
    /// The timestamps are reported without a causal claim attached. An earlier
    /// revision of this probe labelled a newer sibling row as having "sealed
    /// out" the lost one, on the theory that the cache's refresh runs the
    /// <c>lww.Timestamp &gt; callerClock</c> filter in
    /// <c>BPlusLeafGrain.GetDeltaSinceAsync</c>. That theory is withdrawn:
    /// <c>LeafCacheGrain</c> refreshes through
    /// <c>GetDeltaSinceCursorAsync</c>, not <c>GetDeltaSinceAsync</c>, so that
    /// filter is not on the cache's code path at all. The timestamps were
    /// real; the label was an interpretation the probe never measured. The
    /// revision cookie below is the value that does gate the cache.
    /// </para>
    /// </summary>
    private async Task<string> DescribeLeafRowTimestampsAsync(GrainId leafId, string lostKey)
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var delta = await leaf.GetDeltaSinceAsync(new Orleans.Lattice.VersionVector());

        if (!delta.Entries.TryGetValue(lostKey, out var lostRow))
            return $"leaf-delta: leaf does not hold '{lostKey}' at all ({delta.Entries.Count} row(s))";

        var newer = delta.Entries
            .Where(e => e.Key != lostKey && e.Value.Timestamp > lostRow.Timestamp)
            .OrderByDescending(e => e.Value.Timestamp)
            .Take(3)
            .Select(e => $"{e.Key}@{e.Value.Timestamp}")
            .ToList();

        var published = string.Join(",", delta.Version.Entries.Select(e => $"{e.Key}->{e.Value}"));

        // The same-silo revision cookie for this leaf. Reported because it is
        // the gate that decides whether a co-located LeafCacheGrain refreshes
        // at all: absent (the state OnDeactivateAsync leaves behind) sends the
        // cache down the TTL branch, where it may serve a stale snapshot until
        // the TTL elapses; present-and-changed sends it down the revision
        // branch, which refreshes immediately. Replay materialises rows
        // without touching this, so an absent cookie here on a leaf that just
        // replayed is the defect, not an artefact of the probe.
        var cookie = Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.TryGetLeafRevision(leafId, out var rev)
            ? $"present({rev})"
            : "ABSENT";

        return $"leaf-delta: '{lostKey}'@{lostRow.Timestamp}; "
            + (newer.Count == 0
                ? "no row on this leaf is newer"
                : $"{newer.Count} newer row(s) on the same leaf [{string.Join(", ", newer)}]")
            + $"; published version {published}; revision cookie {cookie}";
    }

    private static string DescribeCoverage(
        List<string> lostKeys,
        List<ChainLeaf> chain,
        Dictionary<string, GrainId?> routedForKey)
    {
        if (lostKeys.Count == 0) return "(none)";

        return string.Join("; ", lostKeys.Select(key =>
        {
            // Where routing sends the key, and whether that leaf declares it.
            // This is the direct statement of the disagreement: the write path
            // admits by routing and the replay predicate admits by declared
            // span, so a key whose routed leaf does not declare it is a lost
            // write by construction, whatever else the chain looks like.
            var routedTo = routedForKey.TryGetValue(key, out var r) ? r : null;
            var routedIndex = routedTo is { } rid ? chain.FindIndex(l => l.Id == rid) : -1;
            var routing = routedTo is null
                ? "routing: (not captured)"
                : routedIndex < 0
                    ? $"routing: lands on {routedTo} which is NOT IN THE CHAIN - the row was handed to a leaf already folded out"
                    : $"routing: lands on leaf {routedIndex} {chain[routedIndex].Span}, which "
                        + (chain[routedIndex].Covers(key)
                            ? "DOES declare the key"
                            : "DOES NOT declare the key - routing and the replay predicate disagree, which loses the write");

            var ownerIndex = chain.FindIndex(l => l.Covers(key));
            if (ownerIndex >= 0)
            {
                var owner = chain[ownerIndex];
                return $"{key}: DECLARED by leaf {ownerIndex} {owner.Span} "
                    + $"(key==low: {key == owner.Low}, key==high: {key == owner.High}); {routing}. "
                    + (routedIndex == ownerIndex
                        ? "Routing and ownership AGREE at the end of the run, so the disagreement was transient: a record "
                            + "admitted while the span excluded it is dropped at replay, and once the leaf checkpoints "
                            + "past that offset, widening the span afterwards cannot recover it."
                        : "Routing and ownership DISAGREE: the row was written to a leaf other than the one that owns the key.");
            }

            var belowIndex = chain.FindLastIndex(
                l => l.High is not null && string.CompareOrdinal(l.High, key) <= 0);
            var aboveIndex = chain.FindIndex(
                l => l.Low is not null && string.CompareOrdinal(l.Low, key) > 0);

            var below = belowIndex >= 0 ? chain[belowIndex].Span : "(chain start)";
            var above = aboveIndex >= 0 ? chain[aboveIndex].Span : "(chain end)";

            var boundaryHit = belowIndex >= 0 && chain[belowIndex].High == key;

            return $"{key}: NO LEAF DECLARES IT - gap between {below} and {above}; {routing}. "
                + (boundaryHit
                    ? "The predecessor's high bound EQUALS the lost key: this is the inclusive/exclusive "
                        + "off-by-one signature in the widen, NOT a race, and reordering the fold cannot fix it."
                    : "The key is strictly interior to the gap (the predecessor's high bound does not equal it), "
                        + "which is consistent with a routing/span race rather than an off-by-one.");
        }));
    }

    // --- the window between the probe and the fold ---

    /// <summary>
    /// The regression test for the defect, reproduced exactly rather than
    /// raced for.
    /// <para>
    /// A fold decides on the evidence of a probe and then acts across several
    /// more grain calls. This drives that sequence by hand and lands a write in
    /// the middle of it, which is the interleaving a quiescent test can never
    /// produce and a racing one can only produce sometimes. The leaf must
    /// refuse to retire: it is no longer the empty leaf the probe described,
    /// and retiring it would destroy an acknowledged write.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_leaf_that_takes_a_write_after_its_probe_refuses_to_retire()
    {
        var treeName = $"reclaim-window-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var (leafId, range) = await FindReclaimCandidateAsync(shard);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);

        // Step one of a fold: the probe says the leaf is empty and may go.
        var probe = await leaf.GetReclaimProbeAsync();
        Assert.That(probe.LiveRowCount, Is.Zero, "precondition: the fold must begin from an empty leaf");
        Assert.That(probe.HasBlockingState, Is.False);

        // The window. A write is routed, logged, applied and acknowledged
        // while the fold is still several calls from the destructive step.
        var racingKey = KeyInRange(range);
        await router.SetAsync(racingKey, Encoding.UTF8.GetBytes("racing"));

        // The decision. It must be taken against the leaf as it is now, not as
        // the probe described it several calls ago.
        Assert.That(await leaf.TryBeginRetirementAsync(), Is.False,
            "a leaf that took a write after its probe must refuse to retire; retiring it would erase an acknowledged write");

        // And the write must survive what a restart would do to it.
        await RebuildEveryProjectionAsync(shard);
        Assert.That(await router.GetAsync(racingKey), Is.Not.Null,
            "the acknowledged write must survive a projection rebuild, which is where a silently erased write shows up");
    }

    /// <summary>
    /// A fold must not leave the absorbed split boundary published.
    /// <para>
    /// <c>SplitKey</c> means "keys at or above this value moved to my
    /// successor", and every <c>LeafCacheGrain</c> acts on it by pruning
    /// exactly those keys from its mirror on every refresh. That is sound
    /// while the successor owns them, and it is why the prune was written to
    /// be unconditional: a split leaf never regains keys above its split
    /// point, so re-applying the boundary is free.
    /// </para>
    /// <para>
    /// Leaf reclaim is the feature that makes that premise false. Folding an
    /// empty successor away widens this leaf back over the boundary, so it
    /// owns those keys again and replay materialises them - but the boundary
    /// it publishes still says they belong to somebody else. Every cache in
    /// the cluster then prunes rows the leaf genuinely holds, which reads as a
    /// null through the cache while a direct read returns the row, and stays
    /// that way because each refresh re-applies the same prune.
    /// </para>
    /// <para>
    /// The invariant is asserted at the seam the cache actually reads, not on
    /// internal state: whatever a leaf publishes as a split boundary must lie
    /// at or beyond its own high bound. A boundary strictly inside the leaf's
    /// declared span is the defect, and is a lie about ownership no matter how
    /// it arose.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_folded_leaf_never_publishes_a_split_boundary_inside_its_own_span()
    {
        var treeName = $"reclaim-splitkey-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);

        // Enough keys to split several times, so boundaries exist to absorb.
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        // Precondition. Without a published boundary going into the fold the
        // assertion below is vacuous and would pass against any
        // implementation, including one that never clears anything.
        var carriedBefore = 0;
        foreach (var leafId in await WalkChainAsync(shard))
        {
            var delta = await _cluster.GrainFactory
                .GetGrain<IBPlusLeafGrain>(leafId)
                .GetDeltaSinceCursorAsync(default);
            if (delta.SplitKey is not null) carriedBefore++;
        }

        Assert.That(carriedBefore, Is.GreaterThan(0),
            "precondition: at least one leaf must publish a split boundary before the fold, or this test proves nothing");

        await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        foreach (var leafId in await WalkChainAsync(shard))
        {
            var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var range = await leaf.GetKeyRangeAsync();
            var delta = await leaf.GetDeltaSinceCursorAsync(default);

            if (delta.SplitKey is null) continue;

            var inSpan = range.HighKeyExclusive is null
                || string.CompareOrdinal(delta.SplitKey, range.HighKeyExclusive) < 0;

            Assert.That(inSpan, Is.False,
                $"leaf {leafId} spans [{range.LowKeyInclusive ?? "-inf"}, {range.HighKeyExclusive ?? "+inf"}) "
                + $"but publishes SplitKey='{delta.SplitKey}' inside it; every cache will prune keys >= that "
                + "boundary, discarding rows this leaf owns");
        }

        // And the consequence, end to end. Every row the tree holds must be
        // readable through the router, which is the path that goes through the
        // caches the boundary misdirects.
        foreach (var leafId in await WalkChainAsync(shard))
        {
            foreach (var key in await _cluster.GrainFactory
                .GetGrain<IBPlusLeafGrain>(leafId)
                .GetKeysAsync())
            {
                Assert.That(await router.GetAsync(key), Is.Not.Null,
                    $"key '{key}' is held by leaf {leafId} but reads as absent through the router");
            }
        }
    }

    /// <summary>
    /// Once a leaf has latched itself retired it must refuse writes outright,
    /// not apply them. By that point the fold is committed: the leaf is on its
    /// way out of the routing table and the chain, so a write applied here
    /// would land in state that is about to be cleared. Refusing lets the
    /// caller re-route to the leaf that now owns the range.
    /// <para>
    /// This is the negative case: a latch that never clears. The write must
    /// fail <em>loudly</em> rather than be dropped, and it must not fail on the
    /// first rejection - a fold holds the latch for several grain calls, so an
    /// immediate failure would turn every ordinary fold into a caller-visible
    /// error. The elapsed-time assertion is what distinguishes "retried, then
    /// gave up" from "gave up at once"; without it this test passes against an
    /// implementation that has no retry at all.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_retired_leaf_refuses_writes_until_the_retirement_is_abandoned()
    {
        var treeName = $"reclaim-latch-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var (leafId, range) = await FindReclaimCandidateAsync(shard);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);

        Assert.That(await leaf.TryBeginRetirementAsync(), Is.True,
            "precondition: an empty leaf with no blocking state must be allowed to retire");

        var key = KeyInRange(range);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        Assert.That(async () => await router.SetAsync(key, Encoding.UTF8.GetBytes("refused")),
            Throws.Exception,
            "a retired leaf must refuse a write rather than apply it to state that is about to be cleared");
        clock.Stop();

        // The write is allowed to fail, but only after waiting out the
        // deadline. A near-instant failure means the retry path is not wired.
        Assert.That(clock.Elapsed, Is.GreaterThan(TimeSpan.FromMilliseconds(500)),
            "a write blocked by a retirement latch must retry until the deadline, not fail on the first rejection");

        // Abandoning must fully reopen the leaf, or an interrupted fold would
        // leave a perfectly good leaf permanently unwritable.
        await leaf.AbandonRetirementAsync();

        await router.SetAsync(key, Encoding.UTF8.GetBytes("accepted"));
        await RebuildEveryProjectionAsync(shard);
        Assert.That(await router.GetAsync(key), Is.Not.Null,
            "abandoning a retirement must leave the leaf writable again");
    }

    /// <summary>
    /// The positive case, and the one that justifies the retry existing at all.
    /// <para>
    /// A retirement latch is transient by design: a fold latches the leaf,
    /// re-points routing, unlinks it from the chain and clears it, then the
    /// range belongs to the predecessor and writes flow again. A write that
    /// arrives inside that window must not be turned into a caller-visible
    /// error just because it was unlucky with its timing - it must wait out the
    /// latch and land. This holds the latch for far longer than a real fold
    /// does (200ms against a fold's handful of grain calls) so that the wait is
    /// unambiguously exercised rather than raced for, while staying well inside
    /// the deadline.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_write_waiting_on_a_transient_retirement_latch_lands_when_the_latch_clears()
    {
        var treeName = $"reclaim-transient-latch-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var (leafId, range) = await FindReclaimCandidateAsync(shard);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);

        Assert.That(await leaf.TryBeginRetirementAsync(), Is.True,
            "precondition: an empty leaf with no blocking state must be allowed to retire");

        // Clear the latch part-way through the write's retry window, the way a
        // fold that decides to abandon does.
        var release = Task.Run(async () =>
        {
            await Task.Delay(200);
            await leaf.AbandonRetirementAsync();
        });

        var key = KeyInRange(range);
        await router.SetAsync(key, Encoding.UTF8.GetBytes("waited"));
        await release;

        await RebuildEveryProjectionAsync(shard);
        Assert.That(await router.GetAsync(key), Is.Not.Null,
            "a write that waited out a transient retirement latch must land, not fail");
    }

    /// <summary>
    /// The wait is bounded by a configured option, not a constant. An operator
    /// who has tuned the fold's own retry budget has to be able to move this
    /// with it, and a deployment that would rather fail fast has to be able to
    /// say so.
    /// <para>
    /// Asserting the configured deadline is <em>honoured</em> needs the elapsed
    /// time to separate it from the default, which is why the configured value
    /// is an order of magnitude below
    /// <see cref="LatticeOptions.DefaultLeafRetirementRetryDeadline"/>: the
    /// assertion fails if the resolver drops the property and the grain falls
    /// back to its default.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_configured_retirement_retry_deadline_bounds_how_long_a_write_waits()
    {
        var (router, shard) = await CreateSingleShardTreeAsync(
            SmallLeafClusterFixture.ShortRetirementDeadlineTreeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var (leafId, range) = await FindReclaimCandidateAsync(shard);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);

        Assert.That(await leaf.TryBeginRetirementAsync(), Is.True,
            "precondition: an empty leaf with no blocking state must be allowed to retire");

        var key = KeyInRange(range);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        Assert.That(async () => await router.SetAsync(key, Encoding.UTF8.GetBytes("refused")),
            Throws.Exception,
            "a latch that never clears must still surface as a failure");
        clock.Stop();

        Assert.That(clock.Elapsed, Is.LessThan(LatticeOptions.DefaultLeafRetirementRetryDeadline),
            "the configured deadline must govern the wait, not the default; a wait at or beyond the "
            + "default means the per-tree option was dropped between configuration and the grain");

        await leaf.AbandonRetirementAsync();
    }

    /// <summary>
    /// Pins the seed topology the racing test's coverage silently depends on.
    /// <para>
    /// This asserts a property of the FIXTURE, not of the tree. The racing test
    /// writes <c>k030..k044</c> in round 0, and its whole value rests on those
    /// keys landing in the range a fold is about to move. If a change to the
    /// seed, the delete range, or <c>SmallMaxLeafKeys</c> shifted the leaf
    /// boundaries so that round 0 wrote entirely into leaves no fold touches,
    /// every assertion in the racing test would still pass while it exercised
    /// nothing at all, and nothing would say so. This says so.
    /// </para>
    /// <para>
    /// <b>What the seed does NOT give you, measured rather than assumed.</b>
    /// The fold victim is <c>[k030, k032)</c> and its predecessor is
    /// <c>[k028, k030)</c>, whose high bound is exclusive. So <c>k030</c> is the
    /// victim's own low key and is routed to the victim exactly as <c>k031</c>
    /// is: there is NO round-0 key below the boundary, and therefore no key in
    /// the round that is safe by construction. Do not read the survival of any
    /// particular round-0 key as evidence that damage was narrow - every one of
    /// them is equally exposed, and a control key would have to be added on
    /// purpose. This paragraph exists because that assumption was made once, on
    /// this fixture, and was wrong.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_racing_seed_writes_into_the_range_a_fold_moves()
    {
        var treeName = $"reclaim-topology-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var chain = await SnapshotChainAsync(shard);
        var (victimId, victimRange) = await FindReclaimCandidateAsync(shard);

        var low = victimRange.LowKeyInclusive;
        Assert.That(low, Is.Not.Null, "precondition: the fold victim must declare a low bound");

        var victimIndex = chain.FindIndex(l => l.Id == victimId);
        Assert.That(victimIndex, Is.GreaterThan(0),
            "precondition: the fold victim must have a predecessor to absorb its range");

        // Round 0 of the racing test, reproduced exactly rather than described.
        var roundZeroKeys = Enumerable.Range(0, 15).Select(i => $"k{30 + i:D3}").ToList();

        var inVictimRange = roundZeroKeys.Where(k => victimRange.HighKeyExclusive is not { } high
            ? string.CompareOrdinal(k, low) >= 0
            : string.CompareOrdinal(k, low) >= 0 && string.CompareOrdinal(k, high) < 0).ToList();

        Assert.That(inVictimRange, Is.Not.Empty,
            $"round 0 must write at least one key into the fold victim's range "
            + $"[{low}, {victimRange.HighKeyExclusive ?? "+inf"}), or the racing test never drives a write "
            + "against a leaf that is being folded and its green result means nothing. "
            + "Chain: " + DescribeChain(chain));

        TestContext.Out.WriteLine(
            $"fold victim [{low}, {victimRange.HighKeyExclusive ?? "+inf"}); "
            + $"predecessor {chain[victimIndex - 1].Span}; "
            + $"round-0 keys inside the victim's range: [{string.Join(", ", inVictimRange)}]");
    }

    /// <summary>
    /// The loss primitive that every silent-data-loss defect in this area
    /// reduces to, now inverted: a leaf must refuse to admit a write for a key
    /// its own declared span excludes, and the row must land on the leaf that
    /// does declare it.
    /// <para>
    /// This test previously asserted the defect. The two admission rules
    /// disagreed by construction: the write path admitted by <i>routing</i>,
    /// performing no span check at all because the shard root was supposed to
    /// have resolved the owning leaf already, while replay admitted by
    /// <i>declared span</i>. Any interval in which routing resolved a key to a
    /// leaf that did not declare it was therefore a loss window, and the loss
    /// was invisible until a restart. The original body carried an explicit
    /// instruction to rewrite it as a refusal assertion if the write path ever
    /// started validating spans; issue #2137 made it do so, so this is that
    /// rewrite.
    /// </para>
    /// <para>
    /// This test races nothing. It writes one key directly to a leaf whose
    /// span excludes it, which is precisely what a stale routing entry causes.
    /// The refusal is asserted at the point of admission rather than after a
    /// rebuild, because a rebuild cannot distinguish the two mechanisms:
    /// <c>RebuildEveryProjectionAsync</c> replays from offset zero, so the
    /// declaring leaf re-materialises an orphaned row whether or not the write
    /// path validated the span. Admission is the property under test; survival
    /// is asserted afterwards as the end-to-end consequence.
    /// </para>
    /// <para>
    /// The end-to-end assertion the original made is kept and now holds for a
    /// stronger reason. Before, an acknowledged write survived a rebuild only
    /// if the declaring leaf's checkpoint happened to sit behind the offset the
    /// orphan occupied. Now the row is written to the declaring leaf in the
    /// first place, so its survival does not depend on a checkpoint accident.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_write_outside_a_leafs_declared_span_is_refused_and_lands_on_the_declaring_leaf()
    {
        var treeName = $"span-admission-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 40);

        var chain = await SnapshotChainAsync(shard);

        // Any leaf that declares an upper bound and has a successor will do:
        // the point is a key routing can reach but the span excludes, not a
        // particular position in the chain.
        var donorIndex = chain.FindIndex(l => l.High is not null);
        Assert.That(donorIndex, Is.GreaterThanOrEqualTo(0),
            "precondition: at least one leaf must declare an upper bound. Chain: " + DescribeChain(chain));

        var donor = chain[donorIndex];
        var high = donor.High!;

        // Strictly above the donor's exclusive upper bound, and absent from
        // the tree: the seed only ever writes "kNNN", so a suffixed key
        // collides with nothing and its fate is unambiguous.
        var orphanKey = high + "a";

        Assert.That(await router.GetAsync(orphanKey), Is.Null,
            "precondition: the probe key must not already exist anywhere in the tree");

        var declaringIndex = chain.FindIndex(l => l.Covers(orphanKey));
        Assert.That(declaringIndex, Is.GreaterThanOrEqualTo(0),
            $"precondition: some leaf in the chain must declare '{orphanKey}'. Chain: " + DescribeChain(chain));
        var declaringLeaf = chain[declaringIndex];
        Assert.That(declaringLeaf.Id, Is.Not.EqualTo(donor.Id),
            "precondition: the declaring leaf must not be the donor, or there is no out-of-span write to make");

        var donorGrain = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(donor.Id);
        var declaringGrain = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(declaringLeaf.Id);
        await donorGrain.SetAsync(orphanKey, Encoding.UTF8.GetBytes("out-of-span"));

        var onDonor = await donorGrain.GetAsync(orphanKey);
        var onDeclaring = await declaringGrain.GetAsync(orphanKey);
        var throughRouter = await router.GetAsync(orphanKey);

        TestContext.Out.WriteLine(
            $"wrote '{orphanKey}' directly to {donor.Id} declaring {donor.Span}"
            + Environment.NewLine
            + $"  leaf that declares the key: {declaringLeaf.Id} {declaringLeaf.Span}"
            + Environment.NewLine
            + $"  on the addressed leaf: {(onDonor is null ? "refused" : "ADMITTED")}"
            + Environment.NewLine
            + $"  on the declaring leaf: {(onDeclaring is null ? "ABSENT" : "present")}"
            + Environment.NewLine
            + "  chain: " + DescribeChain(chain));

        Assert.That(onDonor, Is.Null,
            $"leaf {donor.Id} declaring {donor.Span} admitted the out-of-span key '{orphanKey}'. "
            + "The commit path must check its declared range and forward a key it does not declare to "
            + "the leaf that does. Admitting it produces a row this leaf's own replay filter refuses to "
            + "reinstate, so the row's durability comes to rest on where an unrelated leaf's projection "
            + "checkpoint happens to sit.");

        Assert.That(onDeclaring, Is.Not.Null,
            $"the out-of-span write of '{orphanKey}' was refused by {donor.Id} but did not arrive at "
            + $"{declaringLeaf.Id}, which declares {declaringLeaf.Span}. A refusal that drops the write is "
            + "worse than the defect it replaces: the write path must forward, not discard.");

        Assert.That(throughRouter, Is.Not.Null,
            $"'{orphanKey}' was accepted by the tree but is not readable through the router.");

        // The original end-to-end invariant, kept: it now holds because the row
        // sits on the leaf whose replay filter admits it, rather than because a
        // checkpoint happened to fall on the right side of the orphan's offset.
        await RebuildEveryProjectionAsync(shard);

        Assert.That(await router.GetAsync(orphanKey), Is.Not.Null,
            $"an acknowledged write of '{orphanKey}' was lost by rebuilding projections from the log, "
            + $"even though it was placed on {declaringLeaf.Id}, which declares {declaringLeaf.Span} and "
            + "whose replay filter therefore admits it.");
    }
    // --- the whole path, under concurrent load ---

    /// <summary>
    /// The end-to-end form of the same property, and the one that also covers
    /// the routing gap a fold opens between unrouting a leaf and handing its
    /// range on.
    /// <para>
    /// Writes are issued into the emptied range while reclaim passes run
    /// against it, and every acknowledged write is then required to survive a
    /// projection rebuild. This catches both ways the window loses data: a
    /// write erased by the clear, and a write accepted into a range that the
    /// fold left owned by nobody, which the leaf serves from cache and drops
    /// on rebuild because the key falls outside the span its replay filter
    /// admits.
    /// </para>
    /// <para>
    /// The interleaving is not deterministic, which is why the two tests above
    /// exist. What is deterministic is the assertion: whatever order the calls
    /// happen to take, a write that was acknowledged must be readable
    /// afterwards, so this test can only fail when the tree has genuinely lost
    /// one.
    /// </para>
    /// </summary>
    [Test]
    public async Task Writes_racing_a_reclaim_pass_are_never_lost()
        => await RunWriteRaceAsync(withReclaim: true);

    /// <summary>
    /// The control for the test above: the identical write load and the
    /// identical rebuild, with the reclaim pass removed and nothing else
    /// changed.
    /// <para>
    /// This exists because a reproduction is not a diagnosis. The test above
    /// runs writes, a fold, and a projection rebuild together, so a red
    /// implicates all three equally - and every reclaim-shaped hypothesis this
    /// item has spent a cycle on assumed the fold without ever testing it. If
    /// this control also reds, the fold is not a participant and no amount of
    /// reclaim analysis can explain the failure; if it stays green while the
    /// test above reds, the fold is implicated by difference rather than by
    /// assumption. Either way the answer is cheap, and it is the only
    /// experiment here that can remove work rather than add it.
    /// </para>
    /// </summary>
    [Test]
    public async Task Writes_racing_no_reclaim_pass_are_never_lost()
        => await RunWriteRaceAsync(withReclaim: false);

    /// <summary>
    /// The same race and the same fold, but the projections are taken cold by
    /// an ordinary deactivation instead of the admin rebuild.
    /// <para>
    /// This decides the severity of anything the test above finds. The admin
    /// rebuild is an operator action; idle collection is not - the runtime
    /// does it continuously, unprompted, in every deployment. A read-visibility
    /// failure that needs the admin rebuild is a curiosity; one that any cold
    /// leaf can produce is a live defect that returns null for durable data.
    /// The two replay through different code paths, so neither result can be
    /// inferred from the other.
    /// </para>
    /// </summary>
    [Test]
    public async Task Writes_racing_a_reclaim_pass_survive_an_ordinary_deactivation()
        => await RunWriteRaceAsync(withReclaim: true, useAdminRebuild: false);

    /// <summary>
    /// A leaf that has replayed its projection from the write-ahead log must
    /// publish a same-silo revision cookie, so that a co-located
    /// <c>LeafCacheGrain</c> holding a cookie from the previous activation is
    /// forced to refresh rather than serve its own stale snapshot.
    /// <para>
    /// This is the unit-level guard for the read-visibility half of leaf
    /// reclaim. Replay reaches <c>Entries</c> through <c>StoreEntry</c>, which
    /// advances the delivery sequence but not the revision cookie, and
    /// <c>OnDeactivateAsync</c> removes the previous activation's registry
    /// entry. Absent a bump on the replay path the registry therefore has no
    /// entry at all, and <c>LeafCacheGrain.RefreshAsync</c>'s
    /// <c>_lastSeenPrimaryRevision &gt; 0 &amp;&amp; TryGetLeafRevision</c>
    /// guard fails, dropping the cache onto its TTL branch - where it keeps
    /// answering from a snapshot taken before the leaf's span was widened,
    /// for the whole TTL window, while the leaf itself holds the rows.
    /// </para>
    /// <para>
    /// Asserted directly on the registry rather than through a read, because
    /// a read-based assertion passes for either reason (cache refreshed, or
    /// cache never needed to) and so cannot distinguish the fix working from
    /// the fix being absent.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_publishes_a_revision_cookie_for_every_leaf()
    {
        var treeName = $"reclaim-cookie-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 24);

        // Rebuild deactivates each leaf, which removes its registry entry;
        // the next activation replays the log and must republish one.
        await RebuildEveryProjectionAsync(shard);

        // Walking the chain calls into every leaf, forcing the reactivation
        // whose replay path is under test.
        var chain = await WalkChainAsync(shard);
        Assert.That(chain, Is.Not.Empty, "expected a non-empty leaf chain to assert against");

        var missing = chain
            .Where(id => !Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain
                .TryGetLeafRevision(id, out _))
            .ToList();

        Assert.That(missing, Is.Empty,
            $"{missing.Count} of {chain.Count} leaf/leaves published no same-silo revision cookie "
            + "after replaying their projection from the WAL. A co-located LeafCacheGrain holding a "
            + "cookie from the previous activation will fall through to its TTL branch and can serve "
            + "a pre-replay snapshot - which is silent read loss for any row the leaf gained while "
            + "it was cold. Leaves without a cookie: "
            + string.Join(", ", missing));
    }

    /// <summary>
    /// A re-activated leaf must never republish a revision cookie value that
    /// was already observable under an earlier activation, because
    /// <c>LeafCacheGrain.RefreshAsync</c> compares cookies for equality and
    /// treats an equal pair as "provably fresh" - returning without
    /// refreshing at all.
    /// <para>
    /// This is the ABA guard, and it is a distinct failure from the missing
    /// cookie that <see cref="Replay_publishes_a_revision_cookie_for_every_leaf"/>
    /// covers. That guard passes with this defect present: publishing a
    /// cookie satisfies it whether or not the value collides. Seeding each
    /// activation's counter at zero makes the cookie a per-activation bump
    /// count rather than a value unique over the leaf's lifetime, so a
    /// re-activation can land on a value a cache still holds - and the cache
    /// then seals on its stale snapshot with no TTL to bound it, which is
    /// strictly worse than the absent-entry case.
    /// </para>
    /// <para>
    /// Asserted as "strictly greater than the value observed before
    /// deactivation" rather than by engineering a specific collision: that is
    /// the invariant the seed floor actually establishes, it holds for every
    /// leaf independently of how many times either activation happened to
    /// bump, and it fails deterministically under the zero-seed whenever the
    /// replaying activation bumps fewer times than the one that seeded the
    /// tree - which is the normal shape here, not a rare one.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reactivated_leaves_never_republish_an_earlier_cookie_value()
    {
        var treeName = $"reclaim-aba-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 24);

        var chain = await WalkChainAsync(shard);
        Assert.That(chain, Is.Not.Empty, "expected a non-empty leaf chain to assert against");

        var before = new Dictionary<GrainId, long>();
        foreach (var id in chain)
        {
            Assert.That(
                Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.TryGetLeafRevision(id, out var rev),
                Is.True,
                $"leaf {id} published no cookie while active, so there is nothing to collide with; "
                + "the fixture is not exercising what this guard claims to cover");
            before[id] = rev;
        }

        // Deactivates every leaf (dropping its registry entry) and forces the
        // next activation to replay, which is where a zero-seeded counter
        // restarts and can retread values captured above.
        await RebuildEveryProjectionAsync(shard);
        _ = await WalkChainAsync(shard);

        var collisions = new List<string>();
        foreach (var (id, previous) in before)
        {
            if (!Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.TryGetLeafRevision(id, out var now))
            {
                collisions.Add($"{id}: no cookie after replay (was {previous})");
                continue;
            }

            if (now <= previous)
            {
                collisions.Add($"{id}: {now} after replay, but {previous} was already observable");
            }
        }

        Assert.That(collisions, Is.Empty,
            $"{collisions.Count} of {before.Count} leaf/leaves republished a revision cookie that "
            + "was already observable under an earlier activation. A co-located LeafCacheGrain "
            + "holding the earlier value compares equal, takes the 'provably fresh' early return, "
            + "and never refreshes - an unbounded stale read, with no TTL to end it. Offending "
            + "leaves: " + string.Join("; ", collisions));
    }

    private async Task RunWriteRaceAsync(bool withReclaim, bool useAdminRebuild = true)    {
        var treeName = $"reclaim-race-{(withReclaim ? "fold" : "control")}"
            + $"-{(useAdminRebuild ? "rebuild" : "cold")}-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var acknowledged = new List<string>();

        // Read back immediately after each round, before any rebuild. This is
        // what tells the two failure modes apart when the assertion below
        // fires: a key present here and absent after the rebuild was filtered
        // out by the replay predicate, which means it was written to a leaf
        // that did not declare its span - a routing/span gap. A key already
        // absent here was destroyed by the clear, which means the retirement
        // interlock let a write through after the leaf was latched. The two
        // have different fixes, so the failure message names which.
        var missingBeforeRebuild = new List<string>();

        // Reclaim and write at the same time, several times over, so the
        // writes land at varying points in the fold sequence.
        for (var round = 0; round < 4; round++)
        {
            var reclaim = withReclaim
                ? shard.ReclaimEmptyLeavesAsync(int.MaxValue)
                : Task.FromResult(0);

            var writes = new List<Task>();
            var roundKeys = new List<string>();
            for (var i = 0; i < 15; i++)
            {
                // Straight back into the range the pass is folding away.
                var key = $"k{30 + (round * 15) + i:D3}";
                acknowledged.Add(key);
                roundKeys.Add(key);
                writes.Add(router.SetAsync(key, Encoding.UTF8.GetBytes($"race-{key}")));
            }

            await Task.WhenAll(writes);
            await reclaim;

            foreach (var key in roundKeys)
            {
                if (await router.GetAsync(key) is null) missingBeforeRebuild.Add(key);
            }
        }

        // Every write returned successfully above, so every key must be here -
        // and must still be here once the projections are rebuilt from the log
        // rather than served from a cache that has not been torn down yet.
        //
        // Snapshot the chain BEFORE the rebuild. Once a red fires, the only
        // question that matters is which leaf was supposed to own the lost key
        // and what span that leaf declared, and after the rebuild that
        // evidence is gone. Two very different defects present identically in
        // the pass/fail result: a key covered by no leaf at all (a gap the
        // fold opened and never closed) and a key held by one leaf while
        // declared by another (a row written through stale routing). They have
        // different fixes, so the failure message has to name which.
        var preRebuildChain = await SnapshotChainAsync(shard);

        // Where routing sends each acknowledged key, captured before the
        // rebuild for the same reason the chain is: after the rebuild the tree
        // has been re-derived and the evidence of the disagreement is gone.
        var routedForKey = new Dictionary<string, GrainId?>();
        foreach (var key in acknowledged)
            routedForKey[key] = await shard.GetLeafIdForKeyAsync(key);

        // A leaf that is still in the chain, still declaring a span, but which
        // routing no longer reaches is a state no other check here can see, and
        // it is a silent-loss window by construction: writes into its declared
        // range are handed to whichever leaf routing does reach, and that leaf
        // does not declare them.
        var routingOrphans = preRebuildChain
            .Where(l => l.IsRoutingOrphan)
            .Select(l => $"leaf {preRebuildChain.IndexOf(l)} {l.Span} is chained but a descent on its own low bound lands on {l.RoutedOnOwnLow}")
            .ToList();

        if (useAdminRebuild)
        {
            await RebuildEveryProjectionAsync(shard);
        }
        else
        {
            await DeactivateEveryLeafAsync(shard);
        }

        var lost = new List<string>();
        foreach (var key in acknowledged)
        {
            if (await router.GetAsync(key) is null) lost.Add(key);
        }

        var erased = lost.Intersect(missingBeforeRebuild).ToList();
        var filtered = lost.Except(missingBeforeRebuild).ToList();

        // Gather every diagnostic BEFORE asserting any of them. A fixture that
        // short-circuits on the first tripped condition reports the least
        // information on exactly the runs that carry the most: until this was
        // restructured the chain-tiling check sat after the loss assertion and
        // so had never once been evaluated on a failing run, which is the only
        // kind of run whose tiling anyone wants to know about.
        var tilingBreaks = new List<string>();
        var postRebuildChain = await WalkChainAsync(shard);
        for (var i = 0; i < postRebuildChain.Count - 1; i++)
        {
            var here = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(postRebuildChain[i]).GetKeyRangeAsync();
            var next = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(postRebuildChain[i + 1]).GetKeyRangeAsync();

            if (here.HighKeyExclusive != next.LowKeyInclusive)
            {
                tilingBreaks.Add(
                    $"leaf {i} ends at '{here.HighKeyExclusive ?? "+inf"}' but leaf {i + 1} begins at '{next.LowKeyInclusive ?? "-inf"}'");
            }
        }

        // Ground truth for the replay predicate. ShouldApplyDuringReplay is a
        // conjunction over two independent axes - the key-span axis
        // (SplitBoundary.Owns) and the shard axis (IsShardOwnedDuringReplay) -
        // and a coverage report built from spans alone is blind to the second.
        // A key is lost if no leaf satisfies BOTH, which a span-only report
        // cannot distinguish from full coverage.
        //
        // Rather than recompute either axis (which risks diverging from the
        // production predicate), this probes the OUTCOME: after the rebuild,
        // which leaf, if any, actually holds the key. That is the conjunction's
        // verdict as production evaluated it.
        //
        // Read it with the erased/filtered split above:
        //   held by no leaf, and absent before the rebuild  -> never durable, or tombstoned
        //   held by no leaf, but present before the rebuild -> the durable row failed the conjunction
        //   held by a leaf the router cannot reach          -> a routing defect, not a replay one
        // The shard's own moved-away seal. router.GetAsync goes through the
        // shard front door and the leaf; a direct leaf GetAsync goes through
        // the leaf only. If the shard carries a seal, those two can disagree
        // on a single leaf, with no chain or coverage invariant able to see it.
        var shardMoved = await shard.CountWithMovedAwayAsync();
        var shardSeal = shardMoved.MovedAwaySlots is { Length: > 0 } sms
            ? string.Join(",", sms)
            : "(none)";

        var holders = new List<string>();
        foreach (var key in lost)
        {
            var holding = new List<string>();
            for (var i = 0; i < postRebuildChain.Count; i++)
            {
                var leafId = postRebuildChain[i];
                if (await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).GetAsync(key) is null)
                    continue;

                var range = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).GetKeyRangeAsync();
                var span = $"[{range.LowKeyInclusive ?? "-inf"}, {range.HighKeyExclusive ?? "+inf"})";
                var declares = SplitBoundary.Owns(key, range.LowKeyInclusive, range.HighKeyExclusive);
                var probe = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).GetReclaimProbeAsync();
                holding.Add(
                    $"chain index {i} {span} (declares: {declares}, blockingState: {probe.HasBlockingState}) {leafId}");
            }

            // Where a descent lands NOW, after the rebuild. routedForKey was
            // captured before it, so comparing the two separates routing that
            // was already wrong from routing the rebuild moved. Printing the
            // ids makes holder-vs-routed an EQUALITY rather than an inference:
            // a single leaf can hold a row in cache and still return null to a
            // router read, so "holder is not the routed leaf" cannot be
            // concluded from the disagreement alone.
            var routedNow = await shard.GetLeafIdForKeyAsync(key);
            var routedNowDesc = "(no leaf)";
            if (routedNow is { } rn)
            {
                var rnRange = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(rn).GetKeyRangeAsync();
                var rnProbe = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(rn).GetReclaimProbeAsync();
                var rnDirect = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(rn).GetAsync(key);
                routedNowDesc =
                    $"[{rnRange.LowKeyInclusive ?? "-inf"}, {rnRange.HighKeyExclusive ?? "+inf"}) "
                    + $"(blockingState: {rnProbe.HasBlockingState}, direct leaf read: "
                    + $"{(rnDirect is null ? "null" : "HOLDS THE ROW")}) {rn}";
            }

            holders.Add(
                $"'{key}' held by: {(holding.Count == 0 ? "NO LEAF" : string.Join(" AND ", holding))}"
                + $" | routed after rebuild to: {routedNowDesc}"
                + $" | routed before rebuild to: {(routedForKey.TryGetValue(key, out var rb) ? rb?.ToString() ?? "(none)" : "(not captured)")}"
                + $" | shard moved-away slots: {shardSeal}"
                + $" | {await DescribeReadPathsAsync(router, shard, key)}"
                + $" | {(routedNow is { } rl ? await DescribeLeafRowTimestampsAsync(rl, key) : "leaf-delta: (no routed leaf)")}");
        }

        Assert.Multiple(() =>
        {
            Assert.That(lost, Is.Empty,
                $"{lost.Count} acknowledged write(s) did not survive a reclaim pass. "
                + $"Erased before any rebuild (the clear ran over a live write, so the retirement interlock leaked): [{string.Join(", ", erased)}]. "
                + $"Readable before the rebuild and not readable through the router after it - which is a "
                + $"measurement, not yet a cause: it is consistent with the row having been filtered at "
                + $"replay (span/predicate disagreement) AND with the row still being present but hidden "
                + $"by a stale cache. Compare against the direct-to-leaf read below before concluding "
                + $"which: [{string.Join(", ", filtered)}]. "
                + $"Coverage of each lost key in the pre-rebuild chain: {DescribeCoverage(lost, preRebuildChain, routedForKey)}. "
                + $"Which leaf actually holds each lost key after the rebuild (the replay conjunction's own verdict): {string.Join("; ", holders)}. "
                + $"Pre-rebuild chain: {DescribeChain(preRebuildChain)}");

            // Asserted in its own right, NOT folded into the loss check, and
            // that is deliberate. A routing orphan is a loss WINDOW: writes into
            // its declared range are handed to a leaf that does not declare
            // them. Whether this particular run was unlucky enough to issue such
            // a write is a coin toss, so a loss-only assertion catches this
            // INSTANCE with probability well under one. Asserting the structure
            // catches the CLASS deterministically, on every run, whether or not
            // a write happened to land in the window.
            //
            // That is the difference between a test that fails once every forty
            // runs and one that fails every time, so do not "simplify" this into
            // the loss check above.
            Assert.That(routingOrphans, Is.Empty,
                "the chain contains leaves routing no longer reaches, so writes into their declared ranges "
                + $"are handed to leaves that do not declare them: {string.Join("; ", routingOrphans)}");

            // A fold that opened a gap it never closed shows up here too: the
            // chain must still tile the keyspace with no span owned by nobody.
            Assert.That(tilingBreaks, Is.Empty,
                $"after racing writes and reclaim the leaf chain no longer tiles the keyspace: {string.Join("; ", tilingBreaks)}");
        });
    }

    // --- the walk bound ---

    /// <summary>
    /// A pass must stop walking when its fold budget is spent, not merely stop
    /// folding.
    /// <para>
    /// Probing a leaf activates it and counts its rows, and the pass holds the
    /// shard root's activation turn while it runs, so a pass that walked on
    /// after spending its budget would put every remaining leaf in the shard
    /// through an activation and a count for a decision it could no longer
    /// act on - head-of-line blocking every read and write on the shard behind
    /// it.
    /// </para>
    /// <para>
    /// Stopping early is only safe if the next pass picks up where this one
    /// stopped, so the two properties are tested together: no pass folds more
    /// than it was asked to, and repeated small passes still reclaim the whole
    /// chain rather than re-walking the same prefix forever.
    /// </para>
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_stops_at_its_budget_and_resumes_on_the_next_pass()
    {
        var treeName = $"reclaim-budget-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);

        var grown = (await WalkChainAsync(shard)).Count;
        Assert.That(grown, Is.GreaterThan(8), "precondition: the seed must actually have split the tree");

        await router.DeleteRangeAsync("k030", "k090");

        // One leaf per pass. Each pass must honour the budget exactly, and the
        // passes together must still drain the chain: that is what proves the
        // bounded walk resumes rather than restarting at the head every time.
        //
        // A pass that folds nothing is not evidence that the chain is drained.
        // A bounded walk resumes where the last one stopped, so a pass can
        // legitimately spend its whole window on leaves that hold rows, fold
        // nothing, reach the tail and wrap back to the head for the next one.
        // The loop therefore runs a fixed number of passes rather than
        // stopping at the first empty-handed one.
        var totalReclaimed = 0;
        for (var pass = 0; pass < 60; pass++)
        {
            var reclaimed = await shard.ReclaimEmptyLeavesAsync(1);
            Assert.That(reclaimed, Is.LessThanOrEqualTo(1), "a pass must never fold more than its budget");
            totalReclaimed += reclaimed;
        }

        Assert.That(totalReclaimed, Is.GreaterThan(0),
            "single-leaf passes must make progress; a bounded walk that never resumed would keep re-walking the same prefix");

        var settled = (await WalkChainAsync(shard)).Count;
        Assert.That(settled, Is.EqualTo(grown - totalReclaimed),
            "the chain must be exactly as much shorter as the passes claimed to have folded");

        // An unbounded pass now has nothing left to find, so the small passes
        // reached everything a single large one would have.
        Assert.That(await shard.ReclaimEmptyLeavesAsync(int.MaxValue), Is.Zero,
            "repeated bounded passes must reclaim everything an unbounded pass would, or the walk is not resuming");

        Assert.That(await router.CountAsync(), Is.EqualTo(60),
            "reclaim must not have disturbed the rows that are still live");
    }

    /// <summary>
    /// The drain is bounded by a distribution, not by tree size, and this is
    /// the case that distinguishes the two.
    /// <para>
    /// A pass probes at most <c>maxLeaves * 16</c> leaves from where it starts.
    /// Here the emptied range begins <em>beyond</em> that window, so the first
    /// pass spends its entire budget on leaves that hold live rows and folds
    /// nothing. Only a walk that resumes where the last one stopped ever
    /// reaches the candidates, which is the property an operator is relying on
    /// when they ask whether an already-bloated tree drains rather than merely
    /// stops growing. A walk that restarted at the head each time would fold
    /// nothing here, for ever, while looking exactly like a tree with nothing
    /// to reclaim.
    /// </para>
    /// <para>
    /// The precondition is asserted rather than assumed: the test fails loudly
    /// if the seed happens to place a candidate inside the first window, which
    /// would leave it silently proving nothing.
    /// </para>
    /// </summary>
    [Test]
    public async Task Empty_leaves_beyond_the_first_probe_window_are_still_drained()
    {
        const int foldBudget = 1;
        const int probesPerFold = 16;
        const int firstWindow = foldBudget * probesPerFold;

        var treeName = $"reclaim-beyond-window-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 200);

        var grown = await WalkChainAsync(shard);
        Assert.That(grown.Count, Is.GreaterThan(firstWindow + 4),
            "precondition: the seed must produce a chain longer than one pass's probe window");

        // Empty only the tail, leaving a long contiguous run of live leaves at
        // the head - the distribution that actually stalls a non-resuming walk.
        await router.DeleteRangeAsync("k120", "k200");

        var firstCandidate = -1;
        for (var i = 1; i < grown.Count; i++)
        {
            if (await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(grown[i]).CountAsync() != 0) continue;
            firstCandidate = i;
            break;
        }

        Assert.That(firstCandidate, Is.GreaterThan(firstWindow),
            $"precondition: the first empty leaf must lie beyond the first {firstWindow}-leaf probe window, "
            + "or this test proves nothing about resuming");

        // One leaf per pass, so every pass's window is 16 leaves wide and the
        // candidates are unreachable without a cursor that carries forward.
        var totalReclaimed = 0;
        for (var pass = 0; pass < 120; pass++)
        {
            totalReclaimed += await shard.ReclaimEmptyLeavesAsync(foldBudget);
        }

        Assert.That(totalReclaimed, Is.GreaterThan(0),
            "empty leaves beyond the first probe window must still be reclaimed; a walk that restarted at "
            + "the head every pass would never reach them and the tree would never drain");

        Assert.That((await WalkChainAsync(shard)).Count, Is.EqualTo(grown.Count - totalReclaimed),
            "the chain must be exactly as much shorter as the passes claimed to have folded");

        Assert.That(await router.CountAsync(), Is.EqualTo(120),
            "draining the tail must not disturb the live rows at the head");
    }
}
