using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the write-path declared-span admission rule: a leaf
/// must never admit a key its own <c>[LowKeyInclusive, HighKeyExclusive)</c>
/// range excludes, and must forward such a key to the leaf that does declare it.
/// <para>
/// Two different rules decide whether a leaf owns a key. The write path used to
/// admit purely by <b>routing</b> - a write descended the internal nodes, landed
/// on whichever leaf the separators currently pointed at, and was acknowledged
/// and WAL-appended without the leaf ever consulting its own declared span.
/// Replay admits by <b>declared span</b>
/// (<c>BPlusLeafGrain.ShouldApplyDuringReplay</c> calls
/// <see cref="SplitBoundary.Owns"/>). Any window in which the two disagree
/// produced an <i>orphan row</i>: a row held and acknowledged by a leaf that
/// does not declare it, and which that leaf's own replay then drops.
/// </para>
/// <para>
/// An orphan is not by itself a lost write. The WAL is shard-wide, so the leaf
/// that legitimately declares the key admits the same row during its own replay
/// and a rebuild relocates the row rather than dropping it. Loss needs a third
/// condition nothing controls: the declaring leaf's projection checkpoint must
/// already be past the offset the orphan occupies, so its replay never reaches
/// the row. The span disagreement creates the orphan; the checkpoint decides
/// whether it is recoverable. These tests therefore assert the property that is
/// actually under the code's control - that no orphan is created at all - rather
/// than reaching for the checkpoint race, which is a scheduling accident and not
/// a thing a deterministic test can pin. See issue #2137.
/// </para>
/// <para>
/// The window was previously guarded only by
/// <c>SplitState == SplitInProgress</c>, and that guard is dead code on any leaf
/// that has already split once: <c>SplitState</c> is a join-merged one-way
/// ratchet (<c>Unsplit &lt; SplitInProgress &lt; SplitComplete</c>) and
/// <c>Unsplit</c> is written nowhere, so a donor sits at
/// <see cref="SplitState.SplitComplete"/> permanently after its first split and
/// <c>BeginSplit</c> cannot lower it again. Every leaf these tests write to has
/// already split, so every one of them exercises the state in which the old
/// guard could not fire. That is deliberate: keying admission off the declared
/// span instead of off <c>SplitState</c> is what makes the fix robust to the
/// ratchet.
/// </para>
/// <para>
/// Every write here is addressed to the leaf grain <b>directly</b>, bypassing
/// the shard root's descent. That is the point: it is the deterministic
/// stand-in for stale routing, which is otherwise reachable only inside a
/// genuinely racy split window. It reproduces exactly the input a leaf sees when
/// routing sends it a key it no longer declares.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafSpanAdmissionIntegrationTests
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

    private IBPlusLeafGrain Leaf(GrainId id) => _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id);

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

    /// <summary>
    /// The state this whole fixture is about: a leaf that has split at least
    /// once (so it declares a bounded high key and sits at
    /// <see cref="SplitState.SplitComplete"/>), plus a key that its declared
    /// span excludes and the leaf further along the chain that does declare it.
    /// </summary>
    private sealed record SpanFixture(
        GrainId Donor,
        LeafKeyRange DonorRange,
        GrainId Declaring,
        string OutOfSpanKey);

    /// <summary>
    /// Grows a single-shard tree until its leftmost leaf has split, then picks a
    /// key immediately above that leaf's high bound and resolves the leaf that
    /// genuinely declares it by walking the sibling chain.
    /// </summary>
    private async Task<SpanFixture> BuildSealedDonorAsync(ILattice router, IShardRootGrain shard)
    {
        for (var i = 0; i < 40; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var donor = (await shard.GetLeftmostLeafIdAsync())!.Value;
        var donorRange = await Leaf(donor).GetKeyRangeAsync();
        Assert.That(donorRange.HighKeyExclusive, Is.Not.Null,
            "precondition: the leftmost leaf must have split, so it declares a bounded high key and "
            + "sits permanently at SplitState.SplitComplete - the state in which the old "
            + "SplitInProgress-only forwarding guard is dead code");

        // Ordinally just above the donor's high bound, so the donor's declared
        // span excludes it while the very next leaf in the chain declares it.
        var outOfSpanKey = donorRange.HighKeyExclusive + "a";
        Assert.That(
            SplitBoundary.Owns(outOfSpanKey, donorRange.LowKeyInclusive, donorRange.HighKeyExclusive),
            Is.False,
            "precondition: the chosen key must genuinely fall outside the donor's declared span");

        var declaring = await ResolveDeclaringLeafAsync(donor, outOfSpanKey);
        return new SpanFixture(donor, donorRange, declaring, outOfSpanKey);
    }

    /// <summary>
    /// Walks the sibling chain rightwards from <paramref name="from"/> until it
    /// finds the leaf whose declared span owns <paramref name="key"/>. Resolving
    /// this by walking rather than assuming "the immediate next sibling" keeps
    /// the assertions honest if the tree happens to lay out differently.
    /// </summary>
    private async Task<GrainId> ResolveDeclaringLeafAsync(GrainId from, string key)
    {
        var current = from;
        for (var hop = 0; hop < 64; hop++)
        {
            var range = await Leaf(current).GetKeyRangeAsync();
            if (SplitBoundary.Owns(key, range.LowKeyInclusive, range.HighKeyExclusive))
                return current;

            var next = await Leaf(current).GetNextSiblingAsync();
            Assert.That(next, Is.Not.Null,
                $"the chain ended before any leaf declared '{key}', so the tree has a coverage gap");
            current = next!.Value;
        }

        Assert.Fail($"no leaf in the chain declares '{key}' within a bounded walk");
        return default;
    }

    // The conditional batched write path needs a guard to evaluate. These mirror
    // the shapes BPlusLeafGrainTests.ConditionalSetMany.cs uses, so the two
    // fixtures agree on what "matching" means.
    private sealed record Scored(int Score);

    private static byte[] ScoredJson(int score) => Encoding.UTF8.GetBytes($"{{\"Score\":{score}}}");

    private static LatticePredicateNode ScoreAtLeast(int threshold) =>
        LatticePredicatePushdown.Compile<Scored>(
            s => s.Score >= threshold, JsonLatticeSerializer<Scored>.Default);

    /// <summary>
    /// The core claim. A key handed straight to a leaf whose declared span
    /// excludes it must not be admitted there. Before the fix the donor
    /// acknowledged and WAL-appended the row, its own replay filter then dropped
    /// it, and whether the acknowledged write survived came down to where the
    /// declaring leaf's checkpoint happened to be.
    /// </summary>
    [Test]
    public async Task A_leaf_refuses_to_admit_a_key_its_declared_span_excludes()
    {
        var (router, shard) = await CreateSingleShardTreeAsync($"span-set-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);
        var value = Encoding.UTF8.GetBytes("out-of-span");

        await Leaf(f.Donor).SetAsync(f.OutOfSpanKey, value);

        var onDonor = await Leaf(f.Donor).GetAsync(f.OutOfSpanKey);
        var onDeclaring = await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey);
        var throughRouter = await router.GetAsync(f.OutOfSpanKey);

        Assert.Multiple(() =>
        {
            Assert.That(onDonor, Is.Null,
                "the donor's declared span excludes this key, and its own replay filter drops rows it does "
                + "not declare, so admitting the row here creates an orphan whose survival depends on the "
                + "declaring leaf's checkpoint position");
            Assert.That(onDeclaring, Is.EqualTo(value),
                "the write must be forwarded to the leaf that declares the key, not dropped");
            Assert.That(throughRouter, Is.EqualTo(value),
                "and it must still be readable end to end through the router");
        });
    }

    /// <summary>
    /// The batched write path has its own commit funnel
    /// (<c>CommitSetManyAsync</c>) that does not run the per-key path, so a
    /// guard applied only to the single-key entry point would leave the batch
    /// path admitting orphans. Mixing an in-span key with an out-of-span one in
    /// one call pins that the batch is split by span rather than accepted or
    /// forwarded wholesale.
    /// </summary>
    [Test]
    public async Task A_batched_write_splits_by_declared_span_instead_of_admitting_wholesale()
    {
        var (router, shard) = await CreateSingleShardTreeAsync($"span-setmany-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);

        var inSpanKey = (f.DonorRange.LowKeyInclusive ?? "k") + "-in-span";
        Assert.That(
            SplitBoundary.Owns(inSpanKey, f.DonorRange.LowKeyInclusive, f.DonorRange.HighKeyExclusive),
            Is.True,
            "precondition: the control key must fall inside the donor's declared span");

        var inValue = Encoding.UTF8.GetBytes("in");
        var outValue = Encoding.UTF8.GetBytes("out");

        await Leaf(f.Donor).SetManyAsync(
        [
            new KeyValuePair<string, byte[]>(inSpanKey, inValue),
            new KeyValuePair<string, byte[]>(f.OutOfSpanKey, outValue),
        ]);

        var inOnDonor = await Leaf(f.Donor).GetAsync(inSpanKey);
        var outOnDonor = await Leaf(f.Donor).GetAsync(f.OutOfSpanKey);
        var outOnDeclaring = await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey);

        Assert.Multiple(() =>
        {
            Assert.That(inOnDonor, Is.EqualTo(inValue),
                "the in-span entry of the batch belongs here and must still be committed locally");
            Assert.That(outOnDonor, Is.Null,
                "the out-of-span entry of the batch must not be admitted on the donor");
            Assert.That(outOnDeclaring, Is.EqualTo(outValue),
                "the out-of-span entry must be forwarded to the leaf that declares it");
        });
    }

    /// <summary>
    /// <c>MergeManyAsync</c> is a second write entry point with the identical
    /// SplitInProgress-only structure, which is why the fix is keyed off the
    /// declared span rather than applied at one call site. Replication apply,
    /// backup restore, and the reshard import all reach the leaf through it.
    /// </summary>
    [Test]
    public async Task A_merge_splits_by_declared_span_instead_of_admitting_wholesale()
    {
        var (router, shard) = await CreateSingleShardTreeAsync($"span-merge-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);

        var stamp = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.Ticks, Counter = 0 };
        var outValue = Encoding.UTF8.GetBytes("merged-out");

        await Leaf(f.Donor).MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            [f.OutOfSpanKey] = new LwwValue<byte[]> { Value = outValue, Timestamp = stamp },
        });

        var mergedOnDonor = await Leaf(f.Donor).GetAsync(f.OutOfSpanKey);
        var mergedOnDeclaring = await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey);

        Assert.Multiple(() =>
        {
            Assert.That(mergedOnDonor, Is.Null,
                "a merged row outside the donor's declared span must not be admitted here either");
            Assert.That(mergedOnDeclaring, Is.EqualTo(outValue),
                "it must be forwarded to the leaf that declares the key");
        });
    }

    /// <summary>
    /// The cross-shard migration twin of the merge case above, and the
    /// regression guard for issue #3117.
    /// <para>
    /// Migration imports were previously <b>exempt</b> from declared-span
    /// admission (<c>!isCrossShardMigration &amp;&amp; ContainsOutOfSpanKey(...)</c>).
    /// The stated reason was that migration is a topology-seeding operation
    /// whose coordinator sets the destination's range as a separate step, so
    /// its keys are legitimately outside the range when they arrive. That
    /// premise is true, but the exemption was never needed to honour it: a
    /// destination whose range is not set yet has two null bounds, so
    /// <c>HasDeclaredSpan</c> is false and the scan is skipped anyway, and a
    /// freshly-seeded destination has no chain pointers, so the forward falls
    /// open to a local commit. On the seeding shape the exemption was a no-op.
    /// </para>
    /// <para>
    /// The only shape it actually changed was the one it was never meant to
    /// cover, and which this test pins: a leaf whose span was narrowed by its
    /// <b>own</b> split, receiving a late import for a key that split had moved
    /// to its sibling. The exemption re-created that key locally as an
    /// <c>IsMigrated=true</c> pre-saga row on a leaf that no longer owns it. The
    /// asymmetric migration-vs-foreground guard in <c>MergeIntoStateAsync</c>
    /// cannot suppress it, because that guard fires only when the destination
    /// already holds a non-migrated entry and the split had removed the row
    /// entirely. A later split then handed the stale row forward into the live
    /// topology, where a reader observed it as a torn read.
    /// </para>
    /// <para>
    /// <b>Why this fixture is the one that can observe the clause.</b> It is the
    /// only test that drives <c>MergeManyAsync</c> with
    /// <c>isCrossShardMigration: true</c> against a donor whose declared span
    /// genuinely excludes the key - which is exactly and only the predicate the
    /// removed term short-circuited. The sibling test above passes
    /// <c>isCrossShardMigration: false</c>, so it stays green in both worlds and
    /// can observe nothing about this clause. Restoring the term reddens the
    /// donor arm with <c>Expected: null But was: &lt;byte[]&gt;</c> and the
    /// declaring arm with <c>Expected: &lt;migrated-out&gt; But was: null</c> -
    /// the row admitted on the wrong leaf and absent from the right one.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_cross_shard_migration_merge_splits_by_declared_span_instead_of_admitting_wholesale()
    {
        var (router, shard) = await CreateSingleShardTreeAsync($"span-migrate-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);

        var stamp = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.Ticks, Counter = 0 };
        var outValue = Encoding.UTF8.GetBytes("migrated-out");

        await Leaf(f.Donor).MergeManyAsync(
            new Dictionary<string, LwwValue<byte[]>>
            {
                [f.OutOfSpanKey] = new LwwValue<byte[]> { Value = outValue, Timestamp = stamp },
            },
            isCrossShardMigration: true);

        var mergedOnDonor = await Leaf(f.Donor).GetAsync(f.OutOfSpanKey);
        var mergedOnDeclaring = await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey);

        Assert.Multiple(() =>
        {
            Assert.That(mergedOnDonor, Is.Null,
                "a migration import outside the donor's declared span must not be admitted here: "
                + "re-creating the key on a leaf that no longer owns it leaves a stale IsMigrated row "
                + "that the donor's next split hands forward into the live topology (issue #3117)");
            Assert.That(mergedOnDeclaring, Is.EqualTo(outValue),
                "the import must be routed to the leaf that declares the key, exactly as a "
                + "non-migration merge already is");
        });
    }

    /// <summary>
    /// The conditional batched write path (<c>SetManyWherePredicateAsync</c>,
    /// behind <c>ILattice.ConditionalSetManyAsync</c>) is the regression guard
    /// for issue #2663, and it is the one batched write entry point that was
    /// never covered by the rest of this fixture.
    /// <para>
    /// Every other path here forwards an out-of-span key. This one could not,
    /// and the reason it failed differently is what made the defect so quiet.
    /// The conditional path evaluates its guard by probing the leaf's own
    /// cache, and reads a key it finds no row for as "no live committed value",
    /// which it is specified to treat as non-matching and skip. That inference
    /// is sound only for a key the leaf declares. For a key whose row a split
    /// moved to a sibling, the absence says nothing about the guard - the real
    /// value still lives on the declaring leaf and may well satisfy it - yet
    /// the answer is identical: the key is dropped from the written set with
    /// no error, no metric, and nothing to distinguish "your guard did not
    /// match" from "I never evaluated it". The caller is told the write
    /// completed.
    /// </para>
    /// <para>
    /// That is the completeness violation the chaos fixture observes as
    /// <c>matchMissing</c>: soundness holds (nothing wrong is written) while
    /// completeness fails (matching keys go unwritten), which is exactly the
    /// signature of skip-rather-than-corrupt. Reverting the pre-guard span
    /// admission reddens the <c>WrittenKeys</c> arm with
    /// <c>Expected: ... "span-cond-out" ... But was: &lt; "..." &gt;</c> and
    /// the declaring-leaf arm with the seeded value still in place.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_conditional_batched_write_evaluates_the_guard_on_the_leaf_that_declares_the_key()
    {
        var (router, shard) = await CreateSingleShardTreeAsync($"span-cond-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);

        var inSpanKey = (f.DonorRange.LowKeyInclusive ?? "k") + "-cond-in";
        Assert.That(
            SplitBoundary.Owns(inSpanKey, f.DonorRange.LowKeyInclusive, f.DonorRange.HighKeyExclusive),
            Is.True,
            "precondition: the control key must fall inside the donor's declared span");

        // Both keys start with a guard-matching value, the out-of-span one on
        // the leaf that genuinely declares it.
        await router.SetAsync(inSpanKey, ScoredJson(1000));
        await router.SetAsync(f.OutOfSpanKey, ScoredJson(1000));
        Assert.That(await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey), Is.Not.Null,
            "precondition: the out-of-span row must start life on the leaf that declares it");

        var result = await Leaf(f.Donor).SetManyWherePredicateAsync(
            [
                new KeyValuePair<string, byte[]>(inSpanKey, ScoredJson(2000)),
                new KeyValuePair<string, byte[]>(f.OutOfSpanKey, ScoredJson(2000)),
            ],
            ScoreAtLeast(500));

        var inThroughRouter = await router.GetAsync(inSpanKey);
        var outOnDonor = await Leaf(f.Donor).GetAsync(f.OutOfSpanKey);
        var outOnDeclaring = await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey);

        Assert.Multiple(() =>
        {
            Assert.That(result.WrittenKeys, Is.EquivalentTo(new[] { inSpanKey, f.OutOfSpanKey }),
                "both keys hold a guard-matching value, so both must be reported written - reporting "
                + "only the in-span key tells the caller the batch completed while a matching key was "
                + "never evaluated (issue #2663)");
            Assert.That(Encoding.UTF8.GetString(inThroughRouter!), Is.EqualTo("{\"Score\":2000}"),
                "the in-span entry belongs here and must still be committed locally");
            Assert.That(outOnDonor, Is.Null,
                "the out-of-span entry must not be admitted on the donor, whose declared span excludes it");
            Assert.That(Encoding.UTF8.GetString(outOnDeclaring!), Is.EqualTo("{\"Score\":2000}"),
                "the guard must be evaluated against the key's real committed value on the leaf that "
                + "declares it, and the matching entry committed there");
        });
    }

    /// <summary>
    /// The complement of the test above, and what keeps it from being satisfied
    /// by a forward that ignores the guard: an out-of-span key whose real
    /// committed value does <b>not</b> satisfy the predicate must still be
    /// rejected. Forwarding moves where the guard is evaluated; it must not
    /// weaken it into an unconditional write.
    /// </summary>
    [Test]
    public async Task A_forwarded_conditional_entry_is_still_rejected_when_the_real_value_fails_the_guard()
    {
        var (router, shard) = await CreateSingleShardTreeAsync($"span-cond-miss-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);

        await router.SetAsync(f.OutOfSpanKey, ScoredJson(10));

        var result = await Leaf(f.Donor).SetManyWherePredicateAsync(
            [new KeyValuePair<string, byte[]>(f.OutOfSpanKey, ScoredJson(2000))],
            ScoreAtLeast(500));

        var outOnDeclaring = await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey);

        Assert.Multiple(() =>
        {
            Assert.That(result.WrittenKeys, Is.Empty,
                "the forwarded entry's real value fails the guard, so it must be reported unwritten");
            Assert.That(Encoding.UTF8.GetString(outOnDeclaring!), Is.EqualTo("{\"Score\":10}"),
                "forwarding relocates where the guard runs; it must not turn a conditional write into "
                + "an unconditional one");
        });
    }

    /// <summary>
    /// <c>ShardRootGrain.ForwardWrittenEntriesToShadowIfNeededAsync</c> pairs a
    /// leaf's written keys back against the slice it dispatched with an
    /// allocation-free two-pointer walk, which assumes the written set is an
    /// <b>in-order subsequence</b> of that slice. A span forward produces its
    /// written keys out of band, so the union has to be re-derived in the
    /// caller's original order rather than appended. This pins that ordering
    /// with a batch whose forwarded key sits in the middle of the slice, where
    /// an appended tail would be observable.
    /// </summary>
    [Test]
    public async Task A_span_forwarded_conditional_write_reports_written_keys_in_the_callers_order()
    {
        var (router, shard) = await CreateSingleShardTreeAsync($"span-cond-order-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);

        var low = f.DonorRange.LowKeyInclusive ?? "k";
        var firstInSpan = low + "-cond-a";
        var lastInSpan = low + "-cond-z";
        foreach (var key in new[] { firstInSpan, lastInSpan })
        {
            Assert.That(
                SplitBoundary.Owns(key, f.DonorRange.LowKeyInclusive, f.DonorRange.HighKeyExclusive),
                Is.True,
                $"precondition: control key '{key}' must fall inside the donor's declared span");
            await router.SetAsync(key, ScoredJson(1000));
        }

        await router.SetAsync(f.OutOfSpanKey, ScoredJson(1000));

        // The forwarded key is deliberately in the middle of the batch.
        var slice = new List<KeyValuePair<string, byte[]>>
        {
            new(firstInSpan, ScoredJson(2000)),
            new(f.OutOfSpanKey, ScoredJson(2000)),
            new(lastInSpan, ScoredJson(2000)),
        };

        var result = await Leaf(f.Donor).SetManyWherePredicateAsync(slice, ScoreAtLeast(500));

        Assert.That(result.WrittenKeys,
            Is.EqualTo(new[] { firstInSpan, f.OutOfSpanKey, lastInSpan }).AsCollection,
            "the written set must stay an in-order subsequence of the dispatched slice, because the "
            + "shard root pairs the two with a forward-only two-pointer walk");
    }

    /// <summary>
    /// A delete is a write: it appends a tombstone the replay filter drops on a
    /// leaf that does not declare the key, so an out-of-span delete acknowledged
    /// on the donor leaves the real row live on the declaring leaf. The caller
    /// is told the key is gone and it is not.
    /// </summary>
    [Test]
    public async Task A_delete_outside_the_declared_span_removes_the_row_from_the_leaf_that_holds_it()
    {        var (router, shard) = await CreateSingleShardTreeAsync($"span-delete-{Guid.NewGuid():N}");
        var f = await BuildSealedDonorAsync(router, shard);
        var value = Encoding.UTF8.GetBytes("doomed");

        await router.SetAsync(f.OutOfSpanKey, value);
        Assert.That(await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey), Is.EqualTo(value),
            "precondition: the row must start life on the leaf that declares it");

        var deleted = await Leaf(f.Donor).DeleteAsync(f.OutOfSpanKey);

        var stillOnDeclaring = await Leaf(f.Declaring).GetAsync(f.OutOfSpanKey);
        var throughRouter = await router.GetAsync(f.OutOfSpanKey);

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True,
                "the delete must report the truth about the row it actually removed");
            Assert.That(stillOnDeclaring, Is.Null,
                "an acknowledged delete must remove the row from the leaf that holds it, not tombstone a "
                + "key on a leaf that never declared it");
            Assert.That(throughRouter, Is.Null);
        });
    }
}
