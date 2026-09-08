using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers the reconciliation, retry-budget and page-pipelining arms of
/// <c>LatticeGrain.KeysAsync</c>'s k-way merge (<c>LatticeGrain.Keys.cs</c>).
/// <para>
/// The happy path of a strongly-consistent scan across a live split is pinned by
/// <c>StronglyConsistentScanIntegrationTests</c>, but a cluster fixture cannot
/// deterministically drive the arms that only fire when topology keeps changing:
/// the <see cref="LatticeOptions.MaxScanRetries"/> exhaustion throws, the
/// post-drain "final stability check" loop, the empty-drain re-check, and the
/// stale-alias self-heal on scan init. Those are liveness bounds - the guarantee
/// that a scan against a pathologically churning tree terminates with a typed
/// error instead of spinning forever - so they are worth pinning directly.
/// This fixture drives them from a substituted shard root, where each page and
/// each shard-map revision is chosen by the test.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainKeysScanReconciliationTests
{
    private const string TreeId = "keys-scan-tree";

    private sealed class Harness
    {
        public required LatticeGrain Grain { get; init; }
        public required IShardRootGrain Shard { get; init; }
        public required ILatticeRegistry Registry { get; init; }
    }

    /// <summary>
    /// Builds a single-physical-shard tree whose registry hands back
    /// <paramref name="maps"/> in order (the last one repeating), so a test can
    /// make the shard map appear to advance under a running scan.
    /// </summary>
    private static Harness CreateGrain(
        LatticeOptions? options = null,
        params ShardMap[] maps)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var opts = options ?? new LatticeOptions();
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));

        var mapQueue = maps.Length > 0 ? maps : [SingleShardMap(version: 1)];
        var mapCalls = 0;
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(_ =>
        {
            var idx = Math.Min(mapCalls, mapQueue.Length - 1);
            mapCalls++;
            return Task.FromResult<ShardMap?>(mapQueue[idx]);
        });

        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shard);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, opts);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);

        return new Harness { Grain = grain, Shard = shard, Registry = registry };
    }

    private static ShardMap SingleShardMap(long version, params int[] slots) =>
        new() { Slots = slots.Length > 0 ? slots : new int[4], Version = version };

    private static KeysPage Page(
        IEnumerable<string>? keys = null,
        bool hasMore = false,
        int[]? movedAwaySlots = null,
        string? resumeFromKey = null) => new()
        {
            Keys = keys is null ? [] : [.. keys],
            HasMore = hasMore,
            MovedAwaySlots = movedAwaySlots,
            ResumeFromKey = resumeFromKey,
        };

    /// <summary>Stubs the forward page fetch with a scripted sequence.</summary>
    private static void ScriptPages(IShardRootGrain shard, params KeysPage[] pages)
    {
        var calls = 0;
        shard.GetSortedKeysBatchAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(_ => Task.FromResult(pages[Math.Min(calls++, pages.Length - 1)]));
    }

    private static async Task<List<string>> DrainAsync(IAsyncEnumerable<string> source)
    {
        var result = new List<string>();
        await foreach (var k in source) result.Add(k);
        return result;
    }

    // ------------------------------------------------------------- pipelining

    /// <summary>
    /// With prefetch on, the cursor must issue the next page's fetch while the
    /// caller is still draining the current one, and then <em>consume that
    /// in-flight task</em> rather than issuing a duplicate fetch. Without this
    /// test the prefetch consumption arm is never executed: every existing
    /// unit fixture returns a single terminal page.
    /// </summary>
    [Test]
    public async Task Prefetching_scan_consumes_the_in_flight_page_instead_of_refetching()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchKeysScan = true, KeysPageSize = 2 });
        ScriptPages(h.Shard,
            Page(["a", "b"], hasMore: true),
            Page(["c"], hasMore: false));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
            // Two pages of content: one fetch each. A third call would mean the
            // prefetched task was dropped and re-issued.
            Assert.That(
                h.Shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.GetSortedKeysBatchAsync)),
                Is.EqualTo(2),
                "the prefetched page must be consumed, not refetched");
        });
    }

    /// <summary>
    /// The negative control: with prefetch off the same two-page scan yields the
    /// same keys, so the pipelining arm is an optimisation rather than a
    /// behavioural difference.
    /// </summary>
    [Test]
    public async Task Non_prefetching_scan_yields_the_same_keys()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchKeysScan = false, KeysPageSize = 2 });
        ScriptPages(h.Shard,
            Page(["a", "b"], hasMore: true),
            Page(["c"], hasMore: false));

        Assert.That(await DrainAsync(h.Grain.KeysAsync()), Is.EqualTo(new[] { "a", "b", "c" }));
    }

    /// <summary>
    /// A work-bounded page can be empty while still reporting more (every leaf it
    /// walked held only tombstoned, expired, moved-away or predicate-rejected
    /// rows). The cursor must resume from the shard's reported leaf boundary
    /// rather than ending the scan - the issue-1992 truncation.
    /// </summary>
    [Test]
    public async Task Empty_page_that_reports_a_resume_boundary_continues_the_scan_from_that_boundary()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchKeysScan = false, KeysPageSize = 8 });
        ScriptPages(h.Shard,
            Page(hasMore: true, resumeFromKey: "leaf-2-low"),
            Page(["z"], hasMore: false));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "z" }), "an empty-but-more page must not truncate the scan");

        // The resume boundary must be threaded into the follow-up call as the
        // resumeFrom argument, with the continuation left null: resuming from the
        // last returned key is impossible when the page returned none.
        await h.Shard.Received(1).GetSortedKeysBatchAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(),
            null, Arg.Any<LatticePredicateNode?>(), "leaf-2-low");
    }

    /// <summary>
    /// A page that reports more but advances neither a key nor a resume boundary
    /// cannot be resumed at all, so the cursor must treat it as the end rather
    /// than re-issuing an identical request forever.
    /// </summary>
    [Test]
    public async Task Page_that_advances_nothing_ends_the_scan_instead_of_looping()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchKeysScan = false });
        // HasMore is true but there is no key and no boundary: unresumable.
        ScriptPages(h.Shard, Page(hasMore: true));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.That(keys, Is.Empty);
        Assert.That(
            h.Shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.GetSortedKeysBatchAsync)),
            Is.EqualTo(1),
            "an unresumable page must not be re-issued");
    }

    // -------------------------------------------------------- stale-alias heal

    /// <summary>
    /// A shadow-cutover restore can supersede the tree between the activation
    /// cache being populated and the scan starting, so the retained shard throws
    /// <see cref="StaleTreeRoutingException"/> on the initial page fetch. The
    /// scan must self-heal - drop the cached alias, re-resolve routing and rebuild
    /// its cursors - rather than surfacing a transient routing error.
    /// </summary>
    [Test]
    public async Task Scan_init_self_heals_through_a_stale_routing_error_and_re_resolves()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchKeysScan = false });

        var calls = 0;
        h.Shard.GetSortedKeysBatchAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                calls++;
                if (calls == 1)
                {
                    throw new StaleTreeRoutingException
                    {
                        LogicalTreeId = TreeId,
                        StalePhysicalTreeId = $"{TreeId}#old",
                    };
                }
                return Task.FromResult(Page(["a"], hasMore: false));
            });

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { "a" }), "the scan must complete on the re-resolved tree");
            Assert.That(calls, Is.EqualTo(2), "the scan must retry the init after invalidating the stale alias");
        });
        // The self-heal must re-resolve the alias rather than reuse the cached one.
        await h.Registry.Received(2).ResolveAsync(TreeId);
    }

    // ------------------------------------------------- retry-budget exhaustion

    /// <summary>
    /// GAP 1 of the issue-1955 audit: the reconciliation retry budget is threaded
    /// across the whole scan, not reset per page. A shard that keeps reporting a
    /// <em>new</em> moved slot on every page must exhaust
    /// <see cref="LatticeOptions.MaxScanRetries"/> and surface a typed error, so a
    /// pathologically splitting tree cannot spin the scan forever.
    /// </summary>
    [Test]
    public void Scan_that_keeps_observing_new_moved_slots_exhausts_the_retry_budget()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 1 },
            SingleShardMap(version: 1));

        // Each page reports a different moved slot, so every loop iteration sees
        // a slot the previous reconciliation did not cover.
        var calls = 0;
        h.Shard.GetSortedKeysBatchAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                calls++;
                return Task.FromResult(Page([$"k{calls}"], hasMore: true, movedAwaySlots: [calls]));
            });
        // The reconciliation drain finds nothing, so no keys are injected and the
        // scan keeps looping back to the moved-slot check.
        h.Shard.GetSortedKeysBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(Page()));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await DrainAsync(h.Grain.KeysAsync()));

        Assert.That(ex!.Message, Does.Contain("exceeded 1 retries").And.Contain("MaxScanRetries"));
    }

    /// <summary>
    /// A moved slot reported twice is <em>not</em> new the second time: it was
    /// covered by the first reconciliation, so it must not consume a second slice
    /// of the retry budget. This is the guard that stops a single persistent split
    /// from starving an otherwise healthy scan.
    /// </summary>
    [Test]
    public async Task A_moved_slot_reported_twice_only_consumes_one_retry()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 1 },
            SingleShardMap(version: 1));

        // The same slot 3 on both pages: only the first sighting is new.
        ScriptPages(h.Shard,
            Page(["a"], hasMore: true, movedAwaySlots: [3]),
            Page(["b"], hasMore: false, movedAwaySlots: [3]));
        h.Shard.GetSortedKeysBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(Page()));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }),
            "a repeat sighting of an already-reconciled slot must not exhaust the budget");
    }

    /// <summary>
    /// Reconciled keys are injected into the same priority queue as the live
    /// cursors, so they take part in the k-way merge and the output stays globally
    /// sorted across the topology boundary.
    /// </summary>
    [Test]
    public async Task Reconciled_keys_are_merged_into_global_sort_order()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1));

        ScriptPages(h.Shard,
            Page(["a", "d"], hasMore: true, movedAwaySlots: [2]),
            Page(["e"], hasMore: false));
        // The new owner holds "b" - lexicographically between the live cursor's
        // "a" and "d", so a correct merge interleaves it rather than appending it.
        h.Shard.GetSortedKeysBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(Page(["b"])));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "d", "e" }),
            "reconciled keys must merge into global order, not be appended");
    }

    /// <summary>
    /// The reconciliation drain is itself paginated, and prefers the shard's
    /// resume boundary over the last drained key for exactly the reason the live
    /// cursor does: a bounded drain page can be empty while still reporting more.
    /// </summary>
    [Test]
    public async Task Reconciliation_drain_pages_through_the_new_owner_using_its_resume_boundary()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1));

        ScriptPages(h.Shard,
            Page(["a"], hasMore: true, movedAwaySlots: [2]),
            Page(["z"], hasMore: false));

        var drainCalls = 0;
        var drainResumeArgs = new List<string?>();
        var drainContinuationArgs = new List<string?>();
        h.Shard.GetSortedKeysBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(ci =>
            {
                drainContinuationArgs.Add(ci.ArgAt<string?>(3));
                drainResumeArgs.Add(ci.ArgAt<string?>(7));
                drainCalls++;
                return Task.FromResult(drainCalls switch
                {
                    // Empty but more, with a boundary: must be followed, not stopped.
                    1 => Page(hasMore: true, resumeFromKey: "boundary-1"),
                    // A key, still more, no boundary: fall back to the last key.
                    2 => Page(["m"], hasMore: true),
                    _ => Page(["n"], hasMore: false),
                });
            });

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { "a", "m", "n", "z" }));
            Assert.That(drainCalls, Is.EqualTo(3), "the drain must page to the end of the moved slot");
            Assert.That(drainResumeArgs, Is.EqualTo(new string?[] { null, "boundary-1", "boundary-1" }),
                "the reported boundary must be threaded into the follow-up drain call");
            Assert.That(drainContinuationArgs, Is.EqualTo(new string?[] { null, null, "m" }),
                "with no boundary on page 2 the drain must continue from its last key");
        });
    }

    // ------------------------------------------------- final stability re-check

    /// <summary>
    /// A split can commit after every live cursor has drained, so no cursor ever
    /// reported a moved slot. The post-drain stability re-check exists to catch
    /// exactly that, and must inject the late owner's keys rather than returning a
    /// short result.
    /// </summary>
    [Test]
    public async Task A_split_that_commits_after_every_cursor_drained_is_still_reconciled()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 4 },
            // Scan starts on v1 ...
            SingleShardMap(version: 1, 0, 0, 0, 0),
            // ... and the post-drain re-check sees slot 1 re-owned at v2 ...
            SingleShardMap(version: 2, 0, 1, 0, 0),
            // ... after which the map is stable, ending the scan.
            SingleShardMap(version: 2, 0, 1, 0, 0));

        ScriptPages(h.Shard, Page(["a"], hasMore: false));
        h.Shard.GetSortedKeysBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(Page(["b"])));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }),
            "a split committed after the cursors drained must still be reconciled into the result");
    }

    /// <summary>
    /// When the late re-check finds the moved slot empty, the scan must loop back
    /// and re-test stability rather than fall out - and when the map keeps
    /// advancing on every re-check, that loop must terminate on the retry budget
    /// with a typed error. This is the liveness bound for the post-drain half of
    /// the scan.
    /// </summary>
    [Test]
    public void A_map_that_keeps_advancing_after_the_drain_terminates_on_the_retry_budget()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 1 },
            SingleShardMap(version: 1, 0, 0, 0, 0),
            SingleShardMap(version: 2, 0, 1, 0, 0),
            SingleShardMap(version: 3, 0, 1, 1, 0),
            SingleShardMap(version: 4, 1, 1, 1, 0));

        ScriptPages(h.Shard, Page(["a"], hasMore: false));
        // Every drain comes back empty, so the re-check loops instead of injecting.
        h.Shard.GetSortedKeysBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(Page()));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await DrainAsync(h.Grain.KeysAsync()));

        Assert.That(ex!.Message, Does.Contain("exceeded 1 retries"));
    }

    /// <summary>
    /// The stable case must not pay for any of the above: when the map has not
    /// moved by the time the cursors drain, the scan ends without a single
    /// reconciliation drain.
    /// </summary>
    [Test]
    public async Task A_stable_map_ends_the_scan_without_reconciling()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 9, 0, 0, 0, 0));

        ScriptPages(h.Shard, Page(["a", "b"], hasMore: false));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
        await h.Shard.DidNotReceive().GetSortedKeysBatchForSlotsAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
            Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>());
    }

    /// <summary>
    /// A late re-check whose owner diff is confined to slots an earlier
    /// reconciliation already covered has nothing left to drain, so the scan ends
    /// rather than spending a retry on a no-op.
    /// </summary>
    [Test]
    public async Task A_late_owner_diff_confined_to_already_covered_slots_ends_the_scan()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchKeysScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1, 0, 0, 0, 0),
            // Slot 1 is the one the live cursor already reported as moved.
            SingleShardMap(version: 2, 0, 1, 0, 0));

        ScriptPages(h.Shard, Page(["a"], hasMore: false, movedAwaySlots: [1]));
        h.Shard.GetSortedKeysBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(Page()));

        var keys = await DrainAsync(h.Grain.KeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a" }));
    }

    /// <summary>
    /// Cancellation is observed at the merge loop's own check, so a caller that
    /// abandons a long scan stops it rather than draining every remaining page.
    /// </summary>
    [Test]
    public void Cancelling_mid_scan_stops_the_merge_loop()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchKeysScan = false });
        using var cts = new CancellationTokenSource();
        ScriptPages(h.Shard, Page(["a", "b", "c"], hasMore: false));

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in h.Grain.KeysAsync(cancellationToken: cts.Token))
            {
                cts.Cancel();
            }
        });
    }
}
