using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The entries-side twin of <see cref="LatticeGrainKeysScanReconciliationTests"/>:
/// covers the reconciliation, retry-budget and page-pipelining arms of
/// <c>LatticeGrain.EntriesAsync</c>'s k-way merge (<c>LatticeGrain.Entries.cs</c>).
/// <para>
/// The two walks are deliberately parallel implementations rather than one shared
/// one - the entries cursor carries values, so it cannot reuse the keys cursor -
/// which means the keys fixture proves nothing about this file. Both halves are
/// pinned separately for that reason: a fix applied to one and not the other is
/// exactly the drift this fixture catches.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainEntriesScanReconciliationTests
{
    private const string TreeId = "entries-scan-tree";

    private sealed class Harness
    {
        public required LatticeGrain Grain { get; init; }
        public required IShardRootGrain Shard { get; init; }
        public required ILatticeRegistry Registry { get; init; }
    }

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

    private static KeyValuePair<string, byte[]> Entry(string key) =>
        new(key, Encoding.UTF8.GetBytes($"v:{key}"));

    private static EntriesPage Page(
        IEnumerable<string>? keys = null,
        bool hasMore = false,
        int[]? movedAwaySlots = null,
        string? resumeFromKey = null) => new()
        {
            Entries = keys is null ? [] : [.. keys.Select(Entry)],
            HasMore = hasMore,
            MovedAwaySlots = movedAwaySlots,
            ResumeFromKey = resumeFromKey,
        };

    private static void ScriptPages(IShardRootGrain shard, params EntriesPage[] pages)
    {
        var calls = 0;
        shard.GetSortedEntriesBatchAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(_ => Task.FromResult(pages[Math.Min(calls++, pages.Length - 1)]));
    }

    private static void ScriptDrain(IShardRootGrain shard, EntriesPage page) =>
        shard.GetSortedEntriesBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(page));

    private static async Task<List<string>> DrainKeysAsync(
        IAsyncEnumerable<KeyValuePair<string, byte[]>> source)
    {
        var result = new List<string>();
        await foreach (var e in source) result.Add(e.Key);
        return result;
    }

    // ------------------------------------------------------------- pipelining

    /// <summary>
    /// With prefetch on, the entries cursor must consume the page it already has
    /// in flight rather than issuing a duplicate fetch for it.
    /// </summary>
    [Test]
    public async Task Prefetching_scan_consumes_the_in_flight_page_instead_of_refetching()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchEntriesScan = true, KeysPageSize = 2 });
        ScriptPages(h.Shard,
            Page(["a", "b"], hasMore: true),
            Page(["c"], hasMore: false));

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(
                h.Shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.GetSortedEntriesBatchAsync)),
                Is.EqualTo(2),
                "the prefetched page must be consumed, not refetched");
        });
    }

    /// <summary>
    /// Values survive the merge intact - the cursor must carry the payload, not
    /// just the ordering key.
    /// </summary>
    [Test]
    public async Task Scan_yields_each_key_with_its_value()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchEntriesScan = false });
        ScriptPages(h.Shard, Page(["a", "b"], hasMore: false));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in h.Grain.EntriesAsync()) entries.Add(e);

        Assert.That(
            entries.Select(e => $"{e.Key}={Encoding.UTF8.GetString(e.Value)}"),
            Is.EqualTo(new[] { "a=v:a", "b=v:b" }));
    }

    /// <summary>
    /// An empty-but-more page must be resumed from the shard's reported leaf
    /// boundary instead of silently truncating the scan (issue 1992).
    /// </summary>
    [Test]
    public async Task Empty_page_that_reports_a_resume_boundary_continues_the_scan_from_that_boundary()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchEntriesScan = false, KeysPageSize = 8 });
        ScriptPages(h.Shard,
            Page(hasMore: true, resumeFromKey: "leaf-2-low"),
            Page(["z"], hasMore: false));

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "z" }));
        await h.Shard.Received(1).GetSortedEntriesBatchAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(),
            null, Arg.Any<LatticePredicateNode?>(), "leaf-2-low");
    }

    /// <summary>
    /// A page that reports more but advances neither an entry nor a boundary is
    /// unresumable, so it must end the scan rather than be re-issued forever.
    /// </summary>
    [Test]
    public async Task Page_that_advances_nothing_ends_the_scan_instead_of_looping()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchEntriesScan = false });
        ScriptPages(h.Shard, Page(hasMore: true));

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.Empty);
        Assert.That(
            h.Shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.GetSortedEntriesBatchAsync)),
            Is.EqualTo(1));
    }

    // -------------------------------------------------------- stale-alias heal

    /// <summary>
    /// The entries scan must self-heal through a stale-alias routing error on its
    /// initial page fetch, exactly as the keys scan does.
    /// </summary>
    [Test]
    public async Task Scan_init_self_heals_through_a_stale_routing_error_and_re_resolves()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchEntriesScan = false });

        var calls = 0;
        h.Shard.GetSortedEntriesBatchAsync(
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

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { "a" }));
            Assert.That(calls, Is.EqualTo(2));
        });
        await h.Registry.Received(2).ResolveAsync(TreeId);
    }

    // ------------------------------------------------- retry-budget exhaustion

    /// <summary>
    /// GAP 1 of the issue-1955 audit, entries side: the reconciliation budget is
    /// threaded across the whole scan, so a shard reporting a new moved slot on
    /// every page terminates with a typed error instead of spinning.
    /// </summary>
    [Test]
    public void Scan_that_keeps_observing_new_moved_slots_exhausts_the_retry_budget()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 1 },
            SingleShardMap(version: 1));

        var calls = 0;
        h.Shard.GetSortedEntriesBatchAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                calls++;
                return Task.FromResult(Page([$"k{calls}"], hasMore: true, movedAwaySlots: [calls]));
            });
        ScriptDrain(h.Shard, Page());

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await DrainKeysAsync(h.Grain.EntriesAsync()));

        Assert.That(ex!.Message,
            Does.Contain("EntriesAsync exceeded 1 retries").And.Contain("MaxScanRetries"),
            "the entries walk must name itself in the error, not the keys walk");
    }

    /// <summary>
    /// A moved slot reported twice is not new the second time, so it must not
    /// consume a second slice of the retry budget.
    /// </summary>
    [Test]
    public async Task A_moved_slot_reported_twice_only_consumes_one_retry()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 1 },
            SingleShardMap(version: 1));

        ScriptPages(h.Shard,
            Page(["a"], hasMore: true, movedAwaySlots: [3]),
            Page(["b"], hasMore: false, movedAwaySlots: [3]));
        ScriptDrain(h.Shard, Page());

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    /// <summary>
    /// Reconciled entries are injected into the same priority queue as the live
    /// cursors, so global sort order survives a mid-scan split.
    /// </summary>
    [Test]
    public async Task Reconciled_entries_are_merged_into_global_sort_order()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1));

        ScriptPages(h.Shard,
            Page(["a", "d"], hasMore: true, movedAwaySlots: [2]),
            Page(["e"], hasMore: false));
        ScriptDrain(h.Shard, Page(["b"]));

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "d", "e" }));
    }

    /// <summary>
    /// A key already yielded by a live cursor must not be re-yielded when the new
    /// owner also reports it - the cross-cursor dedup that makes a scan across a
    /// split exactly-once rather than at-least-once.
    /// </summary>
    [Test]
    public async Task A_key_visible_in_both_the_pre_and_post_split_view_is_yielded_once()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1));

        ScriptPages(h.Shard,
            Page(["a"], hasMore: true, movedAwaySlots: [2]),
            Page(["c"], hasMore: false));
        // The new owner re-reports "a" alongside its own "b".
        ScriptDrain(h.Shard, Page(["a", "b"]));

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }), "the duplicated key must be suppressed");
    }

    /// <summary>
    /// The reconciliation drain pages through the new owner, preferring the
    /// reported resume boundary over the last drained key.
    /// </summary>
    [Test]
    public async Task Reconciliation_drain_pages_through_the_new_owner_using_its_resume_boundary()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1));

        ScriptPages(h.Shard,
            Page(["a"], hasMore: true, movedAwaySlots: [2]),
            Page(["z"], hasMore: false));

        var drainCalls = 0;
        var drainResumeArgs = new List<string?>();
        var drainContinuationArgs = new List<string?>();
        h.Shard.GetSortedEntriesBatchForSlotsAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
                Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>())
            .Returns(ci =>
            {
                drainContinuationArgs.Add(ci.ArgAt<string?>(3));
                drainResumeArgs.Add(ci.ArgAt<string?>(7));
                drainCalls++;
                return Task.FromResult(drainCalls switch
                {
                    1 => Page(hasMore: true, resumeFromKey: "boundary-1"),
                    2 => Page(["m"], hasMore: true),
                    _ => Page(["n"], hasMore: false),
                });
            });

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { "a", "m", "n", "z" }));
            Assert.That(drainCalls, Is.EqualTo(3));
            Assert.That(drainResumeArgs, Is.EqualTo(new string?[] { null, "boundary-1", "boundary-1" }));
            Assert.That(drainContinuationArgs, Is.EqualTo(new string?[] { null, null, "m" }));
        });
    }

    // ------------------------------------------------- final stability re-check

    /// <summary>
    /// A split that commits after every live cursor drained is caught by the
    /// post-drain stability re-check and reconciled into the result.
    /// </summary>
    [Test]
    public async Task A_split_that_commits_after_every_cursor_drained_is_still_reconciled()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1, 0, 0, 0, 0),
            SingleShardMap(version: 2, 0, 1, 0, 0),
            SingleShardMap(version: 2, 0, 1, 0, 0));

        ScriptPages(h.Shard, Page(["a"], hasMore: false));
        ScriptDrain(h.Shard, Page(["b"]));

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    /// <summary>
    /// When the late re-check drains nothing the scan loops back to re-test
    /// stability, and a map that keeps advancing on every re-check must terminate
    /// on the retry budget rather than looping forever.
    /// </summary>
    [Test]
    public void A_map_that_keeps_advancing_after_the_drain_terminates_on_the_retry_budget()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 1 },
            SingleShardMap(version: 1, 0, 0, 0, 0),
            SingleShardMap(version: 2, 0, 1, 0, 0),
            SingleShardMap(version: 3, 0, 1, 1, 0),
            SingleShardMap(version: 4, 1, 1, 1, 0));

        ScriptPages(h.Shard, Page(["a"], hasMore: false));
        ScriptDrain(h.Shard, Page());

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await DrainKeysAsync(h.Grain.EntriesAsync()));

        Assert.That(ex!.Message, Does.Contain("EntriesAsync exceeded 1 retries"));
    }

    /// <summary>
    /// The stable case must not reconcile at all.
    /// </summary>
    [Test]
    public async Task A_stable_map_ends_the_scan_without_reconciling()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 9, 0, 0, 0, 0));

        ScriptPages(h.Shard, Page(["a", "b"], hasMore: false));

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
        await h.Shard.DidNotReceive().GetSortedEntriesBatchForSlotsAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>(),
            Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<string?>());
    }

    /// <summary>
    /// A late owner diff confined to slots an earlier reconciliation already
    /// covered leaves nothing to drain, so the scan ends.
    /// </summary>
    [Test]
    public async Task A_late_owner_diff_confined_to_already_covered_slots_ends_the_scan()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 4 },
            SingleShardMap(version: 1, 0, 0, 0, 0),
            SingleShardMap(version: 2, 0, 1, 0, 0));

        ScriptPages(h.Shard, Page(["a"], hasMore: false, movedAwaySlots: [1]));
        ScriptDrain(h.Shard, Page());

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a" }));
    }

    /// <summary>
    /// A slot can be reconciled by the <em>owner-diff</em> half of a
    /// reconciliation step (the shard map advanced) before any cursor reports it
    /// as moved. When a later page then does report it, it is not new work: every
    /// reported slot is already covered, so the scan must fall through without
    /// spending another retry. This is the interlock between the two independent
    /// reconciliation triggers - moved-slot reports and map-version changes - and
    /// without it a split that both moves a slot and bumps the map would be
    /// reconciled twice and charged twice against the budget.
    /// </summary>
    [Test]
    public async Task A_slot_already_covered_by_an_owner_diff_is_not_reconciled_again_when_reported_as_moved()
    {
        var h = CreateGrain(
            new LatticeOptions { PrefetchEntriesScan = false, MaxScanRetries = 4 },
            // Scan starts at v1 ...
            SingleShardMap(version: 1, 0, 0, 0, 0),
            // ... the first reconciliation sees v2, whose owner diff covers BOTH
            // slot 1 (reported moved) and slot 2 (not yet reported) ...
            SingleShardMap(version: 2, 0, 1, 1, 0),
            // ... and the map is stable thereafter.
            SingleShardMap(version: 2, 0, 1, 1, 0));

        ScriptPages(h.Shard,
            Page(["a"], hasMore: true, movedAwaySlots: [1]),
            // Slot 2 is reported moved only now - after the owner diff already
            // covered it.
            Page(["b"], hasMore: false, movedAwaySlots: [2]));
        ScriptDrain(h.Shard, Page());

        var keys = await DrainKeysAsync(h.Grain.EntriesAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
        // Exactly one reconciliation step: the second sighting found every
        // reported slot already covered and did no work.
        Assert.That(
            h.Shard.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(IShardRootGrain.GetSortedEntriesBatchForSlotsAsync)),
            Is.EqualTo(1),
            "an already-covered slot must not trigger a second reconciliation drain");
    }

    /// <summary>
    /// Cancellation is observed at the merge loop's own check.
    /// </summary>
    [Test]
    public void Cancelling_mid_scan_stops_the_merge_loop()
    {
        var h = CreateGrain(new LatticeOptions { PrefetchEntriesScan = false });
        using var cts = new CancellationTokenSource();
        ScriptPages(h.Shard, Page(["a", "b", "c"], hasMore: false));

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in h.Grain.EntriesAsync(cancellationToken: cts.Token))
            {
                cts.Cancel();
            }
        });
    }
}
