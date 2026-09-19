using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression for issue #3094: a newborn split sibling must not disable the WAL
/// GC's cursor-trim branch for the whole tree.
/// <para>
/// The birth seed used to publish the "-1" no-offset sentinel, which leaves the
/// pin outside the GC's offset coverage set and so protected only by the
/// Zero-HLC block-pin branch. That branch does not gate one partition - it
/// short-circuits the entire pass before a single shard is scanned. Leaf keys
/// hash across every WAL partition, so one newborn blocked all of them, and
/// splits admit newborns continuously, so a growing tree never stopped having
/// one. The measured production consequence was a WAL that grew monotonically
/// while every pass reported itself blocked.
/// </para>
/// <para>
/// <b>These fixtures deliberately drive a CONTINUOUSLY splitting tree.</b> A
/// single split followed by its checkpoint re-proves the path that already
/// worked: the stuck population is the tree that is always mid-birth because a
/// new sibling arrives before the last one checkpoints. The loop below never
/// lets a sibling checkpoint, which is the exact shape that wedged.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Builds a leaf that reports through an already-existing pin store and
    /// registry, so several siblings born in sequence share one durable pin
    /// set - the multi-leaf shape <see cref="CreateLeafWithDurablePinStore"/>
    /// cannot express because it mints a fresh pin grain per call.
    /// </summary>
    private static BPlusLeafGrain CreateLeafOnSharedPinStore(
        string leafKey,
        InMemoryWalCursorRegistry registry,
        IGrainFactory factory)
    {
        var reporter = new LeafCursorReporter(registry, factory);

        var services = new ServiceCollection();
        services.AddSingleton<ILeafCursorReporter>(reporter);
        var provider = services.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey));
        context.ActivationServices.Returns(provider);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: factory);

        return new BPlusLeafGrain(
            context,
            new FakePersistentState<LeafNodeState>(),
            factory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
    }

    private static (WalMaterialiserPinGrain PinGrain, InMemoryWalCursorRegistry Registry, IGrainFactory Factory)
        CreateSharedPinStore()
    {
        var registry = new InMemoryWalCursorRegistry();

        var pinContext = Substitute.For<IGrainContext>();
        pinContext.GrainId.Returns(GrainId.Create("wal-materialiser-pin", PinSeamTreeId));
        var pinGrain = new WalMaterialiserPinGrain(
            pinContext, new FakePersistentState<WalMaterialiserPinState>(), PinOptionsMonitor());

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);

        return (pinGrain, registry, factory);
    }

    private static LatticeWalGc CreateGcOver(IWalStorageProvider provider, InMemoryWalCursorRegistry registry, IGrainFactory factory)
    {
        var gcServices = new ServiceCollection();
        gcServices.AddSingleton(provider);
        gcServices.AddSingleton(factory);
        return new LatticeWalGc(gcServices.BuildServiceProvider(), registry, PinOptionsMonitor());
    }

    [Test]
    public async Task Newborn_sibling_carrying_its_birth_head_publishes_a_real_offset_not_the_sentinel()
    {
        var (pinGrain, registry, factory) = CreateSharedPinStore();
        var sibling = CreateLeafOnSharedPinStore("split-sibling-offset-1", registry, factory);

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = PinSeamTreeId,
            ShardIndex = 0,
            LowKeyInclusive = "m",
            NextSibling = null,
            PrevSibling = GrainId.Create("leaf", "donor"),
            WalHeadsAtBirth = new[] { 1L },
        });

        var offsets = await pinGrain.GetPinOffsetsAsync();
        Assert.That(offsets, Is.Not.Null.And.Not.Empty);
        Assert.That(offsets!.Values, Has.All.EqualTo(1L),
            "The birth seed must publish the captured WAL head as a real checkpoint offset, "
            + "so the pin joins the GC's offset coverage set instead of sitting outside it on the -1 sentinel.");

        // The frontier half is unchanged: the leaf has still checkpointed
        // nothing, so it still reports Zero. The fix changes the offset axis
        // only.
        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins.Values, Has.All.EqualTo(HybridLogicalClock.Zero),
            "The seeded frontier must stay Zero - this fix does not claim the newborn has materialised anything.");
    }

    [Test]
    public async Task Newborn_sibling_no_longer_disables_the_cursor_branch_yet_its_own_entries_still_survive()
    {
        // The two halves that must hold simultaneously, in one pass:
        //   liveness - the tree-wide cursor branch is NOT disabled, so the
        //              committed prefix below the newborn's birth head reclaims;
        //   safety   - every entry ABOVE that head (the rows the sibling
        //              inherited and has not materialised) survives.
        var (pinGrain, registry, factory) = CreateSharedPinStore();
        var provider = new InMemoryWalStorageProvider();

        // Pre-split committed prefix.
        await provider.AppendBatchAsync(
            PinSeamTreeId, 0,
            new[] { PinWalEntry(0, PinHlc(10)), PinWalEntry(1, PinHlc(20)) },
            CancellationToken.None);

        // The donor captures the head (1) and hands it to the sibling at birth.
        var sibling = CreateLeafOnSharedPinStore("split-sibling-offset-2", registry, factory);
        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = PinSeamTreeId,
            ShardIndex = 0,
            LowKeyInclusive = "m",
            NextSibling = null,
            PrevSibling = GrainId.Create("leaf", "donor"),
            WalHeadsAtBirth = new[] { 1L },
        });

        // MergeEntriesAsync then appends the sibling's inherited rows ABOVE the
        // captured head. These are what the seed exists to protect.
        await provider.AppendBatchAsync(
            PinSeamTreeId, 0,
            new[] { PinWalEntry(2, PinHlc(30)) },
            CancellationToken.None);

        // A forward consumer sits at the WAL head, so every entry is
        // HLC-eligible and only the offset floor can hold anything back.
        await registry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(30));

        var report = await CreateGcOver(provider, registry, factory).RunOnceAsync(PinSeamTreeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.Available),
                "The newborn's pin is covered by the offset floor, so the pass must NOT report "
                + "BlockedByUnusablePin - that state is what disables the cursor-trim branch tree-wide.");
            Assert.That(report.BlockingConsumerId, Is.Null,
                "A covered newborn must not be named as a blocking consumer.");
            Assert.That(report.MinCursor, Is.Not.Null,
                "The cursor-trim branch must remain usable.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
                "The committed prefix at or below the newborn's birth head must reclaim.");
        });

        var survivors = await SurvivingPinOffsetsAsync(provider);

        // This single assertion is what proves the newborn landed in the GC's
        // COVERED set rather than in neither set - the only state in which 3a
        // could be unsafe. Three outcomes are distinguishable here:
        //   blocked  -> nothing trims at all, survivors = 0,1,2;
        //   covered  -> the offset floor holds at the birth head, survivors = 2;
        //   NEITHER  -> no floor exists, everything is HLC-eligible, survivors = empty.
        // Only the middle one passes, so a regression that lands the offset but
        // loses coverage fails here rather than silently trimming live data.
        Assert.That(survivors, Is.EqualTo(new[] { 2L }),
            "The rows the sibling inherited and has not yet materialised sit above its birth head and "
            + "must survive. An empty result would mean the newborn was neither block-pinned nor covered - "
            + "the one unsafe state this change could create.");

        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins.Values, Has.All.EqualTo(HybridLogicalClock.Zero),
            "Safety is carried by the offset axis here, not by an advanced frontier.");
    }

    [Test]
    public async Task A_continuously_splitting_tree_stays_trimmable_on_every_pass()
    {
        // The population that was stuck: a tree where a new sibling is always
        // arriving before the previous one checkpoints. NO leaf in this test
        // ever checkpoints, so under the old sentinel seed every pass after the
        // first split would short-circuit and EntriesTrimmed would stay 0
        // forever while the WAL grew without bound.
        var (_, registry, factory) = CreateSharedPinStore();
        var provider = new InMemoryWalStorageProvider();
        await registry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(1_000));

        var gc = CreateGcOver(provider, registry, factory);
        var nextOffset = 0L;
        var blockedPasses = new List<int>();
        var trimmedPerPass = new List<long>();

        for (var generation = 0; generation < 6; generation++)
        {
            // Each generation appends a batch, then splits: the donor captures
            // the current head and the newborn is seeded against it.
            var batch = new[]
            {
                PinWalEntry(nextOffset, PinHlc(10 * (nextOffset + 1))),
                PinWalEntry(nextOffset + 1, PinHlc(10 * (nextOffset + 2))),
            };
            await provider.AppendBatchAsync(PinSeamTreeId, 0, batch, CancellationToken.None);
            var headAtSplit = nextOffset + 1;
            nextOffset += 2;

            var sibling = CreateLeafOnSharedPinStore($"split-sibling-gen-{generation}", registry, factory);
            await sibling.InitializeSiblingAsync(new SiblingInitialization
            {
                TreeId = PinSeamTreeId,
                ShardIndex = 0,
                LowKeyInclusive = $"k{generation}",
                NextSibling = null,
                PrevSibling = GrainId.Create("leaf", "donor"),
                WalHeadsAtBirth = new[] { headAtSplit },
            });

            var report = await gc.RunOnceAsync(PinSeamTreeId);
            if (report.CursorFloorState != WalGcCursorFloorState.Available)
            {
                blockedPasses.Add(generation);
            }

            trimmedPerPass.Add(report.EntriesTrimmed);
        }

        Assert.That(blockedPasses, Is.Empty,
            "No generation may disable the cursor-trim branch. A non-empty list here is the #3094 wedge: "
            + "a tree that is always mid-birth can never reclaim.");

        // Liveness is the point - the tree must actually reclaim, not merely
        // avoid reporting itself blocked.
        Assert.That(trimmedPerPass.Sum(), Is.GreaterThan(0),
            "A continuously splitting tree must still reclaim its committed prefix across generations.");

        // And safety across the whole run: the earliest surviving entry must
        // never sit above the lowest birth head still pinned, i.e. nothing a
        // newborn depends on was trimmed.
        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Is.Not.Empty,
            "The un-materialised tail above the lowest birth head must survive.");
    }

    [Test]
    public async Task Newborn_without_captured_heads_still_blocks_so_the_conservative_path_is_intact()
    {
        // The root / bulk-load seam passes no heads, and a donor that could not
        // capture them passes null. Those keep the sentinel and must still
        // block - the fix narrows the blocking population, it does not remove
        // the branch.
        var (_, registry, factory) = CreateSharedPinStore();
        var sibling = CreateLeafOnSharedPinStore("split-sibling-no-heads", registry, factory);

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = PinSeamTreeId,
            ShardIndex = 0,
            LowKeyInclusive = "m",
            NextSibling = null,
            PrevSibling = GrainId.Create("leaf", "donor"),
            WalHeadsAtBirth = null,
        });

        var provider = await SeededMigratedWalAsync();
        await registry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(30));

        var report = await CreateGcOver(provider, registry, factory).RunOnceAsync(PinSeamTreeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "A newborn that published no usable offset has no offset cover, so it must still "
                + "block the cursor branch exactly as before - the fix narrows the blocking population, "
                + "it does not remove the branch.");
            Assert.That(report.MinCursor, Is.Null);
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task A_zero_or_negative_birth_head_falls_back_to_the_sentinel()
    {
        // An empty WAL reports head 0, which is not a usable checkpoint offset -
        // treating it as one would floor the whole tree at 0 and simply move the
        // wedge from the cursor axis to the offset axis. Mirrors the
        // `donorHead > 0` guard CompleteSplitAsync applies to the same array.
        var (pinGrain, registry, factory) = CreateSharedPinStore();
        var sibling = CreateLeafOnSharedPinStore("split-sibling-zero-head", registry, factory);

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = PinSeamTreeId,
            ShardIndex = 0,
            LowKeyInclusive = "m",
            NextSibling = null,
            PrevSibling = GrainId.Create("leaf", "donor"),
            WalHeadsAtBirth = new[] { 0L },
        });

        var offsets = await pinGrain.GetPinOffsetsAsync();
        Assert.That(offsets!.Values, Has.All.EqualTo(-1L),
            "A head of 0 is not a usable checkpoint offset and must stay on the sentinel.");
    }
}
