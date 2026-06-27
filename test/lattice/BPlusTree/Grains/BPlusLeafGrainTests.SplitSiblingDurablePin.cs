using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Mechanism-1 regression for issue #947: a data-capable leaf must durably seed
/// a <see cref="HybridLogicalClock.Zero"/> "block" pin at <i>birth</i> - the
/// moment it first acquires a tree id (a split sibling's
/// <see cref="IBPlusLeafGrain.InitializeSiblingAsync"/> or a root/bulk-load
/// leaf's <see cref="IBPlusLeafGrain.SetTreeIdAsync"/>) - so the WAL GC cannot
/// trim past the leaf's un-materialised frontier in the window before its first
/// checkpoint.
/// <para>
/// The companion mechanism-2 guard (PR #946) only <em>surfaces</em> the loss
/// after the fact (a cold replay throws <c>LeafProjectionStaleException</c>);
/// this fix <em>prevents</em> the early trim. These tests drive the real leaf
/// birth seam through the real <see cref="LeafCursorReporter"/> and
/// <see cref="WalMaterialiserPinGrain"/>, then run the real
/// <see cref="LatticeWalGc"/> over an <see cref="InMemoryWalStorageProvider"/>
/// and assert the seeded pin blocks the trim that a forward consumer would
/// otherwise drive.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string PinSeamTreeId = "tree-mech1";

    private static HybridLogicalClock PinHlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalEntry PinWalEntry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = PinSeamTreeId,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static IOptionsMonitor<LatticeOptions> PinOptionsMonitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Wires a real <see cref="LeafCursorReporter"/> over a real
    /// <see cref="WalMaterialiserPinGrain"/> (durable pin store) and a shared
    /// <see cref="InMemoryWalCursorRegistry"/>, and constructs a leaf grain that
    /// reports through them. The same <see cref="IGrainFactory"/> is handed to
    /// the GC so both sides observe one pin store.
    /// </summary>
    private static (BPlusLeafGrain Leaf, WalMaterialiserPinGrain PinGrain,
        InMemoryWalCursorRegistry Registry, IGrainFactory Factory)
        CreateLeafWithDurablePinStore(string leafKey, string? treeId)
    {
        var registry = new InMemoryWalCursorRegistry();

        var pinContext = Substitute.For<IGrainContext>();
        pinContext.GrainId.Returns(GrainId.Create("wal-materialiser-pin", PinSeamTreeId));
        var pinGrain = new WalMaterialiserPinGrain(pinContext, new FakePersistentState<WalMaterialiserPinState>());

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);

        var reporter = new LeafCursorReporter(registry, factory);

        var services = new ServiceCollection();
        services.AddSingleton<ILeafCursorReporter>(reporter);
        var provider = services.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey));
        context.ActivationServices.Returns(provider);

        var state = new FakePersistentState<LeafNodeState>();
        if (treeId is not null)
            state.State.TreeId = treeId;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 }, maxLeafKeys: 128, shardCount: 1, factory: factory);
        var leaf = new BPlusLeafGrain(
            context, state, factory, optionsResolver, TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (leaf, pinGrain, registry, factory);
    }

    private static async Task<InMemoryWalStorageProvider> SeededMigratedWalAsync()
    {
        // Three WAL entries representing the data a fresh leaf inherits/accepts
        // before it checkpoints. Their HLCs (10/20/30) all sit AT OR BELOW the
        // forward consumer's cursor (30), so without a block pin the GC would
        // trim every one of them.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            PinSeamTreeId,
            0,
            new[] { PinWalEntry(0, PinHlc(10)), PinWalEntry(1, PinHlc(20)), PinWalEntry(2, PinHlc(30)) },
            CancellationToken.None);
        return provider;
    }

    private static async Task<List<long>> SurvivingPinOffsetsAsync(IWalStorageProvider provider)
    {
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(
            PinSeamTreeId, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }
        return survivors;
    }

    [Test]
    public async Task InitializeSibling_birth_seeds_block_pin_so_wal_gc_does_not_trim_past_split_sibling_frontier()
    {
        // A freshly created split sibling is born WITHOUT a tree id (the donor
        // seeds it via InitializeSiblingAsync). After birth it holds inherited
        // data in the WAL but has not yet checkpointed.
        var (sibling, pinGrain, registry, factory) =
            CreateLeafWithDurablePinStore(leafKey: "split-sibling-1", treeId: null);

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = PinSeamTreeId,
            ShardIndex = 0,
            LowKeyInclusive = "m",
            HighKeyExclusive = null,
            NextSibling = null,
            PrevSibling = GrainId.Create("leaf", "donor"),
        });

        // The birth seam must have durably seeded a Zero block pin for the
        // sibling. This is the load-bearing assertion: on baseline (no seed)
        // the pin store is empty for the sibling.
        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins, Is.Not.Empty,
            "InitializeSiblingAsync must durably seed a block pin for the newborn sibling "
            + "before its inherited data becomes reachable in the WAL.");
        Assert.That(pins.Values, Has.All.EqualTo(HybridLogicalClock.Zero),
            "The seeded pin must be a Zero block pin (the sibling has checkpointed none of its data).");

        // Now drive the real WAL GC: a forward consumer (shipper) sits at the
        // WAL head and the sibling is ABSENT from the in-memory registry (it
        // never reported a cursor - only a durable pin). Without the seeded pin
        // the GC trims the whole prefix the sibling still needs to replay.
        var provider = await SeededMigratedWalAsync();
        await registry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(30));

        var gcServices = new ServiceCollection();
        gcServices.AddSingleton<IWalStorageProvider>(provider);
        gcServices.AddSingleton(factory);
        var gc = new LatticeWalGc(gcServices.BuildServiceProvider(), registry, PinOptionsMonitor());

        var report = await gc.RunOnceAsync(PinSeamTreeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.Null,
                "The sibling's Zero block pin must disable the cursor-trim branch entirely.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0),
                "No WAL entry may be trimmed while the split sibling's frontier is un-materialised.");
        });

        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 0L, 1L, 2L }),
            "Every inherited WAL entry the sibling needs to replay must survive the GC pass.");
    }

    [Test]
    public async Task SetTreeId_birth_seeds_block_pin_so_wal_gc_does_not_trim_past_root_leaf_frontier()
    {
        // A root / bulk-load leaf acquires its tree id via SetTreeIdAsync and is
        // then handed data via MergeEntriesAsync before it ever checkpoints.
        var (leaf, pinGrain, registry, factory) =
            CreateLeafWithDurablePinStore(leafKey: "root-leaf-1", treeId: null);

        await leaf.SetTreeIdAsync(PinSeamTreeId);

        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins, Is.Not.Empty,
            "SetTreeIdAsync must durably seed a block pin so a leaf that accepts writes before its "
            + "first checkpoint still floors the WAL GC.");
        Assert.That(pins.Values, Has.All.EqualTo(HybridLogicalClock.Zero));

        var provider = await SeededMigratedWalAsync();
        await registry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(30));

        var gcServices = new ServiceCollection();
        gcServices.AddSingleton<IWalStorageProvider>(provider);
        gcServices.AddSingleton(factory);
        var gc = new LatticeWalGc(gcServices.BuildServiceProvider(), registry, PinOptionsMonitor());

        var report = await gc.RunOnceAsync(PinSeamTreeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.Null);
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        });

        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task Block_pin_lifts_after_leaf_reports_real_frontier_so_steady_state_trim_resumes()
    {
        // Conservative-fix guard: the Zero block pin is transient. Once the leaf
        // produces its first real checkpoint frontier the pin advances past
        // Zero and the GC resumes trimming the committed prefix - steady-state
        // trimming for an already-protected (checkpointed) leaf is unchanged.
        var (sibling, pinGrain, registry, factory) =
            CreateLeafWithDurablePinStore(leafKey: "split-sibling-2", treeId: null);

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = PinSeamTreeId,
            ShardIndex = 0,
            LowKeyInclusive = "m",
            HighKeyExclusive = null,
            NextSibling = null,
            PrevSibling = GrainId.Create("leaf", "donor"),
        });

        // Recover the sibling's exact consumer id from the seeded pin so the
        // "first real checkpoint" can advance the same key.
        var seededPins = await pinGrain.GetPinsAsync();
        var consumerId = seededPins.Keys.Single();

        // The leaf checkpoints at HLC 20: it reports its in-memory cursor AND
        // advances the durable pin past Zero (both happen on a real flush).
        await registry.ReportCursorAsync(PinSeamTreeId, consumerId, PinHlc(20));
        await pinGrain.ReportAsync(consumerId, PinHlc(20));

        var provider = await SeededMigratedWalAsync();

        var gcServices = new ServiceCollection();
        gcServices.AddSingleton<IWalStorageProvider>(provider);
        gcServices.AddSingleton(factory);
        var gc = new LatticeWalGc(gcServices.BuildServiceProvider(), registry, PinOptionsMonitor());

        var report = await gc.RunOnceAsync(PinSeamTreeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(PinHlc(20)),
                "Once the leaf has checkpointed, its real frontier governs the trim floor.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
                "The committed prefix at or below the leaf's checkpoint is now trim-eligible.");
        });

        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 2L }),
            "Only the entry above the leaf's checkpoint frontier is retained.");
    }
}
