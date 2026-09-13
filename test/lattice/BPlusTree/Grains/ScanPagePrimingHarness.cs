using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A minimal single-leaf <see cref="ShardRootGrain"/> for the scan-page stall
/// phase priming fixtures (issue #2952).
/// <para>
/// Deliberately separate from the parkable-leaf helper in
/// <c>ShardRootGrainScanPageLeafReadCoalescingTests</c> rather than shared with
/// it. Those fixtures start scans with a discarded task that outlives the test
/// method, and this one asserts exact per-arm totals on a process-wide static
/// counter; coupling the two would let a sibling's late measurement land inside
/// this listener's window. Each harness instance also carries its own tree id,
/// so the listener filter can attribute every measurement to one grain.
/// </para>
/// </summary>
internal sealed class ScanPagePrimingHarness
{
    /// <summary>How many rows the single leaf holds.</summary>
    internal const int RowCount = 2;

    private readonly TaskCompletionSource<List<KeyValuePair<string, byte[]>>> _park = new();
    private readonly List<KeyValuePair<string, byte[]>> _rows = [];
    private readonly bool _park_enabled;

    private ScanPagePrimingHarness(bool park) => _park_enabled = park;

    /// <summary>The grain under test.</summary>
    internal ShardRootGrain Grain { get; private set; } = null!;

    /// <summary>
    /// Unparks a parked read so a stalled scan's abandoned continuation can
    /// complete rather than being left pending for the life of the run.
    /// </summary>
    internal void Release() => _park.TrySetResult([.. _rows]);

    /// <summary>
    /// Builds a shard root over one leaf holding <see cref="RowCount"/> rows.
    /// </summary>
    /// <param name="treeId">
    /// The tree the grain answers for; each fixture passes its own so the
    /// listener can filter to it.
    /// </param>
    /// <param name="stallDuration">
    /// The scan-page stall ceiling. <see cref="Timeout.InfiniteTimeSpan"/>
    /// leaves the page fill unguarded and it completes.
    /// </param>
    /// <param name="park">
    /// When true the leaf read never completes on its own, so a guarded scan
    /// stalls. Used only by the positive control.
    /// </param>
    internal static ScanPagePrimingHarness CreateShard(
        string treeId,
        TimeSpan stallDuration,
        bool park = false)
    {
        var harness = new ScanPagePrimingHarness(park);

        for (var i = 0; i < RowCount; i++)
        {
            harness._rows.Add(new KeyValuePair<string, byte[]>($"k{i:D4}", [(byte)i]));
        }

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", treeId + "/0"));

        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", treeId + "-leaf0");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
            .Returns(_ => harness._park_enabled
                ? harness._park.Task
                : Task.FromResult(new List<KeyValuePair<string, byte[]>>(harness._rows)));
        leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = harness._rows[0].Key,
            HighKeyExclusive = null,
        }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult((GrainId?)null));
        leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 4096,
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
            },
            shardCount: 1,
            factory: factory);

        harness.Grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return harness;
    }}
