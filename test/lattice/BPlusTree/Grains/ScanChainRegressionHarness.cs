using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A <see cref="ShardRootGrain"/> over a deliberately damaged leaf chain, for
/// the chain-regression reporting fixtures (issue 3341).
/// <para>
/// The shape reproduces the field burst rather than approximating it. A healthy
/// head leaf carries the rows, and every leaf after it in the sibling chain
/// re-offers a key the walk has already consumed, which is precisely what an
/// orphaned leaf does: it claims a range that is not reachable by descent, so
/// its rows duplicate a live leaf's and the scan suppresses them. Because the
/// chain cursor's in-page dedupe is scoped to a single page fill, re-walking the
/// same shard re-reports every damaged leaf, which is why 2,886 damaged leaves
/// produced 110,322 warnings in the field. The harness makes both the leaf count
/// and the page count parameters, so a fixture can assert on the two axes apart.
/// </para>
/// <para>
/// Kept separate from <see cref="ScanPagePrimingHarness"/> deliberately. That
/// harness exists to stall, holds a park that can outlive a test method, and its
/// single intact leaf cannot regress a chain at all. This one never stalls and
/// every read completes synchronously, so a fixture asserting exact totals on a
/// process-wide static counter has no late measurement to race. Each instance
/// carries its own tree id for the same reason.
/// </para>
/// </summary>
internal sealed class ScanChainRegressionHarness
{
    /// <summary>
    /// How many rows the intact head leaf holds. Chosen above one so a page
    /// fill returns a page whose contents a fixture can assert on, which is
    /// what proves the walk actually ran rather than short-circuiting.
    /// </summary>
    internal const int HeadRowCount = 2;

    private ScanChainRegressionHarness(
        ShardRootGrain grain,
        RecordingLoggerFactory logs,
        int damagedLeafCount)
    {
        Grain = grain;
        Logs = logs;
        DamagedLeafCount = damagedLeafCount;
    }

    /// <summary>The grain under test.</summary>
    internal ShardRootGrain Grain { get; }

    /// <summary>Everything the grain logged, so a fixture can bound the volume.</summary>
    internal RecordingLoggerFactory Logs { get; }

    /// <summary>How many chain-regressed leaves this shard's chain carries.</summary>
    internal int DamagedLeafCount { get; }

    /// <summary>
    /// Builds a shard root whose sibling chain is one intact head leaf followed
    /// by <paramref name="damagedLeafCount"/> leaves that each regress the scan's
    /// chain watermark.
    /// </summary>
    /// <param name="treeId">
    /// The tree the grain answers for. Each fixture passes its own, so the
    /// counter listener can attribute every measurement to one grain.
    /// </param>
    /// <param name="damagedLeafCount">
    /// How many damaged leaves to place after the head. Zero builds an intact
    /// chain, which is what a fixture uses to show a measured zero.
    /// </param>
    internal static ScanChainRegressionHarness CreateShard(string treeId, int damagedLeafCount)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", treeId + "/0"));

        var state = new FakePersistentState<ShardRootState>();
        var headId = GrainId.Create("leaf", treeId + "-leaf0");
        state.State.RootNodeId = headId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();

        // The head leaf is healthy and carries every row the scan legitimately
        // returns. Its keys sort above the key the damaged leaves re-offer, so
        // the watermark is already past them by the time the walk arrives.
        var headRows = new List<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < HeadRowCount; i++)
        {
            headRows.Add(new KeyValuePair<string, byte[]>($"k5{i:D3}", [(byte)i]));
        }

        var nextAfterHead = damagedLeafCount > 0
            ? GrainId.Create("leaf", treeId + "-damaged0")
            : (GrainId?)null;

        // Built before the Returns() call, never inside it: NSubstitute treats a
        // substitute configured within another substitute's Returns() as a
        // mis-sequenced call and throws.
        var head = StubLeaf(headRows, headRows[0].Key, nextAfterHead);
        factory.GetGrain<IBPlusLeafGrain>(headId).Returns(head);

        for (var i = 0; i < damagedLeafCount; i++)
        {
            // Every damaged leaf re-offers the same already-consumed key. One
            // row is enough: the chain cursor latches the whole leaf untrusted
            // on its first regressing row, so additional rows would change the
            // scenario's cost without changing what it demonstrates.
            var rows = new List<KeyValuePair<string, byte[]>>
            {
                new("k0000", [0]),
            };

            var next = i + 1 < damagedLeafCount
                ? GrainId.Create("leaf", treeId + $"-damaged{i + 1}")
                : (GrainId?)null;

            var damaged = StubLeaf(rows, "k0000", next);
            factory.GetGrain<IBPlusLeafGrain>(GrainId.Create("leaf", treeId + $"-damaged{i}"))
                .Returns(damaged);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 4096,
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = Timeout.InfiniteTimeSpan,
            },
            shardCount: 1,
            factory: factory);

        var logs = new RecordingLoggerFactory();

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            new Logger<ShardRootGrain>(logs),
            TestMutationObservers.NoObservers());

        return new ScanChainRegressionHarness(grain, logs, damagedLeafCount);
    }

    /// <summary>
    /// Fills one page over the whole keyspace, which walks the entire sibling
    /// chain and therefore meets every damaged leaf exactly once.
    /// </summary>
    internal Task<EntriesPage> FillPageAsync() =>
        Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 4096, continuationToken: null);

    private static IBPlusLeafGrain StubLeaf(
        List<KeyValuePair<string, byte[]>> rows,
        string lowKeyInclusive,
        GrainId? nextSibling)
    {
        var leaf = Substitute.For<IBPlusLeafGrain>();

        // A fresh list per call: the scan path is entitled to own what a leaf
        // hands it, and sharing one instance across page fills would let an
        // earlier page's filtering corrupt a later one.
        leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
            .Returns(_ => Task.FromResult(new List<KeyValuePair<string, byte[]>>(rows)));
        leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
            .Returns(_ => Task.FromResult(rows.ConvertAll(r => r.Key)));
        leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = lowKeyInclusive,
            HighKeyExclusive = null,
        }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult(nextSibling));
        leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));

        return leaf;
    }
}
