using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures the shipping coverage timer's stall eligibility, including its
/// periodic WAL-head probe, without substitute-library or grain RPC allocations.
/// Run with <c>--suite coveragestall</c>.
/// </summary>
[MemoryDiagnoser]
public class CoverageStallTimerBenchmarks
{
    private BPlusLeafGrain _leaf = null!;

    /// <summary>Whether the exclusive WAL head is already covered by the checkpoint.</summary>
    [Params(true, false)]
    public bool CaughtUp { get; set; }

    /// <summary>Builds a resident one-row leaf with checkpoint four and a fixed WAL head.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var factory = new FakeGrainFactory();
        var coordinator = new HeadCoordinator(CaughtUp ? 5 : 6);
        factory.RouteByString<ILeafReplayCoordinatorGrain>(_ => coordinator);
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "coverage-stall";
        state.State.ProjectionCheckpointOffset = 4;
        _leaf = new BPlusLeafGrain(
            new FakeGrainContext(GrainId.Create("leaf", "coverage-stall")),
            state, factory,
            new LatticeOptionsResolver(factory, new FakeOptionsMonitor<LatticeOptions>(new LatticeOptions())),
            new MutationObserverDispatcher([], NullLogger<MutationObserverDispatcher>.Instance),
            new DefaultLatticeOriginClusterIdResolver());
        _leaf.EntriesForTest["k"] = new LwwValue<byte[]> { Value = [1] };
        var offered = false;
        for (var tick = 0; tick < 8; tick++)
            offered |= StallEligibilityAsync().GetAwaiter().GetResult();
        if (offered == CaughtUp)
            throw new InvalidOperationException("The benchmark did not exercise the configured WAL-head decision.");
    }

    /// <summary>Runs the real predicate; false decisions complete without allocating a Task.</summary>
    [Benchmark]
    public ValueTask<bool> StallEligibilityAsync() =>
        _leaf.IsCheckpointStalledBehindLiveDataAsync(1, CancellationToken.None);

    private sealed class HeadCoordinator(long head) : ILeafReplayCoordinatorGrain
    {
        private readonly Task<long> _head = Task.FromResult(head);
        public Task<long> GetHeadOffsetAsync(CancellationToken cancellationToken = default) => _head;
        public Task<long> GetTailOffsetAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
        public Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
            long fromOffsetExclusive, long toOffsetInclusive, int budget,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
            long fromOffsetExclusive, long toOffsetInclusive, int budget, WalKeyFilter filter,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
