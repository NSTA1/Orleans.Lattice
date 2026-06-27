using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the producer-side commit hot path that
/// <see cref="ReplicationMutationObserver.OnMutationAsync"/> pays on every
/// locally-originating, replication-eligible write. This is the exact path
/// narrowed by the "remove write-only vector-clock cache" change (#942): the
/// observer no longer builds a per-commit <c>WalRecord</c> nor clones /
/// snapshots a vector clock, it just nudges the sink with the committed tree
/// id.
/// <para>
/// The benchmark drives the real internal observer (reached via
/// <c>InternalsVisibleTo</c>) with a no-op sink and an allow-all merge-mode
/// resolver, so the measured cost is purely the observer's own per-commit
/// work - no Orleans silo, no grain dispatch, no sink I/O. The supplied
/// mutation carries a populated <see cref="VersionVector"/> so the legacy arm
/// takes its deterministic defensive <c>Clone()</c> path and never cold-starts
/// the cache from a grain, keeping both arms free of I/O.
/// </para>
/// <para>
/// One source, two compile arms so the identical harness measures both sides
/// of the change:
/// <list type="bullet">
///   <item><c>REPLOG_NUDGE</c> defined (this branch): the shipped nudge-only
///     <see cref="IReplogSink.WriteAsync(string, System.Threading.CancellationToken)"/>
///     and the 3-arg observer constructor.</item>
///   <item><c>REPLOG_NUDGE</c> undefined (legacy <c>main</c>): the old
///     <c>WriteAsync(WalRecord, ...)</c> sink and the 4-arg observer
///     constructor that takes the now-removed <c>LocalVectorClockCache</c>.</item>
/// </list>
/// Run it via <c>BENCH_MICROBENCH_SUITE=observer</c> (see <c>Program.cs</c>).
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReplicationCommitObserverBenchmarks
{
    private const string Tree = "microbench-replicated-tree";

    private ReplicationMutationObserver _observer = null!;
    private LatticeMutation _mutation;

    /// <summary>Opts every tree id in so the gating short-circuit is not taken.</summary>
    private sealed class AllowAllResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

#if REPLOG_NUDGE
    /// <summary>Shipped nudge-only sink (this branch).</summary>
    private sealed class NoopSink : IReplogSink
    {
        public Task WriteAsync(string treeId, CancellationToken cancellationToken) => Task.CompletedTask;
    }
#else
    /// <summary>Legacy record-accepting sink (main).</summary>
    private sealed class NoopSink : IReplogSink
    {
        public Task WriteAsync(WalRecord entry, CancellationToken cancellationToken) => Task.CompletedTask;
    }
#endif

    [GlobalSetup]
    public void Setup()
    {
        var options = new LatticeReplicationOptions { ClusterId = "site-a" };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var sink = new NoopSink();
        var resolver = new AllowAllResolver();

#if REPLOG_NUDGE
        _observer = new ReplicationMutationObserver(sink, monitor, resolver);
#else
        // The legacy constructor requires the cache instance. The mutation
        // below carries an explicit VectorClock, so GetSnapshotAsync (the only
        // method that touches the grain factory) is never reached; the mock
        // factory is therefore never invoked.
        var grainFactory = Substitute.For<IGrainFactory>();
        var cache = new LocalVectorClockCache(grainFactory);
        _observer = new ReplicationMutationObserver(sink, monitor, resolver, cache);
#endif

        // A small populated frontier (3 replica entries) so the legacy arm's
        // defensive Clone() allocates a realistic dictionary rather than an
        // empty one, and so neither arm needs to consult the cache.
        var frontier = new VersionVector();
        frontier.Tick("site-a");
        frontier.Tick("site-b");
        frontier.Tick("site-c");

        _mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k-00000001",
            Value = new byte[128],
            VectorClock = frontier,
        };
    }

    /// <summary>
    /// One eligible commit through the observer. On the legacy arm this builds
    /// a <c>WalRecord</c> and clones the frontier before nudging the sink; on
    /// this branch it nudges the sink with the tree id only.
    /// </summary>
    [Benchmark(Description = "Replication observer commit (producer hot path)")]
    public Task ObserveCommit() => _observer.OnMutationAsync(_mutation, CancellationToken.None);
}
