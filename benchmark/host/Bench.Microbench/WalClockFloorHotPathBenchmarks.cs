using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Hot-path cost of the WAL clock floor (issue #4586). The floor adds no grain
/// call and no storage access to a write: a write pays one admission check
/// under the WAL partition's existing state gate, and one carried-stamp
/// classification when the commit-log writer routes the record. Both lanes
/// call the <b>real production code</b>. The baseline lane is the empty
/// per-append path a write paid before the floor existed.
/// <para>
/// Lanes:
/// </para>
/// <list type="bullet">
/// <item><description><c>Admission_no_floor</c>: a tree that is not
/// replicated (zero floor), the steady state of every single-cluster tree.</description></item>
/// <item><description><c>Admission_floor_fresh_stamp</c>: a replicated
/// tree's partition with a published floor and a fresh stamp above it, the
/// steady state of every replicated write.</description></item>
/// <item><description><c>Classify</c>: the writer's carried-stamp
/// classification, which reads the ambient HLC override from the request
/// context. <see cref="OverrideInScope"/> selects an ordinary write (no
/// override) or one already stamped under an override; the scope itself is
/// set up outside the measurement, because its owner pays for it whether or not
/// the floor exists.</description></item>
/// </list>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=walclockfloor</c> (or
/// <c>--suite walclockfloor</c>). No silo is involved.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class WalClockFloorHotPathBenchmarks
{
    private const string Local = "site-a";
    private WalRecord _record;
    private HybridLogicalClock _floor;

    /// <summary>Whether an HLC override is in scope when the writer classifies the record.</summary>
    [Params(false, true)]
    public bool OverrideInScope { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var now = DateTimeOffset.UtcNow.UtcTicks;
        _floor = new HybridLogicalClock { WallClockTicks = now - TimeSpan.FromSeconds(60).Ticks };
        _record = new WalRecord
        {
            TreeId = "t",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = new HybridLogicalClock { WallClockTicks = now },
            OriginClusterId = Local,
        };
        RequestContext.Clear();
        if (OverrideInScope)
        {
            LatticeHlcOverrideContext.Current = _floor;
        }
    }

    [Benchmark(Baseline = true)]
    public bool Baseline_no_check() => true;

    [Benchmark]
    public bool Admission_no_floor() => WalClockFloorCore.IsAdmitted(in _record, HybridLogicalClock.Zero, Local);

    [Benchmark]
    public bool Admission_floor_fresh_stamp() => WalClockFloorCore.IsAdmitted(in _record, _floor, Local);

    [Benchmark]
    public bool Classify() =>
        LatticeHlcOverrideContext.Current is not null && !LatticeFreshStampContext.IsActive;
}
