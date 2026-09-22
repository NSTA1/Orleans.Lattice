using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the WAL replay permit admission gate - the per-activation predicate
/// every replaying leaf passes through - so the cost and the allocation of a
/// single admission decision are measurable in the clear, with no Orleans
/// cluster in the loop.
/// <para>
/// The gate is on the activation path of every leaf that has to replay, which
/// during a reactivation storm is every leaf at once, so it is measured rather
/// than reasoned about. Issue #3306 added a freshness test to the smoothed-wait
/// arm of <c>IsReplayPermitQueueNotDraining</c>: a mean that no wait has tested
/// for longer than the configured maximum is discarded instead of being allowed
/// to refuse on evidence from a regime that has ended. The arms below measure
/// the three shapes that test can take, so the claim that the fix is free is a
/// measurement rather than an assertion.
/// </para>
/// <para>
/// Every arm must report <c>0 B</c> allocated. The gate reads and writes
/// <see cref="long"/> statics through <see cref="System.Threading.Volatile"/>
/// and compares <see cref="TimeSpan"/> structs; nothing on the path can reach
/// the heap, and an arm that starts reporting non-zero bytes means something on
/// the admission path has begun boxing or capturing. That is the regression
/// this suite is here to catch, and it is why the arms are cheap enough to be
/// dominated by their own measurement overhead - the <c>Allocated</c> column is
/// the subject, not the nanoseconds.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=replayadmission</c> (or
/// <c>--suite replayadmission</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReplayPermitAdmissionBenchmarks
{
    /// <summary>The shipped default, so the arms measure the real bound.</summary>
    private static readonly TimeSpan MaxQueueWait = new LatticeOptions().WalReplayPermitMaxQueueWait;

    /// <summary>A wait comfortably inside the bound, for the healthy arm.</summary>
    private static readonly TimeSpan HealthyWait = TimeSpan.FromMilliseconds(3);

    /// <summary>
    /// The smoothed wait the incident measured, used so the refusing and
    /// expiring arms are driven with the magnitude that actually occurred.
    /// </summary>
    private static readonly TimeSpan PoisonedMean = TimeSpan.FromMilliseconds(300_044);

    /// <summary>
    /// Leaves the process-wide gate in a known state. BenchmarkDotNet drives
    /// this once per class, and each arm re-seeds what it needs, because the
    /// arms perturb the same statics and would otherwise depend on the order
    /// the harness happened to run them in.
    /// </summary>
    [GlobalSetup]
    public void Setup() => BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

    /// <summary>
    /// Restores the gate so a benchmark class running after this one in the
    /// same process does not inherit a fabricated queue.
    /// </summary>
    [GlobalCleanup]
    public void Cleanup() => BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

    /// <summary>
    /// The overwhelmingly common case: a healthy gate whose recent waits are
    /// well inside the bound. The freshness test short-circuits on the first
    /// comparison, so this is the cost the fix adds to a normal admission.
    /// </summary>
    [Benchmark(Description = "Replay admission - healthy queue", Baseline = true)]
    public bool AdmissionHealthy()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(HealthyWait, sinceLastProgress: TimeSpan.Zero);
        return BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait);
    }

    /// <summary>
    /// A genuinely saturated gate: the mean is over the bound and waits are
    /// still terminating, so the mean is fresh evidence and the arm refuses.
    /// This is the path the gate exists for and it must stay allocation-free.
    /// </summary>
    [Benchmark(Description = "Replay admission - saturated queue")]
    public bool AdmissionSaturated()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(PoisonedMean, sinceLastProgress: TimeSpan.Zero);
        return BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait);
    }

    /// <summary>
    /// The issue #3306 path: the mean is over the bound but nothing has tested
    /// it for longer than the bound itself, so it is discarded and the burst is
    /// admitted. This is the only arm that takes the expiry's write path, which
    /// is why it is measured separately - it is the worst case the fix adds.
    /// </summary>
    [Benchmark(Description = "Replay admission - stale mean expired")]
    public bool AdmissionStaleMeanExpired()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: TimeSpan.Zero,
            sinceLastSample: MaxQueueWait + TimeSpan.FromMinutes(10));
        return BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait);
    }

    /// <summary>
    /// The fold that feeds the mean, driven on the acquisition outcome. It runs
    /// once per terminated wait and now also stamps the sample the arm above
    /// reads, so it is measured to show that stamping is free.
    /// </summary>
    [Benchmark(Description = "Replay permit wait fold")]
    public void NoteQueueWait() =>
        BPlusLeafGrain.NoteReplayPermitQueueWaitForTest(HealthyWait, acquired: true);
}
