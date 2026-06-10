using System.Diagnostics;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

// Co-location read pass-through micro-probe for issue #386 candidate 1.
//
// Pins the primary BPlusLeafGrain and its LeafCacheGrain to a single-silo
// TestCluster so they are provably co-located, then runs a 1,000,000
// GetAsync burst against a 10,000-entry leaf and reports four numbers for
// each cohort:
//   * per-call wall-time p50 / p95 (microseconds)
//   * allocations per call (bytes, GC.GetTotalAllocatedBytes delta / burst)
//   * steady-state working-set delta (bytes, Process.WorkingSet64)
//
// Cohort "baseline-mirror"        : CoLocationReadPassThrough = false (current behaviour).
// Cohort "candidate-passthrough"  : CoLocationReadPassThrough = true  (mirror bypassed).
//
// Acceptance rule (issue #386, candidate 1): ship the behaviour change only
// if the within-cohort working-set delta is materially smaller for the
// candidate AND the per-call wall-time does not regress beyond measurement
// noise. The two cohorts run in one process, so compare the *within-cohort*
// WS delta (mirror populates during baseline's burst, stays flat for the
// candidate) rather than the absolute end-of-burst working set.

const int EntryCount = 10_000;
const long ReadBurst = 1_000_000;
const int WarmupReads = 10_000;
const int ValueBytes = 64;
const string TreeId = "leafcache-colocation-probe";

Console.WriteLine("cohort,p50_us,p95_us,alloc_bytes_per_call,workingset_delta_bytes");
await RunCohort("baseline-mirror", passThrough: false);
await RunCohort("candidate-passthrough", passThrough: true);

static async Task RunCohort(string name, bool passThrough)
{
    Environment.SetEnvironmentVariable("BENCH_COLOCATION_PASSTHROUGH", passThrough ? "true" : "false");
    var cluster = new TestClusterBuilder(1)
        .AddSiloBuilderConfigurator<SiloConfigurator>()
        .Build();
    await cluster.DeployAsync();
    try
    {
        var lattice = cluster.GrainFactory.GetGrain<ILattice>(TreeId);
        var value = new byte[ValueBytes];
        Random.Shared.NextBytes(value);
        for (var i = 0; i < EntryCount; i++)
        {
            await lattice.SetAsync($"key-{i:D6}", value);
        }

        // Sample the working set BEFORE the first read so the mirror's
        // population during the burst is included in the baseline cohort's
        // delta and the candidate cohort's flat mirror is visible as a
        // materially smaller delta.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var wsStart = Process.GetCurrentProcess().WorkingSet64;

        for (var i = 0; i < WarmupReads; i++)
        {
            _ = await lattice.GetAsync($"key-{i % EntryCount:D6}");
        }

        var allocStart = GC.GetTotalAllocatedBytes(precise: true);
        var samples = new double[ReadBurst];
        var tickToUs = 1_000_000.0 / Stopwatch.Frequency;
        for (long i = 0; i < ReadBurst; i++)
        {
            var k = $"key-{(int)(i % EntryCount):D6}";
            var t0 = Stopwatch.GetTimestamp();
            _ = await lattice.GetAsync(k);
            samples[i] = (Stopwatch.GetTimestamp() - t0) * tickToUs;
        }
        var allocEnd = GC.GetTotalAllocatedBytes(precise: true);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var wsEnd = Process.GetCurrentProcess().WorkingSet64;

        Array.Sort(samples);
        double Percentile(double q) => samples[(int)(q * (samples.Length - 1))];
        var allocPerCall = (allocEnd - allocStart) / (double)ReadBurst;
        Console.WriteLine(
            $"{name},{Percentile(0.50):F3},{Percentile(0.95):F3},{allocPerCall:F1},{wsEnd - wsStart}");
    }
    finally
    {
        await cluster.StopAllSilosAsync();
        await cluster.DisposeAsync();
    }
}

file sealed class SiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        siloBuilder.UseInMemoryReminderService();
        var passThrough = Environment.GetEnvironmentVariable("BENCH_COLOCATION_PASSTHROUGH") == "true";
        siloBuilder.ConfigureLattice(o => o.CoLocationReadPassThrough = passThrough);
    }
}
