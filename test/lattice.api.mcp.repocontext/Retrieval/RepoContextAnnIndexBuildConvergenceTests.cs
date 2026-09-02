using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Regression coverage for the convergence defect that stopped an approximate
/// index ever reaching <c>Ready</c> on a real deployment (#1844), re-expressed
/// against the scheduler that replaced the background build (#1872).
/// </summary>
/// <remarks>
/// <para>
/// WHAT WENT WRONG. A background build used to be a SINGLE attempt: any fault
/// tore the build task down, logged, and re-armed so the next QUERY started a
/// fresh attempt. The stated intent was that re-arming is "naturally rate-limited
/// by query traffic instead of by a timer nobody can see", and that reasoning
/// holds for a cheap retry. It does not hold for an expensive one. A build
/// resumes from its last FLUSHED checkpoint, so a fault costs every vector
/// ingested since that flush.
/// </para>
/// <para>
/// Measured on a restored copy of the live deployment: a corpus of roughly 35,800
/// vectors faulted about once per flush boundary with a 30-second Orleans
/// response timeout against a saturated shard root. Query-gated re-arming
/// therefore converted a TRANSIENT fault into PERMANENT non-convergence.
/// </para>
/// <para>
/// The in-place retry loop that first answered it is gone, and so is the
/// background task: the durable coordinator's phase timer is the retry, and its
/// keep-alive reminder makes that retry survive a process death. What these tests
/// drive is therefore exactly what the coordinator's pump drives - one bounded
/// step per tick, faults absorbed - so they are deterministic rather than timed,
/// with no clock, no delay, and no background task anywhere in the suite.
/// </para>
/// <para>
/// The second half is the cost of the shortfall probe. It is an O(corpus) key
/// walk that only decides whether a RESTORED index is behind; an index this
/// activation just streamed knows what it covers, so paying the walk there is
/// pure cost - and it is the most timeout-prone call in the build.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextAnnIndexBuildConvergenceTests
{
    /// <summary>
    /// A ceiling on pump ticks, so a genuinely non-converging build fails the test
    /// instead of looping. It is not a timeout: every tick is a real bounded build
    /// step, so the count is deterministic for a given corpus and batch size.
    /// </summary>
    private const int MaxTicks = 512;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static float[] Query()
    {
        var vector = new float[AnnPlaneFixture.Space.Dimension];
        vector[0] = 1f;
        return vector;
    }

    private static RepoContextAnnOptions BuildOptions() => new()
    {
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        IngestBatchSize = 8,
        MaxItemsPerChunk = 16,
    };

    [Test]
    public async Task A_transient_store_fault_does_not_abandon_the_build()
    {
        using var fixture = new AnnPlaneFixture(BuildOptions());
        fixture.SeedRing(64);

        // Three consecutive faults, which is more than the single attempt the old
        // implementation allowed, so the test distinguishes "absorbs a transient"
        // from "gives up on the first one".
        fixture.Source.FailNextEnumerations(3, static () => new TimeoutException(
            "Response did not arrive on time in 00:00:30 for message: GetSortedEntriesBatchAsync"));

        var ticks = await PumpAsync(fixture, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "A build interrupted by a transient store fault must resume from its checkpoint and reach Ready. "
                + "Abandoning it and waiting for a query to re-arm it turns a transient fault into permanent "
                + "non-convergence on a corpus large enough to fault once per flush.");
            Assert.That(
                fixture.Registry.TryGetProgress(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var progress)
                && progress.Phase == VectorIndexBuildPhase.Ready,
                Is.True,
                "the pump must leave the index Ready, not merely stop faulting");
        });
    }

    [Test]
    public async Task A_build_that_faults_repeatedly_does_not_re_pay_the_count_probe_per_attempt()
    {
        using var fixture = new AnnPlaneFixture(BuildOptions());
        fixture.SeedRing(64);

        // Three faults, so the old query-gated re-arm would have made three separate
        // attempts, each re-opening the index and each paying the O(corpus) probe on
        // the way through. The cost of a retry is the whole point: re-arming was
        // justified as "naturally rate-limited by query traffic", which is sound
        // only while a retry is cheap.
        fixture.Source.FailNextEnumerations(3, static () => new TimeoutException(
            "Response did not arrive on time in 00:00:30 for message: GetSortedEntriesBatchAsync"));

        var ticks = await PumpAsync(fixture, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "the build must reach Ready for this assertion to mean anything");
            Assert.That(fixture.Source.CountCalls, Is.LessThanOrEqualTo(1),
                "The O(corpus) shortfall probe must be paid at most once for a build, not once per attempt. "
                + "Scaling it with the retry count is what let a corpus large enough to fault per flush spend "
                + "more time re-counting than ingesting.");
        });
    }

    [Test]
    public async Task A_restored_index_still_pays_the_probe_because_it_cannot_know_what_it_missed()
    {
        using var fixture = new AnnPlaneFixture(BuildOptions());
        fixture.SeedRing(64);

        Assert.That(await PumpAsync(fixture, Ct), Is.LessThan(MaxTicks));

        // Restart: the next open RESTORES from durable state rather than building,
        // which is the one case where the count is the only way to learn whether
        // the persisted index is behind the store of record.
        fixture.Restart();
        fixture.Source.Set("vec-999999", "repo/acme/file/new.cs", Query());

        Assert.That(await PumpAsync(fixture, Ct), Is.LessThan(MaxTicks));

        Assert.That(fixture.Source.CountCalls, Is.GreaterThan(0),
            "A restored index did not stream the corpus in this process, so it must probe rather than assume.");
    }

    /// <summary>
    /// Drives the plane exactly as the build coordinator's phase pump does: one
    /// bounded step per tick, with a faulting step swallowed and left to the next
    /// tick rather than tearing anything down. Returns the tick count, which
    /// reaches <see cref="MaxTicks"/> only when the build never converges.
    /// </summary>
    private static async Task<int> PumpAsync(AnnPlaneFixture fixture, CancellationToken cancellationToken)
    {
        for (var tick = 1; tick <= MaxTicks; tick++)
        {
            try
            {
                var progress = await fixture.Registry
                    .BuildStepAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, cancellationToken);
                if (progress.Phase == VectorIndexBuildPhase.Ready)
                {
                    return tick;
                }
            }
            catch (Exception)
            {
                // Exactly what CoordinatorGrain does with a faulting phase tick: log
                // it and leave the timer running. The next tick resumes the build
                // from its last checkpoint.
            }
        }

        return MaxTicks;
    }
}
