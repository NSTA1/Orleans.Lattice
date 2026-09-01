using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Regression coverage for the convergence defect that stopped an approximate
/// index ever reaching <c>Ready</c> on a real deployment (#1844), even after the
/// enumerator-abort defect was fixed.
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
/// therefore converted a TRANSIENT fault into PERMANENT non-convergence - the
/// index sat at one flush of 4,096 vectors and never reached Ready, so query cost
/// stayed proportional to corpus size and cold start could not improve.
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
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static float[] Query()
    {
        var vector = new float[AnnPlaneFixture.Space.Dimension];
        vector[0] = 1f;
        return vector;
    }

    private static RepoContextAnnOptions BuildOptions() => new()
    {
        AutoBuild = true,
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
        // implementation allowed and fewer than the in-place retry budget, so the
        // test distinguishes "absorbs a transient" from "retries forever".
        fixture.Source.FailNextEnumerations(3, static () => new TimeoutException(
            "Response did not arrive on time in 00:00:30 for message: GetSortedEntriesBatchAsync"));

        // The first query arms the background build and is answered by the exact
        // path, exactly as an unbuilt deployment behaves.
        await fixture.SearchAsync(Query(), 5, Ct);

        var serving = await WaitForServingAsync(fixture, TimeSpan.FromSeconds(120));

        Assert.That(serving, Is.True,
            "A build interrupted by a transient store fault must resume from its checkpoint and reach Ready. "
            + "Tearing the build down and waiting for a query to re-arm it turns a transient fault into "
            + "permanent non-convergence on a corpus large enough to fault once per flush.");
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

        await fixture.SearchAsync(Query(), 5, Ct);
        var serving = await WaitForServingAsync(fixture, TimeSpan.FromSeconds(120));

        Assert.Multiple(() =>
        {
            Assert.That(serving, Is.True, "the build must reach Ready for this assertion to mean anything");
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

        await fixture.SearchAsync(Query(), 5, Ct);
        Assert.That(await WaitForServingAsync(fixture, TimeSpan.FromSeconds(120)), Is.True);

        // Restart: the next open RESTORES from durable state rather than building,
        // which is the one case where the count is the only way to learn whether
        // the persisted index is behind the store of record.
        fixture.Restart();
        fixture.Source.Set("vec-999999", "repo/acme/file/new.cs", Query());

        await fixture.SearchAsync(Query(), 5, Ct);
        Assert.That(await WaitForServingAsync(fixture, TimeSpan.FromSeconds(120)), Is.True);

        Assert.That(fixture.Source.CountCalls, Is.GreaterThan(0),
            "A restored index did not stream the corpus in this process, so it must probe rather than assume.");
    }

    private static async Task<bool> WaitForServingAsync(AnnPlaneFixture fixture, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var outcome = await fixture.SearchAsync(Query(), 5, CancellationToken.None);
            if (outcome.State != RepoContextAnnServingState.Bootstrapping)
            {
                return true;
            }

            await Task.Delay(100, CancellationToken.None);
        }

        return false;
    }
}
