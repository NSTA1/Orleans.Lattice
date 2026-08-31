using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Allocation probes for the per-request approximate retrieval path, plus the
/// battery that proves the probe can actually fail.
/// <para>
/// Every assertion here is <b>differential</b>: the same work is measured at two
/// sizes after a full-size warm-up and only the growth between them is asserted,
/// with the minimum kept across repeats. An absolute assertion against a GC
/// counter passes alone and fails in a larger batch, because whether the runtime
/// recompiles inside the measured window depends on what the shared test host has
/// already compiled. The search path awaits, so every probe uses the process-wide
/// counter: the per-thread one returns nonsense across a continuation that
/// resumed on another thread.
/// </para>
/// <para>
/// The property that actually matters is not "zero" - the seam returns a list of
/// matches, so a query is obliged to allocate one - but that the per-query cost is
/// <b>bounded and independent of the corpus</b>. That is the whole thesis of
/// routing retrieval through an index: the exact scan it replaces allocates a
/// decoded candidate for every stored vector on every cache miss, so its per-query
/// allocation is proportional to the corpus by construction.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnAllocationTests
{
    private const int K = 10;

    private static CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static float[] Query()
    {
        var vector = new float[AnnPlaneFixture.Space.Dimension];
        vector[0] = 1f;
        return vector;
    }

    private static AnnPlaneFixture BuiltFixture(int corpus)
    {
        var fixture = new AnnPlaneFixture(new RepoContextAnnOptions
        {
            AutoBuild = false,
            MinimumTrainingCount = 8,
            PartitionCount = 4,
            Probes = 4,
        });
        fixture.SeedRing(corpus);
        fixture.BuildAsync(Ct).GetAwaiter().GetResult();
        return fixture;
    }

    /// <summary>
    /// Awaits a search without <c>AsTask()</c>. That matters here and nowhere else:
    /// <c>AsTask</c> allocates a <see cref="Task{TResult}"/> even for an already
    /// completed <see cref="ValueTask{TResult}"/>, so measuring through it would
    /// charge the probe's own scaffolding to the path under test and report a
    /// per-call cost the production caller never pays.
    /// </summary>
    private static RepoContextAnnSearchOutcome Await(ValueTask<RepoContextAnnSearchOutcome> search)
        => search.IsCompletedSuccessfully ? search.Result : search.AsTask().GetAwaiter().GetResult();

    private static void RunQueries(AnnPlaneFixture fixture, int iterations)
    {
        // Read once, outside the loop: TestContext.CurrentContext allocates a fresh
        // context object per read, and charging that to the path under test would
        // measure the harness rather than the query.
        var cancellationToken = Ct;
        var query = Query();
        var total = 0;
        for (var i = 0; i < iterations; i++)
        {
            total += Await(fixture.SearchAsync(query, K, cancellationToken)).Matches.Count;
        }

        AllocationProbe.ScalarSink = total;
    }

    [Test]
    public void A_query_against_the_approximate_index_allocates_a_bounded_amount_per_call()
    {
        using var fixture = BuiltFixture(256);

        var growth = AllocationProbe.Growth(
            prepare: _ => fixture,
            measure: RunQueries,
            smallSize: 200,
            largeSize: 400,
            crossesThreads: true);

        // Per query, over the 200 extra iterations the large window runs.
        var perQuery = growth / 200d;
        TestContext.Out.WriteLine($"Approximate query allocation: {perQuery:F0} bytes per call at k={K}.");

        Assert.Multiple(() =>
        {
            Assert.That(perQuery, Is.GreaterThan(0d),
                "The probe measures something: the seam returns a list of matches, so a query must allocate one. "
                + "A zero here would mean the probe was measuring nothing at all.");
            Assert.That(perQuery, Is.LessThan(4_096d),
                "A query's allocation is a small, bounded envelope - the result list, its identifiers, and the "
                + "async state machines - and never a copy of the corpus.");
        });
    }

    [Test]
    public void A_query_allocates_no_more_against_a_corpus_four_times_the_size()
    {
        using var small = BuiltFixture(128);
        using var large = BuiltFixture(512);

        var smallGrowth = AllocationProbe.Growth(
            prepare: _ => small, measure: RunQueries, smallSize: 200, largeSize: 400, crossesThreads: true);
        var largeGrowth = AllocationProbe.Growth(
            prepare: _ => large, measure: RunQueries, smallSize: 200, largeSize: 400, crossesThreads: true);

        TestContext.Out.WriteLine(
            $"Per-query allocation: {smallGrowth / 200d:F0} bytes at 128 vectors, "
            + $"{largeGrowth / 200d:F0} bytes at 512 vectors.");

        Assert.That(largeGrowth, Is.LessThanOrEqualTo((smallGrowth * 2) + 1_024),
            "This is the property the whole change turns on: query cost must not scale with the corpus. The "
            + "exact scan it replaces decodes a candidate per stored vector, so its per-query allocation is "
            + "proportional to the corpus by construction.");
    }

    [Test]
    public void Reporting_a_bootstrapping_outcome_allocates_nothing_per_call()
    {
        // The common case on an unbuilt deployment, so it must not be the expensive one.
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);

        // The counter choice below is only sound if this path never awaits, so that
        // premise is asserted rather than assumed: a future change that made
        // declining asynchronous would otherwise leave the probe silently measuring
        // a per-thread counter across a continuation that migrated threads.
        Assert.That(
            fixture.SearchAsync(Query(), K, Ct).IsCompletedSuccessfully, Is.True,
            "Declining is a synchronous read of a volatile flag, so it completes without ever awaiting.");

        var growth = AllocationProbe.Growth(
            prepare: _ => fixture,
            measure: static (rig, iterations) =>
            {
                var cancellationToken = Ct;
                var query = Query();
                var declined = 0;
                for (var i = 0; i < iterations; i++)
                {
                    if (Await(rig.SearchAsync(query, K, cancellationToken)).State
                        == RepoContextAnnServingState.Bootstrapping)
                    {
                        declined++;
                    }
                }

                AllocationProbe.ScalarSink = declined;
            },
            smallSize: 2_000,
            largeSize: 4_000,

            // The per-thread counter, because this path never awaits and the
            // assertion has no headroom at all: the process-wide counter would fold
            // in whatever a background or finalizer thread happened to allocate
            // inside the larger window and turn an exact-zero assertion into a
            // occasionally-failing one.
            crossesThreads: false);

        Assert.That(growth, Is.Zero,
            "Declining is a synchronous read of a volatile flag returning a cached outcome, so it must cost "
            + "nothing per call: an unbuilt deployment declines on every query until its build completes.");
    }

    [Test]
    public void The_probe_detects_an_allocation_that_provably_escapes()
    {
        // The battery. The allocation is stored into a static field, which is a
        // definite escape at every JIT tier: a non-escaping constant-size allocation
        // is removed outright by escape analysis, and a battery test whose allocation
        // is elided truthfully reports zero and becomes the exact false negative it
        // exists to prevent. This escape is load-bearing - do not "simplify" it.
        var growth = AllocationProbe.Growth(
            prepare: static _ => 0,
            measure: static (_, iterations) =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    AllocationProbe.EscapeSink = new object();
                }
            },
            smallSize: 1_000,
            largeSize: 2_000,
            crossesThreads: false);

        Assert.That(growth, Is.GreaterThan(0L),
            "A probe that cannot fail silently approves the regression it exists to catch.");
    }

    [Test]
    public void The_probe_reports_zero_for_a_loop_that_allocates_nothing()
    {
        // The other half of the battery: a harness wired to always report a number
        // would pass the escaping case above and fail here.
        var growth = AllocationProbe.Growth(
            prepare: static _ => 0,
            measure: static (_, iterations) =>
            {
                var total = 0L;
                for (var i = 0; i < iterations; i++)
                {
                    total += i;
                }

                AllocationProbe.ScalarSink = total;
            },
            smallSize: 1_000,
            largeSize: 2_000,
            crossesThreads: false);

        Assert.That(growth, Is.Zero);
    }
}
