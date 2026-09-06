using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the resumability of
/// <see cref="RepoContextVectorWriter.LoadEmbeddedMemoryKeysAsync"/>, which is the
/// backstop the memory arm falls back on when the per-source membership flag has
/// been lost to a timed-out write.
/// <para>
/// These pin the fix for issue #2071. The marker load used to be a single walk of
/// the whole marker range that kept nothing when it failed: on a real,
/// leaf-fragmented membership tree a page could not be filled inside the shard's
/// page-fill ceiling, the scan threw, and the next reconcile restarted it from the
/// very beginning and threw in the same place. It therefore never completed once,
/// which left the orphan sweep permanently un-run and the marker evidence
/// permanently unavailable. The fix banks each completed page and its continuation
/// token, so progress is monotonic across passes and the walk finishes in a bounded
/// number of them.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextVectorWriterResumableMarkerScanTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// Comfortably more markers than one scan page holds, so the walk is genuinely
    /// multi-page and a fault can land part-way through it.
    /// </summary>
    private const int MarkerCount = 96;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static string[] MarkerKeys()
        => Enumerable.Range(0, MarkerCount)
            .Select(i => RepoContextKeys.Memory(RepoId, "gotchas", $"note-{i:D3}"))
            .ToArray();

    /// <summary>
    /// An injector aimed at the shard-level range read the marker walk actually
    /// performs. It starts passive (<see cref="LatticeTreeFaultInjector.FailFirst"/>
    /// zero), so a test can first use its
    /// <see cref="LatticeTreeFaultInjector.Matched"/> tally to measure what a
    /// healthy walk costs and only then start faulting at a measured point - which
    /// is what makes "part-way through" deterministic without hard-coding the
    /// tree's shard fan-out.
    /// </summary>
    private static LatticeTreeFaultInjector RangeReadInjector() => new()
    {
        TreeId = RepoContextTrees.VectorMembership,
        Method = "GetSortedEntriesBatchAsync",
        IncludeShardGrains = true,
        FailFirst = 0,
    };

    private static RepoContextMcpHarnessOptions Options(LatticeTreeFaultInjector injector) => new()
    {
        Posture = RepoContextMcpAuthPosture.Writer,
        ConfigureSilo = silo =>
        {
            silo.Services.AddSingleton(injector);
            silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
        },
    };

    [Test]
    public async Task The_marker_scan_walks_a_multi_page_range_to_completion()
    {
        var injector = RangeReadInjector();
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector), Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var keys = MarkerKeys();

        await writer.MarkMemoryEmbeddedAsync(RepoId, keys, Ct);

        var before = injector.Matched;
        var markers = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        var rangeReads = injector.Matched - before;

        Assert.Multiple(() =>
        {
            Assert.That(markers.Keys, Is.EquivalentTo(keys), "Every marker is read,");
            Assert.That(markers.Complete, Is.True, "the walk reached the end of the range,");
            Assert.That(markers.Fault, Is.Null, "with nothing to report,");
            Assert.That(rangeReads, Is.GreaterThan(1),
                "and it took several bounded range reads rather than one. The page size is the interval at "
                + "which progress is banked, so a walk that took a single read would have nothing to resume "
                + "from when the next one stalls.");
        });
    }

    [Test]
    public async Task A_stalled_marker_scan_banks_its_progress_resumes_and_then_starts_over()
    {
        // The whole lifecycle in one test, because each step is the precondition of
        // the next and the fault point is measured from the previous step rather
        // than guessed.
        var injector = RangeReadInjector();
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector), Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var keys = MarkerKeys();

        await writer.MarkMemoryEmbeddedAsync(RepoId, keys, Ct);

        // 1. Measure a healthy whole walk.
        var mark = injector.Matched;
        var calibration = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        var wholeWalkReads = injector.Matched - mark;
        Assert.That(calibration.Complete, Is.True, "Precondition: a healthy walk completes.");
        Assert.That(wholeWalkReads, Is.GreaterThan(1),
            "Precondition: a whole walk takes more than one range read, so it can stall part-way.");

        // 2. Stall it part-way: let roughly the first half of a walk's reads
        //    through, then fail every one after that.
        injector.FailAfterMatches = injector.Matched + (wholeWalkReads / 2);
        injector.FailFirst = int.MaxValue;
        var partial = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);

        // 3. A second attempt that cannot read a single page must still hold
        //    everything the first one banked.
        var retriedWhileStalled = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);

        // 4. The store recovers: the walk resumes rather than restarting.
        injector.FailFirst = 0;
        mark = injector.Matched;
        var resumed = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        var resumeReads = injector.Matched - mark;

        // 5. Once complete the cursor is dropped, so the pass after that walks the
        //    whole range again and sees markers added or disabled since.
        mark = injector.Matched;
        var fresh = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        var freshReads = injector.Matched - mark;

        Assert.Multiple(() =>
        {
            Assert.That(partial.Complete, Is.False, "The stalled walk reports itself incomplete,");
            Assert.That(partial.Fault, Is.Not.Null, "carrying the fault that stopped it,");
            Assert.That(partial.Keys, Is.Not.Empty,
                "and keeps the pages it did read rather than discarding them.");
            Assert.That(partial.Keys, Has.Count.LessThan(MarkerCount),
                "It is genuinely partial, so the assertions below are not vacuous.");

            Assert.That(retriedWhileStalled.Complete, Is.False);
            Assert.That(retriedWhileStalled.Keys, Is.EquivalentTo(partial.Keys),
                "A retry that reads nothing still returns the banked keys. Before the fix this came back "
                + "empty every time, which is exactly why the walk never finished.");

            Assert.That(resumed.Keys, Is.EquivalentTo(keys), "The resumed walk sees every marker,");
            Assert.That(resumed.Complete, Is.True, "and reports complete,");
            Assert.That(resumeReads, Is.LessThan(freshReads),
                "having cost less than a whole walk because it resumed from the banked continuation token "
                + "instead of re-reading the range from the start.");

            Assert.That(fresh.Keys, Is.EquivalentTo(keys),
                "and the pass after completion walks the whole range again rather than serving a stale "
                + "cursor, so a marker added or disabled since is still observed.");
        });
    }
}
