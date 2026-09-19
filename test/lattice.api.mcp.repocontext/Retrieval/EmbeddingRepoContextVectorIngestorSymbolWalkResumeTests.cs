using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the symbol arm's resumable range walk (issue #2953): a pass whose page
/// read faults banks the continuation token it reached, and the next pass resumes
/// from it rather than re-walking the range from the head.
/// <para>
/// <b>Why this is the defect and not merely an inefficiency.</b> The arm selects
/// work by paging the entire symbol range, and the re-drive is not a bystander to
/// the stall that caused it - it is the dominant source of the cold WAL replay
/// permit demand that keeps the leaves it re-reads cold. Scan-page issued leaf
/// reads were measured at roughly 98% of that demand on the deployed acceptance
/// container, against 1.6% for tombstone compaction and 0.16% for the WAL-GC
/// blocked-leaf sweep. A walk that restarts therefore regenerates exactly the load
/// that made it fault, and the cycle has no exit: the walk never completes a
/// circuit, so the arm never banks a snapshot, so the tree's WAL cursor floor never
/// advances and nothing is ever reclaimed.
/// </para>
/// <para>
/// <b>What is asserted, and why it is not the survey's proxy.</b> The observable is
/// the number of range pages the resuming pass actually reads, calibrated in the
/// same fixture against a clean full walk of the same corpus. Counting symbols
/// embedded would NOT separate the two designs - a restarting pass finds the
/// already-flagged symbols of the pages it re-reads and embeds the same number - so
/// the page reads are the behaviour and the embed count is the proxy that a correct
/// fix is engineered not to move.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorSymbolWalkResumeTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// Enough symbols to span several
    /// <see cref="RepoContextPortability.DefaultPageSize"/> pages, so a walk that
    /// resumes reads a strict suffix of the range and a walk that restarts reads all
    /// of it. One page would make the two designs indistinguishable.
    /// </summary>
    private const int SymbolCount = (RepoContextPortability.DefaultPageSize * 2) + 88;

    private const string PageMethod = "GetSortedEntriesBatchAsync";

    /// <summary>
    /// How many range pages <see cref="SymbolCount"/> symbols span. Each page opens
    /// exactly one entry cursor over the remaining range, so the cursor-open count
    /// is the page count - and it is also the call the re-drive is expensive in,
    /// because reopening a scan re-reads the range from its start.
    /// </summary>
    private const int ExpectedPages = 3;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, RepoContextSymbolWalkReporter reporter)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            new FakeEmbeddingProvider(),
            coverageProbeReporter: null,
            symbolWalkReporter: reporter);

    private static RepoContextMcpHarnessOptions Options(
        LatticeTreeFaultInjector injector, LatticeTreeCallCounter counter) => new()
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton(counter);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeCallCountingFilter>();
            },
        };

    private static async Task SeedSymbolsAsync(RepoContextMcpHarness harness, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        for (var i = 0; i < SymbolCount; i++)
        {
            var fqn = $"Acme.Generated.Type{i:D4}";
            var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Type };
            await tree.SetAsync(RepoContextKeys.Symbol(RepoId, fqn), serializer.SerializeToArray(record), ct);
        }
    }

    private static LatticeTreeFaultInjector PageInjector() => new()
    {
        TreeId = RepoContextTrees.Symbol,
        Method = PageMethod,
        IncludeShardGrains = true,
        FailFirst = 0,
    };

    /// <summary>
    /// The core guarantee. A pass that faults partway through the range banks its
    /// position; the next pass reads only the pages it had not reached, so the walk
    /// makes monotonic progress instead of paying for the same leaves again.
    /// <para>
    /// The clean full walk is measured first, in the same fixture and against the
    /// same corpus, so the comparison is calibrated rather than asserted against a
    /// hard-coded page count that a change to fan-out could invalidate silently.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_faulted_walk_resumes_from_its_banked_page_instead_of_re_reading_the_range()
    {
        var injector = PageInjector();
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.Symbol, IncludeShardGrains = true };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        await SeedSymbolsAsync(harness, Ct);

        using var reporter = new RepoContextSymbolWalkReporter();
        var ingestor = Ingestor(harness, reporter);

        // Calibration: what a walk of the WHOLE range costs in page reads, measured
        // rather than assumed.
        counter.Reset();
        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        var fullWalkPages = counter.Count(PageMethod);
        var afterClean = reporter.Snapshot();

        Assert.That(fullWalkPages, Is.GreaterThan(1),
            "Precondition: the corpus must span several pages, or a resumed walk and a restarted one "
            + "read the same thing and the fixture proves nothing.");

        var callsPerPage = fullWalkPages / ExpectedPages;
        Assert.That(fullWalkPages, Is.EqualTo(callsPerPage * ExpectedPages),
            $"Precondition: the {fullWalkPages} page-read call(s) of a full walk must divide evenly across "
            + $"the {ExpectedPages} range page(s) the corpus spans, or the fixture cannot let exactly one "
            + "page through before faulting.");

        // Let exactly one page through, then fault the rest of the range.
        injector.FailAfterMatches = injector.Matched + callsPerPage;
        injector.FailFirst = int.MaxValue;

        counter.Reset();
        Assert.That(
            async () => await ingestor.IngestSymbolsAsync(
                RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>(),
            "The arm still surfaces its fault: banking progress must not turn an incomplete pass into a "
            + "successful one, or the bootstrap run would stop re-driving it.");
        var afterFault = reporter.Snapshot();

        // The plane recovers.
        injector.FailFirst = injector.Failed;

        counter.Reset();
        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        var resumedPages = counter.Count(PageMethod);
        var afterResume = reporter.Snapshot();

        TestContext.Out.WriteLine(
            $"full walk = {fullWalkPages} page read(s); resuming pass = {resumedPages} page read(s).");

        Assert.Multiple(() =>
        {
            Assert.That(afterClean.Complete, Is.EqualTo(1),
                "A walk that never had to bank is reported as complete, not resumed.");
            Assert.That(afterFault.Banked, Is.EqualTo(1),
                "The faulted pass banked its position - without this arm a walk that silently restarted "
                + "would be indistinguishable from one that resumed (issue #2938).");
            Assert.That(afterResume.Resumed, Is.EqualTo(1),
                "and the pass that followed consumed that banked progress and finished the circuit.");
            Assert.That(resumedPages, Is.LessThan(fullWalkPages),
                "The point of the fix: the resuming pass reads a strict SUFFIX of the range. Before it, "
                + $"the pass restarted at the head and re-issued all {fullWalkPages} page read(s) - which "
                + "is itself the cold-replay permit demand that keeps those leaves cold (issue #2953).");
        });
    }

    /// <summary>
    /// The cursor is an optimisation over a range that is walked again once
    /// exhausted, not a permanent position. A completed circuit must drop it, or a
    /// symbol captured after the walk passed its key would never be reached.
    /// </summary>
    [Test]
    public async Task A_completed_circuit_drops_the_cursor_so_the_next_pass_walks_the_whole_range()
    {
        var injector = PageInjector();
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.Symbol, IncludeShardGrains = true };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        await SeedSymbolsAsync(harness, Ct);

        using var reporter = new RepoContextSymbolWalkReporter();
        var ingestor = Ingestor(harness, reporter);

        counter.Reset();
        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        var callsPerPage = counter.Count(PageMethod) / ExpectedPages;

        injector.FailAfterMatches = injector.Matched + callsPerPage;
        injector.FailFirst = int.MaxValue;
        Assert.That(
            async () => await ingestor.IngestSymbolsAsync(
                RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>(),
            "Precondition: a pass banked its position.");

        injector.FailFirst = injector.Failed;

        // Consumes the banked progress and exhausts the range.
        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        counter.Reset();
        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        var afterCircuit = reporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(afterCircuit.Resumed, Is.EqualTo(1),
                "Only the one pass that actually consumed banked progress is reported as resumed,");
            Assert.That(afterCircuit.Complete, Is.EqualTo(2),
                "and the pass after the circuit closed starts fresh, which is what lets it observe "
                + "symbols captured while the cursor was mid-range.");
        });
    }

    /// <summary>
    /// Cancellation is not a page fault. A host shutting down has established
    /// nothing about whether the range is readable, so recording it as banked
    /// progress would make an orderly stop scrape as a degraded walk - and would
    /// leave a cursor behind that no fault justified.
    /// </summary>
    [Test]
    public async Task A_cancelled_pass_banks_nothing_and_is_not_reported_as_a_degraded_walk()
    {
        var injector = PageInjector();
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.Symbol, IncludeShardGrains = true };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        await SeedSymbolsAsync(harness, Ct);

        using var reporter = new RepoContextSymbolWalkReporter();
        var ingestor = Ingestor(harness, reporter);

        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await cancelled.CancelAsync();

        Assert.That(
            async () => await ingestor.IngestSymbolsAsync(
                RepoId, Array.Empty<string>(), Array.Empty<string>(), cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "Precondition: the pass was cancelled rather than faulted.");

        var snapshot = reporter.Snapshot();
        Assert.That(
            snapshot,
            Is.EqualTo(new RepoContextSymbolWalkSnapshot(Complete: 0, Resumed: 0, Banked: 0)),
            "No arm advances for a cancelled pass: it is neither a completed walk nor a degraded one, "
            + "and counting it as banked would report a shutdown as a plane fault.");
    }
}
