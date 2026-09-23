using NSubstitute;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for the order in which a pass runs its embedding arms: memory first, then
/// files, then symbols.
/// <para>
/// Memory is the smallest arm and the most valuable per vector, so running it first
/// makes captured decisions and gotchas searchable within the first pass of a fresh
/// onboard or a reset, rather than behind the whole file and symbol back-fill. Moving
/// it ahead of the file arm must not let a memory fault leak into the coverage
/// verdict the file arm measures (issue #3340).
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    [Test]
    public async Task The_embedding_arms_run_memory_then_files_then_symbols()
    {
        using var harness = new BootstrapHarness();
        harness.WriteFile("src/a.cs", "class A { }");

        await harness.Service.RunAsync(harness.Request(), progress: null);

        var armOrder = harness.VectorIngestor.ReceivedCalls()
            .Select(call => call.GetMethodInfo().Name)
            .Where(name => name is nameof(IRepoContextVectorIngestor.IngestMemoryAsync)
                or nameof(IRepoContextVectorIngestor.IngestAsync)
                or nameof(IRepoContextVectorIngestor.IngestSymbolsAsync))
            .ToList();

        Assert.That(
            armOrder,
            Is.EqualTo(new[]
            {
                nameof(IRepoContextVectorIngestor.IngestMemoryAsync),
                nameof(IRepoContextVectorIngestor.IngestAsync),
                nameof(IRepoContextVectorIngestor.IngestSymbolsAsync),
            }),
            "memory must embed first so it is searchable before the file and symbol back-fill completes");
    }

    [Test]
    public async Task A_memory_arm_fault_does_not_withdraw_the_coverage_verdict_the_file_arm_measured()
    {
        // The memory arm now runs ABOVE the gap-scan classification. Were its fault
        // banked straight into the pass's arm failure, the file arm's unmeasurable
        // verdict would be reclassified as an arm failure, withdrawing convergence
        // and re-arming the every-pass sweep on exactly the saturated store the
        // #3340 stand-down exists for.
        var options = BackOffOptions();
        Assert.That(
            options.PassesPerEmbeddingGapScan,
            Is.GreaterThan(3),
            "precondition: the periodic cadence must not come due within this test's three passes");

        // Pass 1 measures a gap, so pass 2 is guaranteed to arm the scan.
        using var harness = await ConvergedHarnessAsync(options, new RepoFileVectorIngestOutcome(0, 1, true));

        // Pass 2: the file arm measures an unmeasurable verdict, which backs the scan
        // off, while the memory arm ahead of it faults.
        harness.IngestOutcome = Unmeasurable;
        harness.MemoryIngestFault = new InvalidOperationException("memory embedder down");
        Assert.That(
            async () => await harness.Service.RunAsync(GapScanRequest(harness), progress: null),
            Throws.InstanceOf<InvalidOperationException>().And.Message.EqualTo("memory embedder down"),
            "a memory fault must still fail the run so it is re-driven");
        Assert.That(
            harness.CoverageVerdictReporter.Snapshot().Count(RepoContextCoverageVerdict.ArmFailure),
            Is.EqualTo(1),
            "the coverage_verdict charge below the last arm must still see the memory fault (#3354)");

        // Pass 3 is clean and reads whatever pass 2 carried forward.
        harness.MemoryIngestFault = null;
        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(
            harness.UnchangedOfferedToIngestor,
            Is.Empty,
            "a memory-arm fault says nothing about file coverage, so the file arm's unmeasurable verdict "
            + "must carry forward and keep the gap scan backed off");
    }
}
