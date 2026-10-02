using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for memory-only mode (<see cref="RepoContextIndexingOptions.SourceIndexing"/>
/// off): a pass must embed agent memory and touch nothing derived from source - no
/// walk, no structural, symbol, or content write, and no file or symbol embedding.
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    private static RepoContextIndexingOptions MemoryOnlyOptions() => new() { SourceIndexing = false };

    [Test]
    public async Task A_memory_only_pass_runs_the_memory_arm_and_no_source_arm()
    {
        using var harness = new BootstrapHarness(options: MemoryOnlyOptions());
        harness.WriteFile("src/a.cs", "class A { }");
        harness.MemoryIngestResult = 3;

        var result = await harness.Service.RunAsync(harness.Request(), harness.Progress);

        var arms = harness.VectorIngestor.ReceivedCalls()
            .Select(call => call.GetMethodInfo().Name)
            .Where(name => name is nameof(IRepoContextVectorIngestor.IngestMemoryAsync)
                or nameof(IRepoContextVectorIngestor.IngestAsync)
                or nameof(IRepoContextVectorIngestor.IngestSymbolsAsync))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(arms, Is.EqualTo(new[] { nameof(IRepoContextVectorIngestor.IngestMemoryAsync) }),
                "only the memory arm may run when source indexing is off");
            Assert.That(result.FilesScanned, Is.Zero, "a memory-only pass must not walk the tree");
            Assert.That(result.FilesAdded, Is.Zero);
            Assert.That(result.SymbolsCaptured, Is.Zero);
            Assert.That(harness.AtomicWrites, Is.Zero, "no structural, symbol, or content chunk may be committed");
            Assert.That(
                harness.Store.Keys.Where(key => key != RepoContextKeys.Repo(RepoId)),
                Is.Empty,
                "the repository marker is the only record a memory-only pass writes");
            Assert.That(
                harness.ProgressUpdates.Select(update => update.Phase),
                Does.Not.Contain(RepoIndexPhase.Walking).And.Contain(RepoIndexPhase.Vectorising));
        });

        harness.SymbolExtractor.DidNotReceiveWithAnyArgs().Extract(default!, default!, default!);
    }

    [Test]
    public async Task A_memory_only_pass_stamps_a_marker_with_no_files()
    {
        using var harness = new BootstrapHarness(options: MemoryOnlyOptions());
        harness.WriteFile("src/a.cs", "class A { }");

        await harness.Service.RunAsync(harness.Request(), progress: null);

        var marker = harness.ReadRepoNode();
        Assert.That(marker, Is.Not.Null, "the repository must stay listed so its memory is discoverable");
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(marker!.LastIngested), Is.Not.Null);
            Assert.That(RepoContextValues.ReadInt64(marker.FileCount), Is.Zero);
        });
    }

    [Test]
    public async Task A_memory_only_pass_does_not_resolve_or_require_the_repository_root()
    {
        // A git-sourced repository's root is a staging tree that memory-only mode never
        // fetches, so the pass must not depend on it existing.
        using var harness = new BootstrapHarness(options: MemoryOnlyOptions());
        var request = new RepoContextBootstrapRequest
        {
            RepoRoot = Path.Combine(harness.RepoRoot, "never-staged"),
            RepoId = RepoId,
        };

        var result = await harness.Service.RunAsync(request, progress: null);

        Assert.That(result.RepoId, Is.EqualTo(RepoId));
    }

    [Test]
    public void A_memory_only_pass_fails_when_the_memory_arm_faults_so_it_is_re_driven()
    {
        using var harness = new BootstrapHarness(options: MemoryOnlyOptions());
        harness.MemoryIngestFault = new InvalidOperationException("memory embedder down");

        Assert.That(
            async () => await harness.Service.RunAsync(harness.Request(), progress: null),
            Throws.InstanceOf<InvalidOperationException>().And.Message.EqualTo("memory embedder down"));
    }

    [Test]
    public async Task A_memory_only_pass_leaves_previously_indexed_source_records_untouched()
    {
        // The switch stops new indexing; dropping an existing code index is
        // repocontext_reset_index's job, so a stored file record must survive.
        using var harness = new BootstrapHarness(options: MemoryOnlyOptions());
        var fileKey = RepoContextKeys.File(RepoId, "gone.cs");
        lock (harness.Store)
        {
            harness.Store[fileKey] = [1, 2, 3];
        }

        await harness.Service.RunAsync(harness.Request(), progress: null);

        Assert.That(harness.Store.ContainsKey(fileKey), Is.True);
    }
}
