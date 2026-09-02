using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the <b>shape</b> of every presence-marking write
/// <see cref="RepoContextVectorWriter"/> makes on the membership tree:
/// <see cref="RepoContextVectorWriter.AddMembersAsync"/>,
/// <see cref="RepoContextVectorWriter.MarkMemoryEmbeddedAsync"/>, and
/// <see cref="RepoContextVectorWriter.MarkContentlessAsync"/> must each land a
/// whole batch in <b>one</b> batched CRDT apply rather than one apply per key.
/// <para>
/// This is a call-count assertion because it cannot be anything else: a per-key
/// loop and a batched write converge to identical stored state, so only the number
/// of grain calls distinguishes them, and a wall-clock assertion would be flaky.
/// The regression it guards is not latency but a livelock - a per-key chain times
/// out partway, the markers it never wrote leave their sources looking un-embedded,
/// and the next reconcile re-embeds exactly the same passages, forever.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextVectorWriterBatchedMembershipTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static string SourceId(string sourceKey) => VectorCodec.SourceId(sourceKey);

    /// <summary>
    /// A writer-posture harness whose silo counts every <see cref="ILattice"/> call
    /// into the membership tree, so a test can assert how many round trips a write
    /// actually cost.
    /// </summary>
    private static RepoContextMcpHarnessOptions CountingOptions() => new()
    {
        Posture = RepoContextMcpAuthPosture.Writer,
        ConfigureSilo = silo =>
        {
            silo.Services.AddSingleton(
                new LatticeTreeCallCounter { TreeId = RepoContextTrees.VectorMembership });
            silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeCallCountingFilter>();
        },
    };

    private static (RepoContextVectorWriter Writer, LatticeTreeCallCounter Counter) Resolve(
        RepoContextMcpHarness harness)
        => (harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.Services.GetRequiredService<LatticeTreeCallCounter>());

    [Test]
    public async Task AddMembersAsync_lands_a_whole_batch_in_one_batched_apply()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);

        var sources = Enumerable.Range(0, 12)
            .Select(i => RepoContextKeys.File(RepoId, $"src/F{i}.cs"))
            .ToArray();

        counter.Reset();
        await writer.AddMembersAsync(RepoId, sources, Ct);
        var batchedApplies = counter.Count("ApplyCrdtDeltaManyAsync");
        var perKeyApplies = counter.Count("ApplyCrdtDeltaAsync");
        var reads = counter.Count("GetManyAsync");

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(batchedApplies, Is.EqualTo(1),
                "A 12-source batch costs exactly one batched apply, whatever its size.");
            Assert.That(perKeyApplies, Is.Zero,
                "No single-key apply survives: that per-key chain is what timed out and livelocked the loop.");
            Assert.That(reads, Is.EqualTo(1),
                "The enable deltas are minted from one batched read, not one read per key.");
            Assert.That(sources.Select(SourceId), Is.SubsetOf(members),
                "Batching changes the number of round trips, never the recorded membership.");
        });
    }

    [Test]
    public async Task AddMembersAsync_records_every_source_when_a_batch_names_one_twice()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);

        var repeated = RepoContextKeys.File(RepoId, "src/A.cs");
        var other = RepoContextKeys.File(RepoId, "src/B.cs");

        counter.Reset();
        await writer.AddMembersAsync(RepoId, new[] { repeated, other, repeated }, Ct);
        var batchedApplies = counter.Count("ApplyCrdtDeltaManyAsync");

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(batchedApplies, Is.EqualTo(1),
                "A repeated source is de-duplicated into the same single batch, never split across two.");
            Assert.That(members.Contains(SourceId(repeated)), Is.True);
            Assert.That(members.Contains(SourceId(other)), Is.True);
            Assert.That(members.Count, Is.EqualTo(2),
                "De-duplication is on the key, so a source named twice is still one member.");
        });
    }

    [Test]
    public async Task MarkContentlessAsync_lands_a_whole_batch_in_one_batched_apply()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);

        var sources = Enumerable.Range(0, 8)
            .Select(i => RepoContextKeys.File(RepoId, $"src/empty{i}.cs"))
            .ToArray();

        counter.Reset();
        await writer.MarkContentlessAsync(RepoId, sources, Ct);
        var batchedApplies = counter.Count("ApplyCrdtDeltaManyAsync");
        var perKeyApplies = counter.Count("ApplyCrdtDeltaAsync");

        var coverage = await writer.ProbeCoverageAsync(RepoId, sources, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(batchedApplies, Is.EqualTo(1),
                "The contentless markers land in one batched apply for the whole batch.");
            Assert.That(perKeyApplies, Is.Zero);
            Assert.That(sources.Select(SourceId), Is.SubsetOf(coverage.Contentless),
                "Every marked source is still reported contentless, with the marker prefix stripped.");
        });
    }

    [Test]
    public async Task MarkMemoryEmbeddedAsync_lands_a_whole_batch_in_one_batched_apply()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);

        var memoryKeys = Enumerable.Range(0, 6)
            .Select(i => RepoContextKeys.Memory(RepoId, "gotchas", $"note-{i}"))
            .ToArray();

        counter.Reset();
        await writer.MarkMemoryEmbeddedAsync(RepoId, memoryKeys, Ct);
        var batchedApplies = counter.Count("ApplyCrdtDeltaManyAsync");
        var perKeyApplies = counter.Count("ApplyCrdtDeltaAsync");

        var recorded = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(batchedApplies, Is.EqualTo(1),
                "The memory-key markers land in one batched apply, so a batch of entries is embedded once.");
            Assert.That(perKeyApplies, Is.Zero);
            Assert.That(memoryKeys, Is.SubsetOf(recorded),
                "Each marked entry is still recorded under its own key, prefix stripped on read.");
        });
    }

    [Test]
    public async Task An_empty_batch_writes_nothing_at_all()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);

        counter.Reset();
        await writer.AddMembersAsync(RepoId, Array.Empty<string>(), Ct);
        await writer.MarkContentlessAsync(RepoId, Array.Empty<string>(), Ct);
        await writer.MarkMemoryEmbeddedAsync(RepoId, Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(counter.Count("ApplyCrdtDeltaManyAsync"), Is.Zero,
                "An empty batch short-circuits before the tree is touched at all.");
            Assert.That(counter.Count("GetManyAsync"), Is.Zero,
                "It costs no read either, so an empty pass is genuinely free.");
        });
    }
}
