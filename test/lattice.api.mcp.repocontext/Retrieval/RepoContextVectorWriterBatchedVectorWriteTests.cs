using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the <b>shape</b> of the durable vector write
/// <see cref="RepoContextVectorWriter.StoreAsync"/> makes: a whole batch of passage
/// vectors must land in <b>one</b> batched set on the payload tree and <b>one</b> on
/// the metadata tree, never one write per vector.
/// <para>
/// This is the same regression, on the same writer, that
/// <see cref="RepoContextVectorWriterBatchedMembershipTests"/> already guards on the
/// membership tree - the per-key alternative costs a sequential grain round trip
/// <i>per vector</i>. The membership tree was batched; the payload and metadata trees
/// were not. The regression is invisible to a state assertion because a per-key loop
/// and a batched write converge to identical stored state. Only a call count tells
/// them apart, and a wall-clock assertion would be flaky.
/// </para>
/// <para>
/// <b>What this does not guard.</b> The counts here are grain calls, not
/// write-ahead-log appends. Vector keys are hashes, so a batch scatters across shards
/// and each leaf still appends about one entry: a container A/B at a matched 67
/// embedded files measured 1.0 entries per append on both sides and an unchanged log
/// size. Do not read a green run of this fixture as evidence of reduced log growth.
/// </para>
/// <para>
/// The presence probe on the payload tree is guarded here too, and deliberately.
/// Payloads are content-addressed and immutable, so re-writing a payload that is
/// already stored appends to the log for no change at all. A well-meaning
/// simplification that dropped the probe and wrote the batch unconditionally would
/// pass every state assertion in this repository while appending every already-stored
/// payload to the log again on each re-embed.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextVectorWriterBatchedVectorWriteTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpace Space = new("test-model", 4, normalized: true);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// A writer-posture harness counting <see cref="ILattice"/> calls into <b>every</b>
    /// tree, so one test can assert the payload and metadata halves of the same write
    /// without standing up two counters.
    /// </summary>
    private static RepoContextMcpHarnessOptions CountingOptions() => new()
    {
        Posture = RepoContextMcpAuthPosture.Writer,
        ConfigureSilo = silo =>
        {
            silo.Services.AddSingleton(new LatticeTreeCallCounter());
            silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeCallCountingFilter>();
        },
    };

    private static (RepoContextVectorWriter Writer, LatticeTreeCallCounter Counter) Resolve(
        RepoContextMcpHarness harness)
        => (harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.Services.GetRequiredService<LatticeTreeCallCounter>());

    private static ReadOnlyMemory<float>[] DistinctVectors(int count)
        => Enumerable.Range(0, count)
            .Select(i => new ReadOnlyMemory<float>([i + 1f, 0f, 0f, 0f]))
            .ToArray();

    private static string PayloadTree => RepoContextTrees.VectorPayload;

    private static string MetadataTree => RepoContextTrees.VectorMetadata;

    [Test]
    public async Task Storing_a_batch_costs_one_batched_set_per_tree_and_no_per_key_set()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");

        counter.Reset();
        await writer.StoreAsync(RepoId, sourceKey, Space, DistinctVectors(12), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(counter.KeyCountForTree(PayloadTree, "SetManyAsync"), Is.EqualTo(12),
                $"All 12 payloads land in batched sets. Payload tree: {counter.DescribeTree(PayloadTree)}");
            Assert.That(counter.KeyCountForTree(PayloadTree, "SetAsync"), Is.Zero,
                "No single-key payload write survives: that is the per-vector round trip.");
            Assert.That(counter.KeyCountForTree(MetadataTree, "SetManyAsync"), Is.EqualTo(12),
                $"All 12 metadata records land in batched sets. Metadata tree: {counter.DescribeTree(MetadataTree)}");
            Assert.That(counter.KeyCountForTree(MetadataTree, "SetAsync"), Is.Zero,
                "No single-key metadata write survives either.");
            Assert.That(counter.KeyCountForTree(MetadataTree, "GetAsync"), Is.Zero,
                "The read-merge-write chain reads once for the batch, not once per vector.");
        });
    }

    [Test]
    public async Task Re_storing_an_unchanged_source_writes_no_payload_at_all()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");
        var vectors = DistinctVectors(8);

        await writer.StoreAsync(RepoId, sourceKey, Space, vectors, Ct);

        counter.Reset();
        await writer.StoreAsync(RepoId, sourceKey, Space, vectors, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(counter.KeyCountForTree(PayloadTree, "SetManyAsync"), Is.Zero,
                "Payloads are content-addressed and immutable, so an unchanged source "
                + $"re-writes none of them. Payload tree: {counter.DescribeTree(PayloadTree)}");
            Assert.That(counter.KeyCountForTree(PayloadTree, "SetAsync"), Is.Zero,
                "And it certainly writes none one at a time.");
            Assert.That(counter.CountForTree(PayloadTree), Is.GreaterThan(0),
                "The probe itself still runs - this asserts the write was skipped, "
                + "not that the tree was never consulted.");
        });
    }

    [Test]
    public async Task A_batch_repeating_one_passage_writes_that_payload_once()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");

        // Two units sharing one content address: a real corpus does this whenever a
        // file's overlapping windows cover identical text.
        ReadOnlyMemory<float> repeated = new([1f, 0f, 0f, 0f]);
        ReadOnlyMemory<float> other = new([0f, 1f, 0f, 0f]);

        counter.Reset();
        await writer.StoreAsync(RepoId, sourceKey, Space, [repeated, other, repeated], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(counter.KeyCountForTree(PayloadTree, "SetManyAsync"), Is.EqualTo(2),
                "The repeated passage is de-duplicated by content address, so two distinct "
                + $"payloads are written, not three. Payload tree: {counter.DescribeTree(PayloadTree)}");
            Assert.That(counter.KeyCountForTree(MetadataTree, "SetManyAsync"), Is.EqualTo(3),
                "Metadata is keyed per unit, so all three units are still recorded - "
                + "de-duplication is on the payload, never on the source's passages.");
        });
    }

    [Test]
    public async Task Batching_changes_the_round_trips_and_never_the_stored_state()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, _) = Resolve(harness);
        var grains = harness.Services.GetRequiredService<IGrainFactory>();
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");
        var vectors = DistinctVectors(5);

        await writer.StoreAsync(RepoId, sourceKey, Space, vectors, Ct);

        var metadata = grains.GetGrain<ILattice>(MetadataTree);
        var payload = grains.GetGrain<ILattice>(PayloadTree);
        var sourceId = VectorCodec.SourceId(sourceKey);

        var storedMetadata = new List<bool>();
        var storedPayloads = new List<bool>();
        for (var unit = 0; unit < vectors.Length; unit++)
        {
            var encoded = VectorCodec.Encode(vectors[unit]);
            var contentAddress = VectorCodec.ContentAddress(encoded);
            var vectorId = RepoContextVectorWriter.FormatVectorId(sourceId, unit, contentAddress);

            storedMetadata.Add(
                await metadata.ExistsAsync(RepoContextKeys.Vector(RepoId, vectorId), Ct));
            storedPayloads.Add(
                await payload.ExistsAsync(RepoContextKeys.VectorPayload(RepoId, contentAddress), Ct));
        }

        Assert.Multiple(() =>
        {
            Assert.That(storedMetadata, Is.All.True,
                "Every unit still has its metadata presence key under the same derived id.");
            Assert.That(storedPayloads, Is.All.True,
                "And every passage's payload is still stored under its content address.");
        });
    }

    [Test]
    public async Task An_empty_batch_writes_neither_payload_nor_metadata()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(CountingOptions(), Ct);
        var (writer, counter) = Resolve(harness);

        counter.Reset();
        await writer.StoreAsync(
            RepoId, RepoContextKeys.File(RepoId, "src/Empty.cs"), Space, [], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(counter.CountForTree(PayloadTree), Is.Zero,
                "An empty batch short-circuits before the payload tree is touched at all.");
            Assert.That(counter.KeyCountForTree(MetadataTree, "SetManyAsync"), Is.Zero,
                "And it writes no metadata either - it only retires what the source used to have.");
        });
    }
}
