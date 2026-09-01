using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for durable agent-memory vectorisation in
/// <see cref="EmbeddingRepoContextVectorIngestor.IngestMemoryAsync"/> against a live
/// in-memory Lattice cluster and a deterministic <see cref="FakeEmbeddingProvider"/>.
/// <para>
/// These pin the fix for issue #1878, where only files and symbols were ever
/// embedded so a healthy semantic index could not return a captured decision,
/// gotcha or convention at all - memory was reachable only through the degraded
/// keyword path. The observed cost was not merely a failed lookup: a session that
/// searched, found nothing, and concluded the entry had never been written would
/// then write it again, so the store accumulated duplicate captures.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (the memory and
/// reserved vector trees) via <see cref="RepoContextMcpHarness"/>, so it is excluded
/// from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorMemoryTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, IEmbeddingProvider? provider)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            provider);

    /// <summary>
    /// Seeds one memory entry through the real MvRegister envelope encoding, which
    /// is how the store actually persists memory - a bare serialized MemoryRecord
    /// would be the wrong shape and would let a decode bug pass unnoticed.
    /// </summary>
    private static async Task<string> SeedMemoryAsync(
        RepoContextMcpHarness harness,
        string topic,
        string id,
        string title,
        string body,
        CancellationToken ct,
        params string[] tags)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var clock = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 0 };

        var tagSet = new OrSet();
        for (var i = 0; i < tags.Length; i++)
        {
            tagSet.Add(System.Text.Encoding.UTF8.GetBytes(tags[i]), "A", i);
        }

        var record = new MemoryRecord
        {
            RepoId = RepoId,
            Topic = topic,
            Id = id,
            Kind = MemoryKind.Note,
            Title = RepoContextValues.Lww(title, clock),
            Body = RepoContextValues.Lww(body, clock),
            Tags = tagSet,
        };

        var key = RepoContextKeys.Memory(RepoId, topic, id);
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);
        await tree.SetAsync(key, MemoryRegisterTestEncoding.EncodeSingle(serializer, "A", record), ct);
        return key;
    }

    private static async Task<bool> IsMemoryEmbeddedAsync(
        RepoContextMcpHarness harness, string memoryKey, CancellationToken ct)
    {
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, ct);
        return members.Contains(VectorCodec.SourceId(memoryKey));
    }

    [Test]
    public async Task IngestMemoryAsync_embeds_a_memory_entry_and_records_membership()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(
            harness, "gotchas", "g1", "A negative assertion passes vacuously", "Pair it with a positive one.", Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        var embedded = await ingestor.IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        var isEmbedded = await IsMemoryEmbeddedAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "The one memory entry is embedded.");
            Assert.That(isEmbedded, Is.True,
                "The entry's source identifier is a live member after embedding.");
        });
    }

    /// <summary>
    /// The regression guard for the MvRegister envelope. Memory is persisted as an
    /// MvRegister blob whose concurrent values are serialized MemoryRecords, so the
    /// ingestor must FOLD it exactly as the projection does. Deserializing the
    /// envelope directly as a MemoryRecord reads the wrong shape and yields a
    /// passage with none of the entry's real text - which still "embeds something"
    /// and still reports a non-zero count, so only asserting on the passage content
    /// distinguishes a correct decode from a plausible-looking wrong one.
    /// </summary>
    [Test]
    public async Task IngestMemoryAsync_embeds_the_folded_entrys_text_not_the_raw_envelope()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(
            harness,
            "conventions",
            "c1",
            "Memory is the cross-session channel of record",
            "A ruling announced only in chat is not a recorded decision.",
            Ct,
            "coordination",
            "memory");
        var provider = new FakeEmbeddingProvider();

        await Ingestor(harness, provider).IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        var passage = string.Join("\n", provider.CapturedTexts);
        Assert.Multiple(() =>
        {
            Assert.That(provider.CapturedTexts, Is.Not.Empty, "Something reached the embedder at all.");
            Assert.That(passage, Does.Contain("Memory is the cross-session channel of record"),
                "The folded entry's title is in the embedded passage.");
            Assert.That(passage, Does.Contain("A ruling announced only in chat"),
                "The folded entry's body is in the embedded passage.");
            Assert.That(passage, Does.Contain("conventions/c1"),
                "The topic and id are in the passage, so a topic-shaped query can reach it.");
            Assert.That(passage, Does.Contain("coordination"),
                "The entry's tags are in the passage.");
        });
    }

    [Test]
    public async Task IngestMemoryAsync_backfills_an_entry_with_no_live_embedding()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(
            harness, "decisions", "d1", "Embed memory", "Only files and symbols were embedded.", Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        // No changed and no retired keys. This is the mechanism that converts an
        // existing store - every entry in it was captured before memory embedding
        // existed - without a re-walk, so it is the path that actually fixes #1878
        // for a deployment that already holds hundreds of entries.
        var embedded = await ingestor.IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        var isEmbedded = await IsMemoryEmbeddedAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "An entry with no live embedding is back-filled.");
            Assert.That(isEmbedded, Is.True);
        });
    }

    [Test]
    public async Task IngestMemoryAsync_skips_an_already_embedded_entry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(harness, "gotchas", "g2", "Title", "Body", Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        await ingestor.IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);
        var second = await ingestor.IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.That(second, Is.EqualTo(0),
            "An already-embedded entry is not re-embedded on a later reconcile, so the "
            + "always-on sweep does not re-drive the embedder over the whole memory store.");
    }

    [Test]
    public async Task IngestMemoryAsync_retires_a_forgotten_entrys_embedding()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(harness, "todo", "t1", "Title", "Body", Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        await ingestor.IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);
        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.True, "Precondition: embedded.");

        // Model a forget: the record leaves the memory tree before the ingestor
        // retires its vector, so the back-fill pass does not resurrect it.
        await harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory).DeleteAsync(key, Ct);
        await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), new[] { key }, Ct);

        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.False,
            "A forgotten entry's embedding is retired so the membership tally stays honest.");
    }

    [Test]
    public async Task IngestMemoryAsync_retires_a_forgotten_entry_even_when_the_provider_is_unavailable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(harness, "todo", "t2", "Title", "Body", Ct);

        await Ingestor(harness, new FakeEmbeddingProvider())
            .IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        var down = new FakeEmbeddingProvider { Available = false };
        var embedded = await Ingestor(harness, down)
            .IngestMemoryAsync(RepoId, Array.Empty<string>(), new[] { key }, Ct);

        var isEmbedded = await IsMemoryEmbeddedAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(0), "Nothing is embedded while the provider is down.");
            Assert.That(isEmbedded, Is.False,
                "Retirement only deletes stored records, so it runs without an embedder.");
        });
    }

    [Test]
    public async Task IngestMemoryAsync_embeds_nothing_when_no_provider_is_bound()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(harness, "gotchas", "g3", "Title", "Body", Ct);

        var embedded = await Ingestor(harness, provider: null)
            .IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(0), "No provider bound means nothing is embedded.");
            Assert.That(IsMemoryEmbeddedAsync(harness, key, Ct).GetAwaiter().GetResult(), Is.False,
                "and no membership is recorded, so a later pass still back-fills the entry.");
        });
    }
}
