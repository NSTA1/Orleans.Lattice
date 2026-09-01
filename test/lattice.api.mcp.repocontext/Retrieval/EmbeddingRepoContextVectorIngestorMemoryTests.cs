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

    /// <summary>
    /// The write-side change signal. The ingestor gets no per-pass changed set for
    /// memory, so a revised entry would keep ranking on its pre-revision text
    /// forever if nothing invalidated it. `RepoContextStore` retires the vector on
    /// every write, which makes the entry look un-embedded so the ordinary
    /// back-fill re-embeds it from the current text.
    /// </summary>
    [Test]
    public async Task A_revised_entry_is_reembedded_with_its_new_text()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        await store.RememberAsync(
            RepoId, "gotchas", "revised", MemoryKind.Note,
            title: "First title", body: "ORIGINALBODYTOKEN", author: null, provenance: null,
            tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, cancellationToken: Ct);

        var key = RepoContextKeys.Memory(RepoId, "gotchas", "revised");
        var first = new FakeEmbeddingProvider();
        await Ingestor(harness, first).IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.True, "Precondition: embedded.");
        Assert.That(string.Join("\n", first.CapturedTexts), Does.Contain("ORIGINALBODYTOKEN"),
            "Precondition: the original text is what was embedded.");

        // Revise it. The write must retire the vector, or the next back-fill sees
        // a live membership and skips the entry.
        await store.RememberAsync(
            RepoId, "gotchas", "revised", MemoryKind.Note,
            title: "First title", body: "REVISEDBODYTOKEN", author: null, provenance: null,
            tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, cancellationToken: Ct);

        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.False,
            "The write retires the vector, so the entry looks un-embedded to the back-fill.");

        var second = new FakeEmbeddingProvider();
        var reembedded = await Ingestor(harness, second).IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        var passage = string.Join("\n", second.CapturedTexts);
        Assert.Multiple(() =>
        {
            Assert.That(reembedded, Is.EqualTo(1), "The revised entry is re-embedded by the back-fill.");
            Assert.That(passage, Does.Contain("REVISEDBODYTOKEN"),
                "The new body reached the embedder.");
            Assert.That(passage, Does.Not.Contain("ORIGINALBODYTOKEN"),
                "and the superseded text did not, so ranking cannot use stale content.");
        });
    }

    /// <summary>
    /// A hard forget must drop the vector, or the membership tally drifts high and
    /// the semantic path spends ranking slots on a key that no longer hydrates.
    /// </summary>
    [Test]
    public async Task Forgetting_an_entry_retires_its_vector()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        await store.RememberAsync(
            RepoId, "todo", "doomed", MemoryKind.Note,
            title: "T", body: "B", author: null, provenance: null,
            tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, cancellationToken: Ct);

        var key = RepoContextKeys.Memory(RepoId, "todo", "doomed");
        await Ingestor(harness, new FakeEmbeddingProvider()).IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.True, "Precondition: embedded.");

        await store.ForgetAsync(key, lapse: false, lapseSeconds: null, cancellationToken: Ct);

        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.False,
            "The forget retires the vector rather than leaving it to rank a dead key.");
    }

    /// <summary>
    /// A memory entry that expires by its own time-to-live is removed with nothing
    /// observing it, so its vector would outlive it forever. The sweep computes the
    /// orphan set as (recorded - live) and retires the difference. Without this,
    /// vectorising memory would leak an embedding per expired entry.
    /// </summary>
    [Test]
    public async Task An_expired_entrys_orphaned_vector_is_swept()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(harness, "todo", "expiring", "T", "B", Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.True, "Precondition: embedded.");

        // Model a time-to-live expiry: the record simply vanishes from the tree.
        // Nothing calls forget, so only the sweep can notice.
        await harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory).DeleteAsync(key, Ct);

        await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.False,
            "The sweep retires the vector of an entry that expired without an explicit forget.");
    }

    /// <summary>
    /// The memory-key markers share the membership tree with the source-id flags,
    /// so every membership enumeration must skip them. If one did not, the marker
    /// would be counted as an embedded source and inflate the user-visible
    /// <c>embeddedVectorCount</c> - a wrong number that looks entirely plausible.
    /// </summary>
    [Test]
    public async Task Memory_key_markers_do_not_inflate_the_embedded_count()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var key = await SeedMemoryAsync(harness, "gotchas", "counted", "T", "B", Ct);

        await Ingestor(harness, new FakeEmbeddingProvider())
            .IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        var count = await writer.CountEmbeddedAsync(RepoId, Ct);
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(1),
                "One memory entry is embedded, so the count is 1 - not 2 with its marker double-counted.");
            Assert.That(members, Has.Count.EqualTo(1),
                "and the member set holds only the real source id.");
            Assert.That(members, Does.Contain(VectorCodec.SourceId(key)),
                "which is the entry's source id, proving the count is the right 1 rather than the marker alone.");
        });
    }

    [Test]
    public async Task Updating_an_entry_retires_its_vector()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        await store.RememberAsync(
            RepoId, "gotchas", "patched", MemoryKind.Note,
            title: "T", body: "B", author: null, provenance: null,
            tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, cancellationToken: Ct);

        var key = RepoContextKeys.Memory(RepoId, "gotchas", "patched");
        await Ingestor(harness, new FakeEmbeddingProvider()).IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.True, "Precondition: embedded.");

        // A patch changes the passage text just as a remember does, so it must
        // invalidate too - otherwise the vector keeps ranking the pre-patch body.
        await store.UpdateAsync(
            key,
            new Dictionary<string, string> { ["body"] = "PATCHEDBODY" },
            addTags: null, removeTags: null, addLinks: null, removeLinks: null, cancellationToken: Ct);

        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.False,
            "repocontext_update retires the vector so the back-fill re-embeds the patched text.");
    }

    [Test]
    public async Task Lapsing_an_entry_retires_its_vector()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        await store.RememberAsync(
            RepoId, "todo", "lapsing", MemoryKind.Note,
            title: "T", body: "B", author: null, provenance: null,
            tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, cancellationToken: Ct);

        var key = RepoContextKeys.Memory(RepoId, "todo", "lapsing");
        await Ingestor(harness, new FakeEmbeddingProvider()).IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.True, "Precondition: embedded.");

        await store.ForgetAsync(key, lapse: true, lapseSeconds: 60L, cancellationToken: Ct);

        Assert.That(await IsMemoryEmbeddedAsync(harness, key, Ct), Is.False,
            "A lapse is a deliberate retirement, so the vector goes with it rather than "
            + "waiting for the entry to expire unobserved.");
    }

    [Test]
    public async Task The_embedded_memory_key_record_round_trips_and_clears()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var key = RepoContextKeys.Memory(RepoId, "gotchas", "tracked");

        var beforeAny = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        await writer.MarkMemoryEmbeddedAsync(RepoId, new[] { key }, Ct);
        var afterMark = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        await writer.UnmarkMemoryEmbeddedAsync(RepoId, key, Ct);
        var afterUnmark = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            // The empty case is asserted first and the positive case second, so a
            // fixture that never reached the store cannot make all three pass.
            Assert.That(beforeAny, Is.Empty, "Nothing is recorded before the first mark.");
            Assert.That(afterMark, Is.EquivalentTo(new[] { key }),
                "The mark records the entry's own record key, not its source id.");
            Assert.That(afterUnmark, Is.Empty, "and the unmark clears it.");
        });
    }

    /// <summary>
    /// The memory-key lookup must scan only the marker's own key range, never the
    /// whole membership prefix. The membership tree carries one record per embedded
    /// source - tens of thousands of files and symbols on a real repository - while
    /// the memory markers number in the hundreds, and this lookup runs on EVERY
    /// reconcile pass. Widening it would put a full-tree scan on the hot path, over
    /// the very tree whose cold-start replay is already the bottleneck.
    /// <para>
    /// This asserts the SCAN RANGE, not the returned keys. The lookup filters
    /// markers inside its loop, so its result is correct whether the scan is narrow
    /// or wide - a test over the result passes either way and guards nothing. Only
    /// the range distinguishes the two.
    /// </para>
    /// </summary>
    [Test]
    public void The_memory_key_scan_range_is_bounded_by_the_marker()
    {
        var prefix = RepoContextVectorWriter.MemoryKeysScanPrefix(RepoId);
        var membershipPrefix = RepoContextKeys.VectorMembershipsPrefix(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(prefix, Is.EqualTo(membershipPrefix + RepoContextVectorWriter.MemoryKeyMarkerPrefix),
                "The scan is narrowed to the marker's own range.");
            Assert.That(prefix, Is.Not.EqualTo(membershipPrefix),
                "and is strictly narrower than the whole membership prefix, which would be a full-tree scan.");
            Assert.That(prefix, Does.StartWith(membershipPrefix),
                "while still sitting inside the membership key space.");
        });
    }

    /// <summary>
    /// The result-level companion to the range assertion above: ordinary source-id
    /// membership must not surface as a memory key.
    /// </summary>
    [Test]
    public async Task Loading_memory_keys_ignores_ordinary_source_membership()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        // A file and a symbol source, recorded the ordinary way, plus one memory key.
        await writer.AddMembersAsync(
            RepoId,
            new[] { RepoContextKeys.File(RepoId, "src/Foo.cs"), RepoContextKeys.Symbol(RepoId, "Acme.Foo") },
            Ct);
        var memoryKey = RepoContextKeys.Memory(RepoId, "gotchas", "only-me");
        await writer.MarkMemoryEmbeddedAsync(RepoId, new[] { memoryKey }, Ct);

        var memoryKeys = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(memoryKeys, Is.EquivalentTo(new[] { memoryKey }),
                "Only the memory marker is returned - the file and symbol members are outside the scanned range.");
            Assert.That(members, Has.Count.EqualTo(2),
                "and the ordinary membership set still holds exactly the file and the symbol,");
            Assert.That(members, Does.Not.Contain(VectorCodec.SourceId(memoryKey)),
                "with the memory marker excluded from it rather than double-counted.");
        });
    }

    [Test]
    public async Task Marking_no_memory_keys_is_a_no_op()    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        await writer.MarkMemoryEmbeddedAsync(RepoId, Array.Empty<string>(), Ct);

        Assert.That(await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct), Is.Empty,
            "An empty batch writes nothing rather than creating a stray record.");
    }

    [Test]
    public async Task The_memory_key_writer_validates_its_arguments()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        Assert.Multiple(() =>
        {
            Assert.That(() => writer.MarkMemoryEmbeddedAsync(null!, Array.Empty<string>(), Ct),
                Throws.ArgumentNullException);
            Assert.That(() => writer.MarkMemoryEmbeddedAsync(RepoId, null!, Ct),
                Throws.ArgumentNullException);
            Assert.That(() => writer.UnmarkMemoryEmbeddedAsync(null!, "k", Ct),
                Throws.ArgumentNullException);
            Assert.That(() => writer.UnmarkMemoryEmbeddedAsync(RepoId, null!, Ct),
                Throws.ArgumentNullException);
            Assert.That(() => writer.LoadEmbeddedMemoryKeysAsync(null!, Ct),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_ingestor_validates_its_arguments()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        Assert.Multiple(() =>
        {
            Assert.That(() => ingestor.IngestMemoryAsync(null!, Array.Empty<string>(), Array.Empty<string>(), Ct),
                Throws.ArgumentNullException);
            Assert.That(() => ingestor.IngestMemoryAsync(RepoId, null!, Array.Empty<string>(), Ct),
                Throws.ArgumentNullException);
            Assert.That(() => ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), null!, Ct),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// The passage is built from whatever the entry actually carries. A title-less
    /// or tag-less entry must still embed - it is exactly the sparse note a session
    /// jots down and later needs to find - so the builder must not depend on the
    /// optional parts being present.
    /// </summary>
    [Test]
    public async Task An_entry_with_no_title_or_tags_still_embeds_its_topic_id_and_body()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedMemoryAsync(harness, "todo", "sparse", title: "", body: "SPARSEBODYTOKEN", Ct);
        var provider = new FakeEmbeddingProvider();

        var embedded = await Ingestor(harness, provider)
            .IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        var passage = string.Join("\n", provider.CapturedTexts);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "A sparse entry is still embedded.");
            Assert.That(passage, Does.Contain("todo/sparse"), "Its topic and id anchor the passage.");
            Assert.That(passage, Does.Contain("SPARSEBODYTOKEN"), "and its body is present.");
            Assert.That(passage, Does.Not.Contain("tags:"),
                "with no tags line emitted for an entry that carries none.");
        });
    }

    /// <summary>
    /// A memory body is prose and can be long, so it is chunked exactly as a file
    /// is rather than truncated to a single passage - a gotcha's operative detail
    /// (the fix, the corollaries) is as often at its end as its start.
    /// </summary>
    [Test]
    public async Task A_long_entry_is_chunked_so_its_tail_is_reachable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var body = "HEADTOKEN " + string.Join(" ", Enumerable.Repeat("filler prose sentence.", 900)) + " TAILTOKEN";
        var key = await SeedMemoryAsync(harness, "gotchas", "long", "A long gotcha", body, Ct);
        var provider = new FakeEmbeddingProvider();

        await Ingestor(harness, provider).IngestMemoryAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        var passage = string.Join("\n", provider.CapturedTexts);
        Assert.Multiple(() =>
        {
            Assert.That(provider.CapturedTexts, Has.Count.GreaterThan(1),
                "A long body yields several passages rather than one truncated window.");
            Assert.That(passage, Does.Contain("HEADTOKEN"), "The start of the body is embedded.");
            Assert.That(passage, Does.Contain("TAILTOKEN"),
                "and so is the end, which a single truncated passage would have dropped.");
        });
    }
}
