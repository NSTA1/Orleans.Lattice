using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for symbol vectorisation in
/// <see cref="EmbeddingRepoContextVectorIngestor.IngestSymbolsAsync"/> against a
/// live in-memory Lattice cluster and a deterministic
/// <see cref="FakeEmbeddingProvider"/>. They pin the contract: a changed symbol is
/// embedded, a symbol with no live embedding is back-filled, an already-embedded
/// unchanged symbol is skipped, and a pruned symbol's embedding is retired - even
/// when the embedding provider is unavailable.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (the symbol and
/// reserved vector trees) via <see cref="RepoContextMcpHarness"/>, so it is excluded
/// from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorSymbolTests
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

    private static async Task<string> SeedSymbolAsync(
        RepoContextMcpHarness harness, string fqn, SymbolKind kind, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = kind };
        var key = RepoContextKeys.Symbol(RepoId, fqn);
        await tree.SetAsync(key, serializer.SerializeToArray(record), ct);
        return key;
    }

    private static async Task<bool> IsSymbolEmbeddedAsync(
        RepoContextMcpHarness harness, string symbolKey, CancellationToken ct)
    {
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, ct);
        return members.Contains(Encoding.UTF8.GetBytes(VectorCodec.SourceId(symbolKey)));
    }

    [Test]
    public async Task IngestSymbolsAsync_embeds_a_changed_symbol_and_records_membership()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedSymbolAsync(harness, "Acme.Foo.Bar", SymbolKind.Method, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        var embedded = await ingestor.IngestSymbolsAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        var isEmbedded = await IsSymbolEmbeddedAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "The one changed symbol is embedded.");
            Assert.That(isEmbedded, Is.True,
                "The symbol's source identifier is a live member after embedding.");
        });
    }

    [Test]
    public async Task IngestSymbolsAsync_backfills_a_symbol_with_no_live_embedding()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedSymbolAsync(harness, "Acme.Foo.Baz", SymbolKind.Type, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        // No changed and no pruned keys: the symbol was captured before symbol
        // embedding existed, so it is back-filled purely because it has no vector.
        var embedded = await ingestor.IngestSymbolsAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        var isEmbedded = await IsSymbolEmbeddedAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "A symbol with no live embedding is back-filled.");
            Assert.That(isEmbedded, Is.True);
        });
    }

    [Test]
    public async Task IngestSymbolsAsync_skips_an_already_embedded_unchanged_symbol()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedSymbolAsync(harness, "Acme.Foo.Qux", SymbolKind.Method, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        await ingestor.IngestSymbolsAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);
        var second = await ingestor.IngestSymbolsAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.That(second, Is.EqualTo(0),
            "An already-embedded, unchanged symbol is not re-embedded on a later pass.");
    }

    [Test]
    public async Task IngestSymbolsAsync_retires_a_pruned_symbols_embedding()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedSymbolAsync(harness, "Acme.Foo.Gone", SymbolKind.Method, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        await ingestor.IngestSymbolsAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);
        Assert.That(await IsSymbolEmbeddedAsync(harness, key, Ct), Is.True, "Precondition: embedded.");

        // Model the reconcile: a pruned symbol's record is deleted from the symbol
        // tree before the ingestor retires its embedding, so the back-fill pass does
        // not re-embed it.
        await harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol).DeleteAsync(key, Ct);
        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), new[] { key }, Ct);

        Assert.That(await IsSymbolEmbeddedAsync(harness, key, Ct), Is.False,
            "A pruned symbol's embedding is retired so the membership tally stays honest.");
    }

    [Test]
    public async Task IngestSymbolsAsync_retires_a_pruned_symbol_even_when_the_provider_is_unavailable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedSymbolAsync(harness, "Acme.Foo.Doomed", SymbolKind.Method, Ct);

        // Embed with a healthy provider, then prune with an unavailable one: retirement
        // only deletes stored records, so it must run regardless of the embedder.
        await Ingestor(harness, new FakeEmbeddingProvider())
            .IngestSymbolsAsync(RepoId, new[] { key }, Array.Empty<string>(), Ct);

        var down = new FakeEmbeddingProvider { Available = false };
        var embedded = await Ingestor(harness, down)
            .IngestSymbolsAsync(RepoId, Array.Empty<string>(), new[] { key }, Ct);

        var isEmbedded = await IsSymbolEmbeddedAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(0), "Nothing is embedded while the provider is down.");
            Assert.That(isEmbedded, Is.False,
                "The pruned symbol is still retired even though the embedder is unavailable.");
        });
    }
}
