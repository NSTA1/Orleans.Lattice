using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for memory-link staleness detection on
/// <see cref="RepoContextStore"/>. When a memory entry links to a structural
/// target, the store captures the target's content digest at link time; a later
/// evaluating recall (and each neighbor of a knowledge-graph walk) compares that
/// captured digest against the target's current digest and reports drift through
/// <see cref="RepoContextEntryView.Stale"/> and
/// <see cref="RepoContextEntryView.StaleLinks"/>. A non-evaluating recall leaves
/// those fields <see langword="null"/> ("not evaluated"), mirroring the expiry
/// convention.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev
/// loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreStalenessTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static ILattice Tree(RepoContextMcpHarness harness, string treeName)
        => harness.GrainFactory.GetGrain<ILattice>(treeName);

    private static async Task SeedFileAsync(
        RepoContextMcpHarness harness, string repoId, string path, string digest, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<FileNode>>();
        var clock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var node = new FileNode { RepoId = repoId, Path = path, Digest = RepoContextValues.Lww(digest, clock) };
        await Tree(harness, RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.File(repoId, path), serializer.SerializeToArray(node), ct);
    }

    private static async Task<string> RememberLinkedAsync(
        RepoContextStore store, string repoId, string fileKey, CancellationToken ct)
    {
        var result = await store.RememberAsync(
            repoId, "glossary", id: null, MemoryKind.Note, title: "linked", body: null,
            author: null, provenance: null, tags: null,
            addLinks: new Dictionary<string, IReadOnlyList<string>> { ["related"] = new[] { fileKey } },
            removeLinks: null, ttlSeconds: null, ct);
        return result.Key;
    }

    [Test]
    public async Task Recall_reports_a_link_as_fresh_when_the_target_digest_is_unchanged()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);
        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.False, "The captured digest still matches the target.");
            Assert.That(view.StaleLinks, Is.Null.Or.Empty);
        });
    }

    [Test]
    public async Task Recall_flags_a_link_as_stale_after_the_target_digest_drifts()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // The target file changes on disk: its content digest is re-projected.
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True, "The target digest drifted since the link was made.");
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
        });
    }

    [Test]
    public async Task Recall_flags_a_link_as_stale_when_the_target_is_deleted()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // The target file is removed entirely: it no longer carries any digest.
        await Tree(harness, RepoContextTrees.Structural).DeleteAsync(fileKey, Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True, "A vanished target is drift.");
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
        });
    }

    [Test]
    public async Task Non_evaluating_recall_leaves_staleness_unevaluated()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.Null, "A bulk-convention recall does not evaluate staleness.");
            Assert.That(view.StaleLinks, Is.Null);
        });
    }

    [Test]
    public async Task Recall_ignores_the_captured_digest_of_an_unlinked_target()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // Remove the edge, then drift the target: with no live link, the lingering
        // captured digest must not produce a phantom stale flag.
        await store.UpdateAsync(
            memKey, fields: null, addTags: null, removeTags: null, addLinks: null,
            removeLinks: new Dictionary<string, IReadOnlyList<string>> { ["related"] = new[] { fileKey } }, Ct);
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.False, "An unlinked target is not evaluated for staleness.");
            Assert.That(view.StaleLinks, Is.Null.Or.Empty);
        });
    }

    [Test]
    public async Task Recall_of_a_memory_entry_without_links_reports_not_stale()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.RememberAsync(
            "acme", "notes", id: null, MemoryKind.Note, title: "plain", body: "no links",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        var view = await store.RecallAsync(result.Key, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.False);
            Assert.That(view.StaleLinks, Is.Null.Or.Empty);
        });
    }
}
