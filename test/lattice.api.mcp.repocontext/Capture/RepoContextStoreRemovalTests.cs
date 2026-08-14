using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for <see cref="RepoContextStore.RemoveRepoAsync"/> and the
/// repository enumeration it is verified against. Removal drains every reserved
/// context tree's <c>repo/{repoId}/</c> subtree through the resilient range-delete
/// helper and then deletes the bare root marker, so a purge must empty all five
/// trees, drop the marker, exclude the repository from <c>list_repos</c>, and
/// leave a sibling repository untouched. The sibling-visibility test also pins the
/// enumeration fix: a hyphenated sibling id sorts between a shorter id's marker and
/// its subtree, so it must still be discovered.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory grain
/// storage and the reserved structural, memory, and vector trees) via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev
/// loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreRemovalTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static ILattice Tree(RepoContextMcpHarness harness, string treeName)
        => harness.GrainFactory.GetGrain<ILattice>(treeName);

    private static async Task SeedMarkerAsync(RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<RepoNode>>();
        var bytes = serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        await Tree(harness, RepoContextTrees.Structural).SetAsync(RepoContextKeys.Repo(repoId), bytes, ct);
    }

    /// <summary>
    /// Writes one record into every reserved tree under the repository's subtree
    /// plus its root marker, and returns the (tree, key) pairs so a test can assert
    /// each one is gone after removal.
    /// </summary>
    private static async Task<IReadOnlyList<(string Tree, string Key)>> SeedFullRepoAsync(
        RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        await SeedMarkerAsync(harness, repoId, ct);

        var payload = new byte[] { 1, 2, 3 };

        // The membership tree stores an enable-wins OrFlag per source, JSON-encoded;
        // the repo summary path scans and decodes it, so seed a real enabled flag
        // rather than opaque bytes.
        var membershipFlag = new OrFlag();
        membershipFlag.Enable("seed", 1);
        var membershipValue = JsonLatticeSerializer<OrFlag>.Default.Serialize(membershipFlag);

        var seeded = new (string Tree, string Key)[]
        {
            (RepoContextTrees.Structural, RepoContextKeys.File(repoId, "src/A.cs")),
            (RepoContextTrees.Symbol, RepoContextKeys.Symbol(repoId, "Acme.A")),
            (RepoContextTrees.Memory, RepoContextKeys.Memory(repoId, "notes", "1")),
            (RepoContextTrees.VectorMembership, RepoContextKeys.VectorMembership(repoId, "default")),
            (RepoContextTrees.VectorPayload, RepoContextKeys.VectorPayload(repoId, "cafe")),
            (RepoContextTrees.VectorMetadata, RepoContextKeys.Vector(repoId, "v1")),
        };

        foreach (var (treeName, key) in seeded)
        {
            var value = treeName == RepoContextTrees.VectorMembership ? membershipValue : payload;
            await Tree(harness, treeName).SetAsync(key, value, ct);
        }

        return seeded;
    }

    [Test]
    public async Task RemoveRepoAsync_purges_every_tree_and_the_marker_and_reports_the_count()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var seeded = await SeedFullRepoAsync(harness, "acme", Ct);

        var result = await store.RemoveRepoAsync("acme", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            // Six subtree records plus the bare root marker.
            Assert.That(result.EntriesDeleted, Is.EqualTo(7));
        });

        foreach (var (treeName, key) in seeded)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Null, $"{treeName}:{key} should have been tombstoned.");
        }

        var marker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("acme"), Ct);
        Assert.That(marker, Is.Null, "The root marker should have been deleted.");

        var repos = await store.ListReposAsync(Ct);
        Assert.That(repos.Repos.Select(r => r.RepoId), Does.Not.Contain("acme"));
    }

    [Test]
    public async Task RemoveRepoAsync_leaves_a_sibling_repository_untouched()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedFullRepoAsync(harness, "acme", Ct);
        var siblingSeeded = await SeedFullRepoAsync(harness, "acme-tools", Ct);

        await store.RemoveRepoAsync("acme", Ct);

        foreach (var (treeName, key) in siblingSeeded)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Not.Null, $"Sibling {treeName}:{key} must survive the removal of acme.");
        }

        var siblingMarker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("acme-tools"), Ct);
        Assert.That(siblingMarker, Is.Not.Null, "The sibling root marker must survive.");

        var repos = await store.ListReposAsync(Ct);
        Assert.That(repos.Repos.Select(r => r.RepoId), Does.Contain("acme-tools"));
        Assert.That(repos.Repos.Select(r => r.RepoId), Does.Not.Contain("acme"));
    }

    [Test]
    public async Task RemoveRepoAsync_on_an_absent_repository_is_a_zero_deletion_no_op()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.RemoveRepoAsync("never-onboarded", Ct);

        Assert.That(result.EntriesDeleted, Is.EqualTo(0));
    }

    [Test]
    public async Task ListReposAsync_discovers_a_hyphenated_sibling_that_sorts_inside_a_shorter_ids_span()
    {
        // "svc" has marker repo/svc and subtree repo/svc/...; "svc-api" sorts
        // between them because '-' (0x2D) orders below '/' (0x2F). A jump straight
        // to the end of svc's subtree would skip svc-api; enumeration must not.
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedFullRepoAsync(harness, "svc", Ct);
        await SeedFullRepoAsync(harness, "svc-api", Ct);

        var repos = await store.ListReposAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(repos.Repos.Select(r => r.RepoId), Does.Contain("svc"));
            Assert.That(repos.Repos.Select(r => r.RepoId), Does.Contain("svc-api"));
            Assert.That(repos.Repos.Select(r => r.RepoId).Distinct().Count(),
                Is.EqualTo(repos.Repos.Count), "Each repository is enumerated exactly once.");
        });
    }
}
