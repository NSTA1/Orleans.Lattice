using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for <see cref="RepoContextStore.ResetIndexAsync"/>: the
/// code-only reset drops the structural, symbol, content, cross-reference,
/// session, and every vector tree for the repository plus the root marker, and
/// leaves the store-of-record memory tree untouched. It shares the
/// cancel/drain/clear preamble with the full remove path, so the same
/// in-flight-run drain applies.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory
/// grain storage and the reserved trees) via <see cref="RepoContextMcpHarness"/>,
/// so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreIndexResetTests
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
    /// Seeds every code-index tree and the memory tree for a repository. Returns
    /// (tree, key) pairs for each seeded record so a test can assert which
    /// entries survived and which were tombstoned.
    /// </summary>
    private static async Task<(IReadOnlyList<(string Tree, string Key)> CodeIndex,
        IReadOnlyList<(string Tree, string Key)> Memory)> SeedFullRepoAsync(
        RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        await SeedMarkerAsync(harness, repoId, ct);

        var payload = new byte[] { 1, 2, 3 };

        var membershipFlag = new OrFlag();
        membershipFlag.Enable("seed", 1);
        var membershipValue = JsonLatticeSerializer<OrFlag>.Default.Serialize(membershipFlag);

        // Seed one representative record per code-index tree.
        var codeIndex = new (string Tree, string Key)[]
        {
            (RepoContextTrees.Structural, RepoContextKeys.File(repoId, "src/A.cs")),
            (RepoContextTrees.Symbol, RepoContextKeys.Symbol(repoId, "Acme.A")),
            (RepoContextTrees.Content, RepoContextKeys.Content(repoId, "src/A.cs")),
            (RepoContextTrees.VectorMembership, RepoContextKeys.VectorMembership(repoId, "default")),
            (RepoContextTrees.VectorPayload, RepoContextKeys.VectorPayload(repoId, "cafe")),
            (RepoContextTrees.VectorMetadata, RepoContextKeys.Vector(repoId, "v1")),
        };

        foreach (var (treeName, key) in codeIndex)
        {
            var value = treeName == RepoContextTrees.VectorMembership ? membershipValue : payload;
            await Tree(harness, treeName).SetAsync(key, value, ct);
        }

        // Seed one memory entry per topic under the memory tree - the reset must
        // leave every one of these intact.
        var memory = new (string Tree, string Key)[]
        {
            (RepoContextTrees.Memory, RepoContextKeys.Memory(repoId, "decisions", "d1")),
            (RepoContextTrees.Memory, RepoContextKeys.Memory(repoId, "gotchas", "g1")),
        };

        foreach (var (treeName, key) in memory)
        {
            await Tree(harness, treeName).SetAsync(key, payload, ct);
        }

        return (codeIndex, memory);
    }

    [Test]
    public async Task ResetIndexAsync_drops_every_code_index_tree_and_the_marker_and_preserves_memory()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var (codeIndex, memory) = await SeedFullRepoAsync(harness, "acme", Ct);

        var result = await store.ResetIndexAsync("acme", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            // Six code-index records plus the root marker; the two memory records
            // are preserved and are NOT counted.
            Assert.That(result.EntriesDeleted, Is.EqualTo(codeIndex.Count + 1));
        });

        foreach (var (treeName, key) in codeIndex)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Null, $"Code-index {treeName}:{key} should have been tombstoned.");
        }

        var marker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("acme"), Ct);
        Assert.That(marker, Is.Null, "The root marker should have been deleted.");

        foreach (var (treeName, key) in memory)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Not.Null, $"Memory {treeName}:{key} must survive the code-only reset.");
        }
    }

    /// <summary>
    /// Trap 1 pinned as a test: the vector plane holds embeddings for memory
    /// entries too, and the vector-membership tree records which memory keys are
    /// already embedded as memkey- markers. A code-only reset drops both the
    /// membership markers and the payloads together, so the surviving memory
    /// records are re-embeddable on the next indexing pass. Preserving the
    /// markers while dropping the payloads would leave memory permanently
    /// unreachable by semantic search; this test would fail if that regression
    /// were introduced.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_drops_vector_membership_markers_together_with_the_payloads()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedMarkerAsync(harness, "acme", Ct);

        var payload = new byte[] { 9, 9, 9 };
        var membershipFlag = new OrFlag();
        membershipFlag.Enable("seed", 1);
        var membershipValue = JsonLatticeSerializer<OrFlag>.Default.Serialize(membershipFlag);

        // Mark a memory record as embedded (memkey- marker) and stash its
        // payload/metadata, so the plane looks like a real embedded memory entry.
        var memkeyMarker = RepoContextKeys.VectorMembershipsPrefix("acme")
            + "memkey-decisions/d1";
        await Tree(harness, RepoContextTrees.VectorMembership).SetAsync(memkeyMarker, membershipValue, Ct);
        await Tree(harness, RepoContextTrees.VectorPayload)
            .SetAsync(RepoContextKeys.VectorPayload("acme", "abc123"), payload, Ct);
        await Tree(harness, RepoContextTrees.VectorMetadata)
            .SetAsync(RepoContextKeys.Vector("acme", "mem1"), payload, Ct);
        await Tree(harness, RepoContextTrees.Memory)
            .SetAsync(RepoContextKeys.Memory("acme", "decisions", "d1"), payload, Ct);

        await store.ResetIndexAsync("acme", Ct);

        Assert.Multiple(async () =>
        {
            Assert.That(await Tree(harness, RepoContextTrees.VectorMembership).GetAsync(memkeyMarker, Ct),
                Is.Null,
                "The memkey- marker MUST be tombstoned alongside the payloads or memory becomes unsearchable.");
            Assert.That(await Tree(harness, RepoContextTrees.VectorPayload)
                    .GetAsync(RepoContextKeys.VectorPayload("acme", "abc123"), Ct),
                Is.Null);
            Assert.That(await Tree(harness, RepoContextTrees.VectorMetadata)
                    .GetAsync(RepoContextKeys.Vector("acme", "mem1"), Ct),
                Is.Null);
            Assert.That(await Tree(harness, RepoContextTrees.Memory)
                    .GetAsync(RepoContextKeys.Memory("acme", "decisions", "d1"), Ct),
                Is.Not.Null,
                "The memory record itself must survive - only its vector footprint is dropped.");
        });
    }

    /// <summary>
    /// Trap 2 pinned as a test: the write-once, content-addressed vector payload
    /// tree is dropped by an operator-invoked reset (and re-derived by the
    /// follow-up ingest), even though the self-healer's allow-list excludes it.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_drops_the_write_once_vector_payload_tree()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedMarkerAsync(harness, "acme", Ct);
        var payloadKey = RepoContextKeys.VectorPayload("acme", "cafe");
        await Tree(harness, RepoContextTrees.VectorPayload).SetAsync(payloadKey, new byte[] { 1 }, Ct);

        await store.ResetIndexAsync("acme", Ct);

        Assert.That(await Tree(harness, RepoContextTrees.VectorPayload).GetAsync(payloadKey, Ct),
            Is.Null,
            "An operator-invoked reset drops the write-once payload tree; the follow-up ingest re-derives it.");
    }

    [Test]
    public async Task ResetIndexAsync_leaves_a_sibling_repository_untouched()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedFullRepoAsync(harness, "acme", Ct);
        var (siblingCodeIndex, siblingMemory) = await SeedFullRepoAsync(harness, "acme-tools", Ct);

        await store.ResetIndexAsync("acme", Ct);

        foreach (var (treeName, key) in siblingCodeIndex)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Not.Null, $"Sibling code-index {treeName}:{key} must survive.");
        }

        foreach (var (treeName, key) in siblingMemory)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Not.Null, $"Sibling memory {treeName}:{key} must survive.");
        }

        var siblingMarker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("acme-tools"), Ct);
        Assert.That(siblingMarker, Is.Not.Null, "The sibling root marker must survive.");
    }

    [Test]
    public async Task ResetIndexAsync_on_an_absent_repository_is_a_zero_deletion_no_op()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.ResetIndexAsync("never-onboarded", Ct);

        Assert.That(result.EntriesDeleted, Is.EqualTo(0));
    }

    /// <summary>
    /// The hard constraint from the issue: adding a code-only reset must not
    /// silently convert <c>repocontext_remove_repo</c> into a preserving verb.
    /// A full remove still tombstones every memory record.
    /// </summary>
    [Test]
    public async Task RemoveRepoAsync_still_removes_memory_after_reset_index_landed()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var (_, memory) = await SeedFullRepoAsync(harness, "acme", Ct);

        await store.RemoveRepoAsync("acme", Ct);

        foreach (var (treeName, key) in memory)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Null,
                $"remove_repo must still tombstone {treeName}:{key} - the destructive verb is unchanged.");
        }
    }
}
