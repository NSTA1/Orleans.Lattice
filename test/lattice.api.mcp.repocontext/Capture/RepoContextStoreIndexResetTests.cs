using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for <see cref="RepoContextStore.ResetIndexAsync"/>: the
/// code-only reset drops the structural, symbol, content, cross-reference,
/// session, and every vector tree for the repository, preserves the root marker
/// with its index-derived fields cleared so the repository stays enumerable in
/// <see cref="RepoContextStore.ListReposAsync"/>, and leaves the store-of-record
/// memory tree untouched. It shares the
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
    /// Seeds the root marker the way a completed ingest leaves it: the three
    /// index-derived registers populated, plus authored metadata (display name,
    /// default branch, and a tag) that a reset has no claim on.
    /// </summary>
    private static async Task SeedIngestedMarkerAsync(
        RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var tags = new OrSet();
        tags.Add(System.Text.Encoding.UTF8.GetBytes("primary"), "seed", 1);

        var node = new RepoNode
        {
            RepoId = repoId,
            DisplayName = RepoContextValues.Lww("Acme Platform", clock),
            DefaultBranch = RepoContextValues.Lww("main", clock),
            LastIngested = RepoContextValues.Lww("2026-01-01T00:00:00.0000000+00:00", clock),
            FileCount = RepoContextValues.Lww(1234L, clock),
            IndexedCommit = RepoContextValues.Lww("deadbeefcafe", clock),
            Tags = tags,
        };

        var serializer = harness.Services.GetRequiredService<Serializer<RepoNode>>();
        await Tree(harness, RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.Repo(repoId), serializer.SerializeToArray(node), ct);
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
    public async Task ResetIndexAsync_drops_every_code_index_tree_and_preserves_the_marker_and_memory()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var (codeIndex, memory) = await SeedFullRepoAsync(harness, "acme", Ct);

        var result = await store.ResetIndexAsync("acme", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            // Six code-index records. The root marker is preserved rather than
            // deleted, so it is NOT counted, and neither are the two memory
            // records.
            Assert.That(result.EntriesDeleted, Is.EqualTo(codeIndex.Count));
        });

        foreach (var (treeName, key) in codeIndex)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Null, $"Code-index {treeName}:{key} should have been tombstoned.");
        }

        var marker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("acme"), Ct);
        Assert.That(marker, Is.Not.Null,
            "The root marker MUST survive: it is the only structural key left, so deleting it would drop "
            + "the repository out of list_repos while its preserved memory sat underneath, undiscoverable.");

        foreach (var (treeName, key) in memory)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Not.Null, $"Memory {treeName}:{key} must survive the code-only reset.");
        }
    }

    /// <summary>
    /// The discriminator for issue 2168. A code-only reset preserves the memory
    /// tree, which is worthless if the repository itself becomes unreachable: a
    /// caller resolves a repository id from <c>list_repos</c>, and the standing
    /// guidance is explicitly not to derive it from a working directory. Deleting
    /// the root marker emptied the repository's structural subtree entirely and
    /// <see cref="RepoContextStore.ListRepoIdsAsync"/> derives the whole listing
    /// from that tree, so the repository vanished from the listing while its
    /// memory survived underneath - reachable only by an id the caller already
    /// knew. This test fails on the delete-the-marker behaviour.
    /// <para>
    /// The three index-derived fields must read back null rather than carrying
    /// the pre-reset ingest across: a file count and an ingest timestamp for an
    /// index that was just deleted is a confidently precise lie, worse than the
    /// absence it replaces. Null on all three says exactly what is true - the
    /// repository is known but not indexed - which absence could never
    /// distinguish from "never onboarded".
    /// </para>
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_leaves_the_repository_listed_with_its_index_fields_cleared_and_memory_intact()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var (_, memory) = await SeedFullRepoAsync(harness, "acme", Ct);
        await SeedIngestedMarkerAsync(harness, "acme", Ct);

        var before = await store.ListReposAsync(Ct);
        Assert.That(before.Repos.Select(r => r.RepoId), Does.Contain("acme"),
            "Precondition: the seeded repository is listed before the reset.");

        await store.ResetIndexAsync("acme", Ct);

        var after = await store.ListReposAsync(Ct);
        var row = after.Repos.SingleOrDefault(r => r.RepoId == "acme");

        Assert.That(row, Is.Not.Null,
            "A reset repository MUST still be resolvable through list_repos - that is the only way a caller "
            + "who does not already know the id can reach the memory the reset deliberately preserved.");
        Assert.Multiple(() =>
        {
            Assert.That(row!.LastIngested, Is.Null,
                "lastIngested must be cleared: the ingest it named no longer exists.");
            Assert.That(row.FileCount, Is.Null,
                "fileCount must be cleared: reporting a precise count for a deleted index is worse than none.");
            Assert.That(row.IndexedCommit, Is.Null,
                "indexedCommit must be cleared: the index no longer corresponds to any revision.");
        });

        foreach (var (treeName, key) in memory)
        {
            var value = await Tree(harness, treeName).GetAsync(key, Ct);
            Assert.That(value, Is.Not.Null, $"Memory {treeName}:{key} must survive the reset.");
        }
    }

    /// <summary>
    /// The marker's authored metadata - display name, default branch, and tags -
    /// is not index-derived: it is patched in through <c>repocontext_update</c>
    /// rather than written by an ingest, so a code-only reset has no claim on it.
    /// Only the three ingest-derived registers are cleared.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_carries_authored_repository_metadata_across_the_reset()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedIngestedMarkerAsync(harness, "acme", Ct);

        await store.ResetIndexAsync("acme", Ct);

        var markerBytes = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("acme"), Ct);
        Assert.That(markerBytes, Is.Not.Null);

        var node = harness.Services.GetRequiredService<Serializer<RepoNode>>().Deserialize(markerBytes!);

        Assert.Multiple(() =>
        {
            Assert.That(node.RepoId, Is.EqualTo("acme"));
            Assert.That(RepoContextValues.ReadString(node.DisplayName), Is.EqualTo("Acme Platform"));
            Assert.That(RepoContextValues.ReadString(node.DefaultBranch), Is.EqualTo("main"));
            Assert.That(node.Tags.Elements().Select(System.Text.Encoding.UTF8.GetString), Does.Contain("primary"));

            Assert.That(RepoContextValues.ReadString(node.LastIngested), Is.Null);
            Assert.That(RepoContextValues.ReadInt64(node.FileCount), Is.Null);
            Assert.That(RepoContextValues.ReadString(node.IndexedCommit), Is.Null);
        });
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

        // A reset preserves a registration; it must never invent one. Writing a
        // marker here would register a repository nobody onboarded and surface it
        // in list_repos out of nothing.
        var marker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("never-onboarded"), Ct);
        Assert.That(marker, Is.Null,
            "Resetting a never-onboarded repository must not write a root marker for it.");

        var listing = await store.ListReposAsync(Ct);
        Assert.That(listing.Repos.Select(r => r.RepoId), Does.Not.Contain("never-onboarded"));
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

    /// <summary>
    /// The guard for issue 2168, and the assertion that stops marker preservation
    /// leaking into the destructive verb. <c>reset_index</c> now keeps the root
    /// marker so a reset repository stays listed; <c>remove_repo</c> must still
    /// delete it, so a removed repository disappears from
    /// <see cref="RepoContextStore.ListReposAsync"/> entirely. This test passes
    /// both before and after the reset change - it exists to fail if the two
    /// verbs are ever conflated.
    /// </summary>
    [Test]
    public async Task RemoveRepoAsync_still_drops_the_repository_from_the_listing_entirely()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedFullRepoAsync(harness, "acme", Ct);
        await SeedIngestedMarkerAsync(harness, "acme", Ct);
        await SeedFullRepoAsync(harness, "acme-tools", Ct);

        await store.RemoveRepoAsync("acme", Ct);

        var listing = await store.ListReposAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(listing.Repos.Select(r => r.RepoId), Does.Not.Contain("acme"),
                "remove_repo must still drop the repository from list_repos entirely - marker preservation "
                + "belongs to reset_index alone and must not leak into the destructive verb.");
            Assert.That(listing.Repos.Select(r => r.RepoId), Does.Contain("acme-tools"),
                "A sibling repository is unaffected.");
        });

        var marker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("acme"), Ct);
        Assert.That(marker, Is.Null, "remove_repo must still delete the root marker.");
    }
}
