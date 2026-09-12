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
    /// The census clear must land before the tree sweep, not after it.
    /// <para>
    /// Sampling only after the reset returns cannot tell a correct reset from one
    /// whose steps are sequenced wrongly: both end in the same state. The window
    /// that separates them is the sweep itself, which runs for minutes on a real
    /// corpus and is exactly when an operator asks "did it work?". So this test
    /// takes two samples - one with the reset provably parked mid-sweep, one after
    /// it returns - and asserts the cleared census at both.
    /// </para>
    /// <para>
    /// The gate is aimed at the symbol tree because the structural tree is swept
    /// first: parking a call on the tree that also holds the marker would block the
    /// very read this test needs to take. <see cref="LatticeTreeCallGate.WasReached"/>
    /// is asserted so a gate that never matched fails loudly instead of quietly
    /// degrading this into a single-sample test that proves nothing.
    /// </para>
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_clears_the_census_before_the_sweep_so_it_is_observable_while_the_reset_runs()
    {
        var gate = new LatticeTreeCallGate
        {
            TreeId = RepoContextTrees.Symbol,
            Method = nameof(ILattice.DeleteRangeAsync),
        };

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureSilo = silo =>
                {
                    silo.Services.AddSingleton(gate);
                    silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeGatingFilter>();
                },
            }, Ct);
        var store = Store(harness);

        await SeedFullRepoAsync(harness, "acme", Ct);
        await SeedIngestedMarkerAsync(harness, "acme", Ct);

        var before = (await store.ListReposAsync(Ct)).Repos.Single(r => r.RepoId == "acme");
        Assert.That(before.FileCount, Is.EqualTo(1234L),
            "Precondition: before the reset, list_repos serves the ingest census. Without this the "
            + "mid-sweep sample below could read null because nothing was ever there.");

        var reset = store.ResetIndexAsync("acme", Ct);

        await gate.Reached.WaitAsync(TimeSpan.FromSeconds(30), Ct);

        // Sample A - the reset is parked inside the sweep, one tree in.
        var during = (await store.ListReposAsync(Ct)).Repos.SingleOrDefault(r => r.RepoId == "acme");

        gate.Release();
        var result = await reset;

        // Sample B - the reset has returned.
        var after = (await store.ListReposAsync(Ct)).Repos.SingleOrDefault(r => r.RepoId == "acme");

        Assert.That(gate.WasReached, Is.True,
            "The gate never matched, so no sample was taken mid-sweep and this test proves nothing "
            + "about ordering. Fix the gate rather than the assertion.");

        Assert.That(during, Is.Not.Null,
            "A reset in flight must not un-list the repository: an operator polling list_repos through "
            + "the sweep would read the repository as gone rather than as resetting.");

        Assert.Multiple(() =>
        {
            // Sample A. This is the assertion the pre-fix ordering fails: the clear
            // used to run after the sweep, so for the sweep's whole duration these
            // three served the complete pre-reset census at full confidence.
            Assert.That(during!.FileCount, Is.Null,
                "Mid-sweep, fileCount still reported the pre-reset census. That is not merely a late "
                + "signature - it is unobservable at the only moment anyone looks.");
            Assert.That(during.LastIngested, Is.Null,
                "Mid-sweep, lastIngested still named an ingest whose index is being deleted.");
            Assert.That(during.IndexedCommit, Is.Null,
                "Mid-sweep, indexedCommit still named a revision the index no longer corresponds to.");

            // Sample B. Separates the two candidate mechanisms: if the clear landed
            // but only late, A fails and B passes; if it never became observable at
            // all, both fail.
            Assert.That(after, Is.Not.Null);
            Assert.That(after!.FileCount, Is.Null, "After the reset, fileCount must be cleared.");
            Assert.That(after.LastIngested, Is.Null, "After the reset, lastIngested must be cleared.");
            Assert.That(after.IndexedCommit, Is.Null, "After the reset, indexedCommit must be cleared.");

            // The reset now reports what it did rather than leaving a caller to
            // infer it from an absence. RepoContextStore has no logger, so the
            // return value is the only channel available to it.
            Assert.That(result.CensusCleared, Is.True,
                "The reset must report clearing the census, so a caller that reads a cleared census can "
                + "tell 'the reset cleared it' from 'there was never one to clear'.");
            Assert.That(result.TreesSwept, Is.EquivalentTo(RepoContextTrees.CodeIndexTrees),
                "The reset must name the trees it swept, so partial coverage is visible rather than "
                + "presenting as a low deletion count.");
            Assert.That(result.MemoryPreserved, Is.True,
                "The reset must state that memory was preserved rather than leaving it to be inferred.");
            Assert.That(result.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(0L));
        });
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

        await Assert.MultipleAsync(async () =>
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
    /// The discriminator for issue 2179. Marker preservation was
    /// preserve-if-present, so a repository holding index records but no root
    /// marker still vanished from <c>list_repos</c> across a reset - the exact
    /// 2168 failure mode, surviving in a corner. It is enumerable before the
    /// reset (its structural subtree keys carry the listing) and not enumerable
    /// after it (subtree swept, no marker to preserve).
    /// <para>
    /// The count is what makes this a discriminator rather than a null check: a
    /// sibling repository is seeded alongside, so the pre-fix listing is 1 and
    /// the post-fix listing is 2.
    /// </para>
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_re_derives_the_root_marker_when_the_sweep_found_an_index_but_no_marker()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        // A repository whose index records exist but whose marker does not. The
        // portability seam reaches this state: ExportAsync is bounded by an
        // arbitrary prefix, and RepoScanPrefix covers repo/{repoId}/ while the
        // marker key carries no trailing separator, so a subtree-scoped snapshot
        // restores index records with no marker among them.
        await SeedFullRepoAsync(harness, "orphan", Ct);
        await Tree(harness, RepoContextTrees.Structural).DeleteAsync(RepoContextKeys.Repo("orphan"), Ct);

        // An ordinary sibling, so the listing count discriminates rather than
        // merely reporting empty.
        await SeedMarkerAsync(harness, "sibling", Ct);

        var before = await store.ListReposAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(before.Repos.Count, Is.EqualTo(2),
                "Precondition: the subtree keys alone keep the marker-less repository enumerable.");
            Assert.That(before.Repos.Select(r => r.RepoId), Does.Contain("orphan"));
        });

        var result = await store.ResetIndexAsync("orphan", Ct);
        Assert.That(result.EntriesDeleted, Is.GreaterThan(0),
            "Precondition: the sweep must find an index, which is what proves the repository was onboarded.");

        var after = await store.ListReposAsync(Ct);
        Assert.That(after.Repos.Count, Is.EqualTo(2),
            "A reset repository whose marker was missing must stay enumerable: the sweep just deleted its "
            + "index, which is direct evidence it had one, so re-deriving the marker registers nothing new.");

        var row = after.Repos.SingleOrDefault(r => r.RepoId == "orphan");
        Assert.That(row, Is.Not.Null);
        Assert.Multiple(() =>
        {
            // The same "registered, no index" shape the preserve branch produces.
            Assert.That(row!.LastIngested, Is.Null);
            Assert.That(row.FileCount, Is.Null);
            Assert.That(row.IndexedCommit, Is.Null);
        });

        // The preserved memory is now reachable by a caller who does not already
        // know the id, which is the whole point of keeping the repository listed.
        var survivingMemory = await Tree(harness, RepoContextTrees.Memory)
            .GetAsync(RepoContextKeys.Memory("orphan", "decisions", "d1"), Ct);
        Assert.That(survivingMemory, Is.Not.Null);
    }

    /// <summary>
    /// The guard that keeps the re-derivation above from becoming "invent a
    /// marker whenever one is missing". The discriminator is the sweep's own
    /// deletion count, not the presence of any record at all: a repository
    /// holding only memory records has no code index, so the sweep deletes
    /// nothing and no registration is invented.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_invents_no_marker_for_a_repository_holding_only_memory_records()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await Tree(harness, RepoContextTrees.Memory)
            .SetAsync(RepoContextKeys.Memory("memory-only", "decisions", "d1"), new byte[] { 1, 2, 3 }, Ct);

        var result = await store.ResetIndexAsync("memory-only", Ct);

        Assert.That(result.EntriesDeleted, Is.EqualTo(0),
            "The memory tree is never swept by a code-only reset, so the deletion count stays zero.");

        var marker = await Tree(harness, RepoContextTrees.Structural)
            .GetAsync(RepoContextKeys.Repo("memory-only"), Ct);
        Assert.That(marker, Is.Null,
            "A zero-deletion reset proves no code index was present, so it must invent no registration.");
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

    /// <summary>
    /// The completion half of the #2642 observability contract. After a reset
    /// finishes, the same status verb onboarding uses (<c>index_status</c>,
    /// backed by the job grain) reports the teardown <see cref="RepoIndexStatus.Completed"/>
    /// with a completion time, an elapsed duration, and the count of trees swept -
    /// not the pre-2642 <see cref="RepoIndexStatus.None"/>, which read as "never
    /// attempted" and was indistinguishable from a reset that never ran.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_reports_a_completed_teardown_through_index_status()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var (codeIndex, _) = await SeedFullRepoAsync(harness, "acme", Ct);

        var result = await store.ResetIndexAsync("acme", Ct);

        var progress = await harness.GrainFactory.GetGrain<IRepoIndexJobGrain>("acme").GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Completed),
                "A finished reset reports Completed through index_status - not None, which reads as "
                + "'never attempted' and is the reading that prompts re-running a destructive verb.");
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Done));
            Assert.That(progress.CompletedAt, Is.Not.Null, "A completed reset stamps a completion time.");
            Assert.That(progress.ElapsedMilliseconds, Is.Not.Null,
                "A completed reset records how long it took.");
            Assert.That(progress.TreesSwept, Is.EqualTo(RepoContextTrees.CodeIndexTrees.Count),
                "Every code-index tree was swept, and the completion snapshot says so.");
            Assert.That(progress.EntriesDeleted, Is.EqualTo(result.EntriesDeleted),
                "The pollable status agrees with the returned result on how much was dropped.");
            Assert.That(progress.EntriesDeleted, Is.EqualTo(codeIndex.Count));
        });
    }

    /// <summary>
    /// The in-progress half, and the guard against an optimistic completion
    /// marker. A reset interrupted before it finishes (here, a token already
    /// cancelled when the sweep begins) must stay <see cref="RepoIndexStatus.Running"/>
    /// in phase <see cref="RepoIndexPhase.Resetting"/> and must never carry a
    /// completion time: <see cref="RepoIndexJobGrain.CompleteResetAsync"/> is the
    /// sole completion signal and runs only after the sweep loop finishes, which
    /// an interrupted reset never reaches. If a completion marker were ever
    /// written at or before <see cref="RepoIndexJobGrain.BeginResetAsync"/> - the
    /// exact inversion #2642 exists to prevent - this test's
    /// <c>CompletedAt Is.Null</c> assertion fires.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_interrupted_before_it_finishes_stays_running_and_never_reports_complete()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        await SeedFullRepoAsync(harness, "acme", Ct);
        await SeedIngestedMarkerAsync(harness, "acme", Ct);

        // TearDown and BeginReset take no token and run, marking the teardown
        // Running; the census read and the sweep loop honour the token and throw
        // before CompleteResetAsync is ever reached.
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Assert.CatchAsync<OperationCanceledException>(
            async () => await store.ResetIndexAsync("acme", cts.Token));

        var progress = await harness.GrainFactory.GetGrain<IRepoIndexJobGrain>("acme").GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Running),
                "An interrupted reset is in progress, not done.");
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Resetting),
                "The teardown phase is visible so a caller sees a reset - not a build - in flight.");
            Assert.That(progress.CompletedAt, Is.Null,
                "A reset that never finished must never carry a completion time - the optimistic-marker guard.");
            Assert.That(progress.Status, Is.Not.EqualTo(RepoIndexStatus.Completed),
                "Absence of completion must never be readable as success.");
        });
    }
}
