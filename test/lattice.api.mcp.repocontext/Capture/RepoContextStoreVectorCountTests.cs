using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for the durable embedded-vector tally that
/// <see cref="RepoContextStore.ListReposAsync"/> reports as
/// <see cref="RepoContextRepoSummary.EmbeddedVectorCount"/>. The count is read
/// from the store of record (the vector-membership add-wins set the vector writer
/// maintains), never from a run's in-flight progress, so it is a restart-durable
/// diagnostic: it is exactly the number of files whose embedding has landed and
/// converges on the repository's file count as vectorisation completes.
/// <para>
/// The read is non-blocking (issue 1992): a listing serves the last completed
/// membership walk and schedules a refresh out of band rather than scanning the
/// tree inline. The tally is therefore eventually exact, so these tests settle the
/// outstanding refresh before asserting a figure.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory grain
/// storage and the reserved structural and vector trees) via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev
/// loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreVectorCountTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static (RepoContextStore Store, RepoContextVectorWriter Writer) Resolve(RepoContextMcpHarness harness)
        => (harness.Services.GetRequiredService<RepoContextStore>(),
            harness.Services.GetRequiredService<RepoContextVectorWriter>());

    private static async Task SeedRepoMarkerAsync(RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        // ListReposAsync discovers a repository from its structural keys, so a
        // marker must exist for the repo to be enumerated at all. A default node
        // (identity only) is enough - the tally under test comes from the
        // vector-membership tree, not this marker.
        var serializer = harness.Services.GetRequiredService<Serializer<RepoNode>>();
        var bytes = serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        var structural = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        await structural.SetAsync(RepoContextKeys.Repo(repoId), bytes, ct);
    }

    /// <summary>
    /// Lists the repositories and returns the settled tally for one of them.
    /// <para>
    /// A listing never scans the membership tree inline, so the first call can report
    /// a not-yet-measured (<see langword="null"/>) or stale count while a refresh is
    /// outstanding. This drains that refresh and re-lists until the reported count is
    /// flagged current, which is what makes the eventual figure assertable.
    /// </para>
    /// </summary>
    private static async Task<long?> SettledEmbeddedVectorCountAsync(
        RepoContextStore store, RepoContextVectorWriter writer, string repoId, CancellationToken ct)
    {
        // Bounded rather than unbounded: a genuinely stuck refresh must fail the test
        // rather than hang it. Each pass drains at most one refresh, and no membership
        // write happens concurrently in these tests, so a couple of passes suffice.
        for (var attempt = 0; attempt < 10; attempt++)
        {
            var list = await store.ListReposAsync(ct);
            var summary = list.Repos.Single(r => r.RepoId == repoId);
            if (!summary.EmbeddedVectorCountPending)
            {
                return summary.EmbeddedVectorCount;
            }

            var pending = writer.PendingEmbeddedCountRefresh(repoId);
            if (pending is not null)
            {
                await pending.WaitAsync(ct);
            }
        }

        Assert.Fail($"The embedded-vector count for '{repoId}' never settled.");
        return null;
    }

    [Test]
    public async Task EmbeddedVectorCount_is_zero_for_a_repo_with_no_embeddings()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, writer) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        var count = await SettledEmbeddedVectorCountAsync(store, writer, RepoId, Ct);

        Assert.That(count, Is.EqualTo(0L),
            "A repository whose embeddings have not started landing reports a zero tally, not an error.");
    }

    [Test]
    public async Task EmbeddedVectorCount_matches_the_live_membership_set_size()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, writer) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        var keyA = RepoContextKeys.File(RepoId, "src/A.cs");
        var keyB = RepoContextKeys.File(RepoId, "src/B.cs");
        await writer.AddMembersAsync(RepoId, new[] { keyA, keyB }, Ct);

        var count = await SettledEmbeddedVectorCountAsync(store, writer, RepoId, Ct);

        Assert.That(count, Is.EqualTo(2L),
            "The tally is the live size of the vector-membership set - one per file whose embedding landed.");
    }

    [Test]
    public async Task EmbeddedVectorCount_drops_when_a_source_is_retired()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, writer) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        var keyA = RepoContextKeys.File(RepoId, "src/A.cs");
        var keyB = RepoContextKeys.File(RepoId, "src/B.cs");
        await writer.AddMembersAsync(RepoId, new[] { keyA, keyB }, Ct);
        await writer.RetireAsync(RepoId, keyA, Ct);

        var count = await SettledEmbeddedVectorCountAsync(store, writer, RepoId, Ct);

        Assert.That(count, Is.EqualTo(1L),
            "Retiring a deleted source observed-removes it from the set, so the tally stays honest.");
    }

    /// <summary>
    /// The listing itself must not walk the membership tree. That walk is what timed
    /// <c>repocontext_list_repos</c> out during a back-fill, because every membership
    /// write invalidates the count's exactness key and the tool then re-scanned tens of
    /// thousands of entries per call (issue 1992).
    /// <para>
    /// Asserted by its observable consequence: with a write landed since the last
    /// completed walk, the listing reports the <em>earlier</em> figure and flags it
    /// pending. A listing that scanned inline could not report the earlier figure.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_listing_serves_a_stale_count_rather_than_scanning_the_membership_tree()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, writer) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);
        await SettledEmbeddedVectorCountAsync(store, writer, RepoId, Ct);

        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/B.cs") }, Ct);
        var list = await store.ListReposAsync(Ct);
        var summary = list.Repos.Single(r => r.RepoId == RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(summary.EmbeddedVectorCount, Is.EqualTo(1L),
                "The listing reports the last completed walk, proving it did not walk the "
                + "membership tree inline - that would have returned 2.");
            Assert.That(summary.EmbeddedVectorCountPending, Is.True,
                "and marks the figure stale, so an operator can tell a settled count from a "
                + "superseded one instead of being handed a wrong number that looks current.");
        });
    }

    /// <summary>
    /// A never-counted repository must report an unknown count, not zero: an operator
    /// waiting on a back-fill reads zero as "no vectors landed", a distinct and
    /// actionable state that must not be manufactured by the diagnostic itself.
    /// </summary>
    [Test]
    public async Task EmbeddedVectorCount_is_unknown_until_a_walk_completes()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, _) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        var list = await store.ListReposAsync(Ct);
        var summary = list.Repos.Single(r => r.RepoId == RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(summary.EmbeddedVectorCount, Is.Null,
                "The first listing reports the count as not yet measured rather than as zero.");
            Assert.That(summary.EmbeddedVectorCountPending, Is.True,
                "and says a walk is outstanding, which is what makes the null readable.");
        });
    }
}
