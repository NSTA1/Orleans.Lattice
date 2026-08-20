using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for <see cref="RepoContextEmbeddingGapScanner"/> against a
/// live in-memory Lattice cluster: the keys-only, resumable structural-file scan
/// that the self-index grain uses to decide whether a repository has a file whose
/// embedding never landed. They prove it reports no gap when every file is a live
/// member, reports a gap at the first unembedded file, and pages a large file
/// range with a resumable checkpoint.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (the
/// structural and membership trees) via <see cref="RepoContextMcpHarness"/>, so it
/// is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextEmbeddingGapScannerTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private async Task SeedFileAsync(RepoContextMcpHarness harness, string relativePath)
    {
        // The scanner reads keys only, never the node, so any non-null value suffices
        // to make the structural file key present in the scanned range.
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        await tree.SetAsync(RepoContextKeys.File(RepoId, relativePath), new byte[] { 1 }, Ct);
    }

    private static RepoContextEmbeddingGapScanner Scanner(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextEmbeddingGapScanner>();

    private static RepoContextVectorWriter Writer(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextVectorWriter>();

    [Test]
    public async Task ScanFilePageAsync_reports_no_gap_when_every_file_is_embedded()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        await SeedFileAsync(harness, "src/A.cs");
        await SeedFileAsync(harness, "src/B.cs");
        await Writer(harness).AddMembersAsync(
            RepoId,
            new[] { RepoContextKeys.File(RepoId, "src/A.cs"), RepoContextKeys.File(RepoId, "src/B.cs") },
            Ct);

        var scanner = Scanner(harness);
        var page = await scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(page.GapFound, Is.False, "Both files are live members, so there is no gap.");
            Assert.That(page.HasMore, Is.False, "The whole (small) file range fit in one page.");
            Assert.That(page.NextResumeKey, Is.Null);
        });
    }

    [Test]
    public async Task ScanFilePageAsync_reports_a_gap_at_the_first_unembedded_file()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        await SeedFileAsync(harness, "src/A.cs");
        await SeedFileAsync(harness, "src/B.cs");
        // Only A is embedded; B is the gap.
        await Writer(harness).AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        var scanner = Scanner(harness);
        var page = await scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(page.GapFound, Is.True, "B has no live embedding, so the repository has a gap.");
            Assert.That(page.HasMore, Is.False, "A found gap ends the scan; no mid-repository resume is handed back.");
            Assert.That(page.NextResumeKey, Is.Null);
        });
    }

    [Test]
    public async Task ScanFilePageAsync_reports_no_gap_for_an_empty_repository()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var scanner = Scanner(harness);
        var page = await scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(page.GapFound, Is.False);
            Assert.That(page.HasMore, Is.False);
            Assert.That(page.NextResumeKey, Is.Null);
        });
    }

    [Test]
    public async Task ScanFilePageAsync_pages_a_clean_range_and_resumes_to_completion()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        // Seed five files, all embedded, and scan with a page size of two so the
        // range spans multiple pages and the resume checkpoint is exercised.
        var keys = new List<string>();
        foreach (var name in new[] { "a.cs", "b.cs", "c.cs", "d.cs", "e.cs" })
        {
            await SeedFileAsync(harness, name);
            keys.Add(RepoContextKeys.File(RepoId, name));
        }

        await Writer(harness).AddMembersAsync(RepoId, keys, Ct);

        var scanner = Scanner(harness);

        string? resume = null;
        var pages = 0;
        var sawMore = false;
        while (true)
        {
            var page = await scanner.ScanFilePageAsync(RepoId, resume, pageSize: 2, Ct);
            pages++;
            Assert.That(page.GapFound, Is.False, "Every file is embedded, so no page finds a gap.");

            if (!page.HasMore)
            {
                break;
            }

            sawMore = true;
            Assert.That(page.NextResumeKey, Is.Not.Null, "A page that reports more hands back a resume key.");
            resume = page.NextResumeKey;
            Assert.That(pages, Is.LessThan(10), "The paged walk must terminate.");
        }

        Assert.Multiple(() =>
        {
            Assert.That(sawMore, Is.True, "Five files at a page size of two must span more than one page.");
            Assert.That(pages, Is.GreaterThan(1), "The clean range was walked across multiple resumable pages.");
        });
    }

    [Test]
    public async Task ScanFilePageAsync_reports_no_gap_for_a_contentless_marked_file()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        await SeedFileAsync(harness, "src/A.cs");
        await SeedFileAsync(harness, "src/empty.cs");
        // A carries a real embedding; empty.cs was considered and found contentless, so
        // it is recorded as a marker rather than an embedded member. Neither is a gap -
        // this is the #1553 fix: an empty file must stop being an eternal gap.
        await Writer(harness).AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);
        await Writer(harness).MarkContentlessAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/empty.cs") }, Ct);

        var scanner = Scanner(harness);
        var page = await scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);

        Assert.That(page.GapFound, Is.False,
            "A contentless-marked file is covered, so the gap sweep no longer treats it as a missing embedding.");
    }

    [Test]
    public async Task ScanFilePageAsync_rejects_a_non_positive_page_size()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var scanner = Scanner(harness);

        Assert.That(
            () => scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 0, Ct),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
