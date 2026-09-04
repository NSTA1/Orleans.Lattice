using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for the non-blocking embedded-source count that
/// <c>repocontext_list_repos</c> reports as <c>embeddedVectorCount</c>.
/// <para>
/// Computing that count exactly means walking the whole vector-membership tree, and
/// the memo it used to be served from is keyed by a generation that <b>any</b>
/// membership write advances. During a back-fill - exactly when an operator lists
/// repositories to watch progress - the memo therefore never hit and every call
/// re-walked tens of thousands of entries on the largest tree in the store, timing
/// the tool out and head-of-line-blocking the shard it walked (issue 1992).
/// </para>
/// <para>
/// The contract asserted here is the one that replaced it:
/// <see cref="RepoContextVectorWriter.CountEmbeddedAsync"/> serves the last completed
/// figure and states its currency, starts at most one refresh per repository, and
/// reports a never-measured repository as unknown rather than as zero. The exact walk
/// stays available, and priced, as <see cref="RepoContextVectorWriter.ScanEmbeddedAsync"/>.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory grain
/// storage and the reserved vector trees) via <see cref="RepoContextMcpHarness"/>, so
/// it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextVectorWriterEmbeddedCountTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextVectorWriter Resolve(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextVectorWriter>();

    private static Task<RepoContextMcpHarness> StartAsync(CancellationToken ct)
        => RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, ct);

    private static string FileKey(int index) => RepoContextKeys.File(RepoId, $"src/f{index}.cs");

    /// <summary>
    /// Drains any outstanding refresh and re-reads until the count is flagged current,
    /// so a test can assert the eventual exact figure without racing the background
    /// walk. Bounded, so a stuck refresh fails the test rather than hanging it.
    /// </summary>
    private static async Task<RepoContextEmbeddedCount> SettleAsync(
        RepoContextVectorWriter writer, CancellationToken ct)
    {
        for (var attempt = 0; attempt < 10; attempt++)
        {
            var count = await writer.CountEmbeddedAsync(RepoId, ct);
            if (!count.Pending)
            {
                return count;
            }

            var pending = writer.PendingEmbeddedCountRefresh(RepoId);
            if (pending is not null)
            {
                await pending.WaitAsync(ct);
            }
        }

        Assert.Fail("The embedded count never settled.");
        return default;
    }

    /// <summary>
    /// The never-measured case must be distinguishable from the genuinely-empty one. An
    /// operator watching a back-fill reads <c>0</c> as "no vectors landed yet", which is
    /// a real and alarming state; reporting it for "nobody has counted yet" would be a
    /// wrong answer that looks entirely plausible.
    /// </summary>
    [Test]
    public async Task An_unmeasured_repository_reports_an_unknown_count_rather_than_zero()
    {
        await using var harness = await StartAsync(Ct);
        var writer = Resolve(harness);

        await writer.AddMembersAsync(RepoId, new[] { FileKey(0) }, Ct);

        var first = await writer.CountEmbeddedAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(first.Count, Is.Null,
                "A repository nothing has counted yet reports an unknown count - not zero, "
                + "which is the distinct and alarming answer 'no vectors have landed'.");
            Assert.That(first.Pending, Is.True,
                "and says a refresh is outstanding, so a caller knows to poll again.");
        });
    }

    /// <summary>
    /// The defect in issue 1992: a membership write advances the generation, so a read
    /// that insisted on exactness re-walked the whole tree. The fix is asserted by its
    /// observable consequence - the read returns the <em>pre-write</em> figure, which it
    /// could only do by not scanning.
    /// </summary>
    [Test]
    public async Task A_read_after_a_write_serves_the_previous_figure_instead_of_rescanning()
    {
        await using var harness = await StartAsync(Ct);
        var writer = Resolve(harness);

        await writer.AddMembersAsync(RepoId, new[] { FileKey(0) }, Ct);
        await writer.ScanEmbeddedAsync(RepoId, Ct);

        // Advance the generation the memo is keyed by. Under the old exact-only
        // contract this alone forced a whole-tree walk on the very next read.
        await writer.AddMembersAsync(RepoId, new[] { FileKey(1) }, Ct);

        var afterWrite = await writer.CountEmbeddedAsync(RepoId, Ct);
        var settled = await SettleAsync(writer, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(afterWrite.Count, Is.EqualTo(1),
                "The read serves the last completed figure - proof it did not walk the tree, "
                + "which would have returned 2.");
            Assert.That(afterWrite.Pending, Is.True,
                "and flags it stale rather than passing a superseded number off as current.");
            Assert.That(settled.Count, Is.EqualTo(2),
                "The out-of-band refresh then brings it up to date,");
            Assert.That(settled.Pending, Is.False, "and it is exact again.");
        });
    }

    /// <summary>
    /// A burst of reads during a back-fill - every one of them a generation miss - must
    /// schedule one membership walk, not one per read. Without the single-flight guard
    /// the fix would simply move the pile-up off the request thread rather than remove
    /// it, and the tree would still be walked once per <c>list_repos</c> call.
    /// </summary>
    [Test]
    public async Task At_most_one_refresh_runs_at_a_time_for_a_repository()
    {
        await using var harness = await StartAsync(Ct);
        var writer = Resolve(harness);

        await writer.AddMembersAsync(RepoId, new[] { FileKey(0) }, Ct);

        // Each iteration writes, so every read is a generation miss and would start a
        // refresh if nothing suppressed it.
        var observed = new List<Task>();
        for (var i = 1; i <= 24; i++)
        {
            await writer.AddMembersAsync(RepoId, new[] { FileKey(i) }, Ct);
            _ = await writer.CountEmbeddedAsync(RepoId, Ct);

            if (writer.PendingEmbeddedCountRefresh(RepoId) is { } refresh
                && (observed.Count == 0 || !ReferenceEquals(observed[^1], refresh)))
            {
                // A refresh instance can only be replaced once its predecessor settled -
                // that is precisely "at most one in flight", asserted without racing a
                // clock.
                if (observed.Count > 0)
                {
                    Assert.That(observed[^1].IsCompleted, Is.True,
                        "A new refresh started only after the previous one finished, so the "
                        + "reads share one membership walk instead of piling them up.");
                }

                observed.Add(refresh);
            }
        }

        Assert.That(observed, Is.Not.Empty,
            "The reads did schedule refreshes, so the assertion above was actually exercised.");

        var settled = await SettleAsync(writer, Ct);
        Assert.That(settled.Count, Is.EqualTo(25),
            "and the count still converges on the true membership size once the writes stop.");
    }

    /// <summary>
    /// The escape hatch stays honest: a caller that genuinely needs an exact figure can
    /// still get one, at the cost of the walk, and doing so re-primes the memo so the
    /// next non-blocking read is served as exact rather than pending.
    /// </summary>
    [Test]
    public async Task An_exact_scan_is_still_available_and_primes_the_served_count()
    {
        await using var harness = await StartAsync(Ct);
        var writer = Resolve(harness);

        await writer.AddMembersAsync(RepoId, new[] { FileKey(0), FileKey(1), FileKey(2) }, Ct);

        var scanned = await writer.ScanEmbeddedAsync(RepoId, Ct);
        var served = await writer.CountEmbeddedAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(scanned, Is.EqualTo(3), "The exact walk counts every live member,");
            Assert.That(served.Count, Is.EqualTo(3), "the cheap read then agrees with it,");
            Assert.That(served.Pending, Is.False,
                "and reports it as current, because the walk stamped the memo at the "
                + "generation the read observes.");
        });
    }

    /// <summary>
    /// A failing refresh must not wedge the guard. If the single-flight entry survived a
    /// fault, the count would freeze at whatever it last held and never retry - a defect
    /// that only shows up long after the fault that caused it.
    /// </summary>
    [Test]
    public async Task A_settled_refresh_releases_the_guard_so_a_later_read_can_retry()
    {
        await using var harness = await StartAsync(Ct);
        var writer = Resolve(harness);

        await writer.AddMembersAsync(RepoId, new[] { FileKey(0) }, Ct);

        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        var first = writer.PendingEmbeddedCountRefresh(RepoId);
        if (first is not null)
        {
            await first.WaitAsync(Ct);
        }

        await writer.AddMembersAsync(RepoId, new[] { FileKey(1) }, Ct);
        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        var settled = await SettleAsync(writer, Ct);

        Assert.That(settled.Count, Is.EqualTo(2),
            "A second refresh ran after the first settled, so the guard released rather "
            + "than freezing the count at the first walk's result.");
    }
}
