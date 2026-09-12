using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Regression tests for issue #2395: a stalled range-scan page raised by the
/// vector retirement arm must not abort the whole indexing pass.
/// <para>
/// The retirement arm reads a range of the vector-metadata tree, so it can raise
/// <see cref="ScanPageStalledException"/>. It ran unguarded and ahead of every
/// structural write, so that fault aborted the pass before a single file node was
/// committed: the repository re-scanned every file on the next pass and banked
/// nothing, indefinitely. The arm now joins the same collect-and-rethrow
/// discipline the embedding arms already used - the pass is still reported as
/// failed and re-driven, but the adds, updates and back-fills it computed are
/// committed first.
/// </para>
/// <para>
/// The deferral is deliberately ALL-OR-NOTHING: on a retirement fault the removal
/// set is emptied wholesale, so no pruning decision is ever taken on a partial
/// view of the vector membership. A tolerance model that banked a truncated read
/// and acted on it would be the convergence defect of issue #2287 in a new place;
/// deferring whole asserts nothing and cannot produce it.
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    private static ScanPageStalledException Stall() => new()
    {
        TreeId = "repo-context-vector-metadata",
        ShardIndex = 41,
        Operation = "GetSortedKeysBatchAsync",
        Phase = "walk",
        LeavesVisited = 7,
    };

    /// <summary>
    /// Arranges a pass with both a new file to commit and a stored-but-gone file to
    /// prune, and faults the retirement arm with a stalled scan page.
    /// </summary>
    private ScanPageStalledException ArrangeStalledRetirement()
    {
        _harness.WriteFile("kept.cs", "class Kept { }");
        _harness.SeedStoredFile("gone.cs", "class Gone { }");

        var stall = Stall();
        _harness.VectorIngestor
            .RetireAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(stall));
        return stall;
    }

    [Test]
    public void A_stalled_retirement_still_commits_the_added_file_nodes()
    {
        var stall = ArrangeStalledRetirement();

        // The pass must still FAIL - the fault is collected, never swallowed - so a
        // caller and the run record both still see it, and the pass is re-driven.
        var thrown = Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await _harness.Service.RunAsync(_harness.Request(), progress: null));

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(stall), "The originating stall must propagate unwrapped.");

            // The whole point of the change: structural progress is banked even though
            // the pass failed. Before the fix nothing was committed and the next pass
            // re-scanned every file from scratch.
            Assert.That(
                _harness.Store.Keys, Does.Contain(RepoContextKeys.File(RepoId, "kept.cs")),
                "A stalled retirement must not cost the pass its structural writes.");
            Assert.That(
                _harness.AtomicWrites, Is.GreaterThan(0),
                "The apply must have run despite the retirement fault.");
        });
    }

    [Test]
    public void A_stalled_retirement_defers_the_removal_wholesale()
    {
        ArrangeStalledRetirement();

        Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await _harness.Service.RunAsync(_harness.Request(), progress: null));

        Assert.Multiple(() =>
        {
            // The discriminating pair. That the removal did not happen is on its own
            // consistent with the OLD behaviour (which pruned nothing because it
            // aborted first), so it is asserted together with the fact that the apply
            // ran anyway. Only the guarded arm produces both.
            Assert.That(
                _harness.AtomicWrites, Is.GreaterThan(0),
                "The pass must have proceeded to the apply.");
            Assert.That(
                _harness.Store.Keys, Does.Contain(RepoContextKeys.File(RepoId, "gone.cs")),
                "A structural record must never be pruned when its vector retirement did not complete.");
        });
    }

    [Test]
    public void A_stalled_retirement_is_reported_with_its_tree_and_shard()
    {
        ArrangeStalledRetirement();

        Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await _harness.Service.RunAsync(_harness.Request(), progress: null));

        Assert.Multiple(() =>
        {
            // Which arm the abort cost is exactly what the lattice-layer stall counter
            // cannot say, so the arm names itself and carries the stall's own typed
            // context rather than leaving it to timestamp correlation.
            Assert.That(
                _harness.LogEntries.Any(e =>
                    e.Level == LogLevel.Warning
                    && e.Message.Contains("repo-context-vector-metadata", StringComparison.Ordinal)
                    && e.Message.Contains("41", StringComparison.Ordinal)),
                Is.True,
                "The stall must be reported with the tree and shard it stalled on.");
            Assert.That(
                _harness.LogEntries.Any(e =>
                    e.Level == LogLevel.Warning
                    && e.Message.Contains("deferring", StringComparison.Ordinal)),
                Is.True,
                "The wholesale deferral must be stated in the log rather than inferred.");
        });
    }
}
