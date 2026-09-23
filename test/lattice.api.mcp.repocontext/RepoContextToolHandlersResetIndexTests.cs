using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Integration tests for <see cref="RepoContextToolHandlers.ResetIndexAsync"/> at the
/// tool seam: the reset's sweep is bound to the host lifetime rather than to the
/// calling request, so a caller that is cancelled or disconnects part-way through
/// no longer takes the sweep down with it. The reset still runs to completion and
/// reports its outcome through <c>repocontext_index_status</c>, which is how a
/// caller that lost the response learns whether the reset finished (issue #2642).
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextToolHandlersResetIndexTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static ILattice Tree(RepoContextMcpHarness harness, string treeName)
        => harness.GrainFactory.GetGrain<ILattice>(treeName);

    private static IRepoIndexJobGrain Job(RepoContextMcpHarness harness)
        => harness.GrainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);

    private static async Task<(string Tree, string Key)[]> SeedCodeIndexAsync(
        RepoContextMcpHarness harness, CancellationToken ct)
    {
        var records = new (string Tree, string Key)[]
        {
            (RepoContextTrees.Structural, RepoContextKeys.File(RepoId, "src/A.cs")),
            (RepoContextTrees.Symbol, RepoContextKeys.Symbol(RepoId, "Acme.A")),
            (RepoContextTrees.Content, RepoContextKeys.Content(RepoId, "src/A.cs")),
        };

        foreach (var (treeName, key) in records)
        {
            await Tree(harness, treeName).SetAsync(key, [1, 2, 3], ct);
        }

        return records;
    }

    /// <summary>
    /// Polls the job surface until it reports a terminal outcome
    /// (<see cref="RepoIndexStatus.Completed"/> or <see cref="RepoIndexStatus.Failed"/>),
    /// which is exactly what a caller that lost the response does. The detached
    /// sweep may not have begun yet when polling starts, so a transient
    /// <see cref="RepoIndexStatus.None"/> (the teardown clears the job first) is
    /// waited through rather than read as an outcome.
    /// </summary>
    private static async Task<RepoIndexProgress> PollUntilSettledAsync(RepoContextMcpHarness harness)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        var progress = await Job(harness).GetProgressAsync();
        while (progress.Status is not (RepoIndexStatus.Completed or RepoIndexStatus.Failed) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
            progress = await Job(harness).GetProgressAsync();
        }

        return progress;
    }

    /// <summary>
    /// The regression for #2642. Before the fix the handler passed the request's
    /// cancellation token straight into the sweep, so a caller that was cancelled
    /// or dropped its connection cancelled the teardown with it, and the job
    /// surface was left reporting a running reset that nothing would ever finish -
    /// indistinguishable from one still working. Now the request token abandons
    /// only the wait: the sweep keeps running, finishes, and says so.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_finishes_the_reset_when_the_calling_request_is_cancelled()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var records = await SeedCodeIndexAsync(harness, Ct);
        var context = await RepoContextRequestContexts.CreateAsync(harness.Services);

        using var requestCancelled = new CancellationTokenSource();
        await requestCancelled.CancelAsync();

        Assert.CatchAsync<OperationCanceledException>(
            async () => await RepoContextToolHandlers.ResetIndexAsync(context, RepoId, requestCancelled.Token),
            "A cancelled caller stops waiting; it is not handed a result it is no longer there to read.");

        var progress = await PollUntilSettledAsync(harness);

        Assert.Multiple(() =>
        {
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Completed),
                "The reset outlives the cancelled request and reports its own completion through index_status.");
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Done));
            Assert.That(progress.CompletedAt, Is.Not.Null);
            Assert.That(progress.EntriesDeleted, Is.EqualTo(records.Length),
                "The completion snapshot reports the records the detached sweep actually dropped.");
        });

        foreach (var (treeName, key) in records)
        {
            Assert.That(await Tree(harness, treeName).GetAsync(key, Ct), Is.Null,
                $"The detached sweep dropped '{key}' from '{treeName}'.");
        }
    }

    /// <summary>
    /// A caller that stays connected is still handed the reset result, and the
    /// result agrees with the job surface.
    /// </summary>
    [Test]
    public async Task ResetIndexAsync_returns_the_reset_result_to_a_caller_that_waits()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var records = await SeedCodeIndexAsync(harness, Ct);
        var context = await RepoContextRequestContexts.CreateAsync(harness.Services);

        var result = await RepoContextToolHandlers.ResetIndexAsync(context, RepoId, Ct);
        var progress = await Job(harness).GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo(RepoId));
            Assert.That(result.EntriesDeleted, Is.EqualTo(records.Length));
            Assert.That(result.MemoryPreserved, Is.True);
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Completed));
            Assert.That(progress.EntriesDeleted, Is.EqualTo(result.EntriesDeleted));
        });
    }
}
