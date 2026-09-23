using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// The coverage-digest audit standing aside for the indexing pacer (issue #3447).
/// The audit is the one O(sources) membership read on the self-heal path, so a due
/// audit is postponed while the pacer says background work should defer - but at
/// most <see cref="RepoContextSelfIndexGrain.MaxCoverageAuditPostponements"/> sweeps
/// in a row, so a host that is busy all the time still audits.
/// </summary>
[TestFixture]
public sealed class RepoContextSelfIndexGrainPacingTests
{
    private static RepoContextIndexingPacer IdlePacer() => new(
        new RepoContextIndexingOptions(),
        TimeProvider.System,
        NullLogger<RepoContextIndexingPacer>.Instance,
        memoryLoad: () => 0.1);

    private static async Task<SelfIndexGrainHarness> ArmedWithDueAuditAsync(RepoContextIndexingPacer pacer)
    {
        var harness = new SelfIndexGrainHarness { Pacer = pacer };
        harness.SeedEmbeddedFile("src/A.cs");
        await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());
        harness.State.State.NextReconcileAfterTicks = long.MaxValue;
        harness.State.State.NextCoverageAuditAfterTicks = 0;
        return harness;
    }

    /// <summary>Runs one sweep, first clearing the cooldown the previous sweep set.</summary>
    private static Task SweepAsync(SelfIndexGrainHarness harness)
    {
        harness.State.State.NextSweepAfterTicks = 0;
        return harness.TickAsync();
    }

    [Test]
    public async Task ScanStep_idle_pacer_runs_a_due_audit_at_once()
    {
        var harness = await ArmedWithDueAuditAsync(IdlePacer());

        await SweepAsync(harness);

        Assert.That(harness.State.State.NextCoverageAuditAfterTicks, Is.GreaterThan(0),
            "Nothing is competing, so the due audit runs and schedules the next one.");
    }

    [Test]
    public async Task ScanStep_foreground_request_postpones_a_due_audit_boundedly()
    {
        var pacer = IdlePacer();
        var harness = await ArmedWithDueAuditAsync(pacer);
        using var lease = pacer.EnterForeground();

        for (var sweep = 1; sweep <= RepoContextSelfIndexGrain.MaxCoverageAuditPostponements; sweep++)
        {
            await SweepAsync(harness);
            Assert.That(harness.State.State.NextCoverageAuditAfterTicks, Is.Zero,
                $"sweep {sweep}: the audit stands aside and stays due, so the next sweep asks again");
        }

        await SweepAsync(harness);

        Assert.That(harness.State.State.NextCoverageAuditAfterTicks, Is.GreaterThan(0),
            "Past the postponement cap the audit runs regardless, so it is never starved.");
    }
}
