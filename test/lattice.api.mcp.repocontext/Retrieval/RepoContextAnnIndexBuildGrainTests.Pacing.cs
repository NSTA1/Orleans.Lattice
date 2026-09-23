using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The approximate-index build standing aside for the indexing pacer (issue #3447):
/// while a foreground request is open the build defers its slices, but boundedly,
/// so it is slowed and never starved, and the activation's first step - the one that
/// opens the in-memory index queries are served from - is never deferred.
/// </summary>
public sealed partial class RepoContextAnnIndexBuildGrainTests
{
    private static RepoContextIndexingPacer IdlePacer() => new(
        new RepoContextIndexingOptions(),
        TimeProvider.System,
        NullLogger<RepoContextIndexingPacer>.Instance,
        memoryLoad: () => 0.1);

    private static async Task<int> ConvergeAsync(RepoContextIndexingPacer? pacer, bool foreground)
    {
        var durable = new Durable { Pacer = pacer };
        durable.SeedRing(64);
        using var lease = foreground ? pacer!.EnterForeground() : null;
        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);
        var ticks = await PumpAsync(process);
        Assert.That(durable.State.State.Converged, Is.True, "The build must still converge.");
        return ticks;
    }

    [Test]
    public async Task ProcessNextPhaseAsync_idle_pacer_never_defers_the_build()
    {
        var baseline = await ConvergeAsync(pacer: null, foreground: false);

        var paced = await ConvergeAsync(IdlePacer(), foreground: false);

        Assert.That(paced, Is.EqualTo(baseline), "An idle pacer costs the build nothing.");
    }

    [Test]
    public async Task ProcessNextPhaseAsync_foreground_request_defers_each_slice_boundedly_and_the_build_still_converges()
    {
        var baseline = await ConvergeAsync(pacer: null, foreground: false);
        Assert.That(baseline, Is.GreaterThan(1), "The corpus must take several slices, or deferral proves nothing.");

        var paced = await ConvergeAsync(IdlePacer(), foreground: true);

        // The first step runs at once; every later step stands aside for exactly the
        // deferral cap of ticks and then runs, so each costs cap + 1 ticks.
        var window = RepoContextAnnIndexBuildGrain.MaxConsecutivePacerDeferrals + 1;
        Assert.That(paced, Is.EqualTo(1 + ((baseline - 1) * window)),
            $"baseline {baseline} ticks; a permanently-open lease must slow the build, never starve it");
    }
}
