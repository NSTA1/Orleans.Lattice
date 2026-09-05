using System.IO;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests that the directory-modification-time prune cache is actually acted on
/// across consecutive reconciles, rather than merely being written and never read.
/// <para>
/// Pruning has no direct observable on the result, so it is proved by its one
/// documented side effect: a pruned directory is carried forward wholesale, so an
/// in-place content edit - which does not bump the parent directory's modification
/// time - is invisible to a pruned pass and visible to a full one.
/// </para>
/// <para>
/// Passes are spaced by the widest gap the background reconcile can produce
/// (<c>ReconcileInterval</c> plus the whole of <c>ReconcileIntervalJitter</c>),
/// but the deadline pruning has to survive is counted in <em>passes</em>, not
/// wall clock: the reconcile is single-flight, so a pass that runs long simply
/// pushes the next one out and must not, by itself, spend the whole budget. A
/// <c>PassesPerFullWalk</c> of one makes every reconcile a forced full sweep,
/// which is a silent whole-feature regression with no other symptom.
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    /// <summary>
    /// A fixed modification time stamped onto every directory before each pass, so
    /// the two passes observe identical directory modification times. Real in-place
    /// content edits leave a directory's modification time alone, but an NTFS
    /// directory's lazily-flushed time can drift by a tick between two walks under
    /// parallel load, which would defeat pruning non-deterministically. The
    /// lower-level <see cref="RepoTreeWalkerPruningTests"/> pin for the same reason.
    /// </summary>
    private static readonly DateTime PinnedDirTime = new(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static readonly RepoContextIndexingOptions Defaults = new();

    /// <summary>The widest gap the background reconcile can leave between two passes.</summary>
    private static readonly TimeSpan WidestReconcileGap =
        Defaults.ReconcileInterval + Defaults.ReconcileIntervalJitter;

    private RepoContextBootstrapRequest PruneRequest(bool allowPrune) =>
        new() { RepoRoot = _harness.RepoRoot, RepoId = RepoId, AllowPrune = allowPrune };

    private void PinDirectoryMtimes()
    {
        Directory.SetLastWriteTimeUtc(_harness.RepoRoot, PinnedDirTime);
        foreach (var dir in Directory.EnumerateDirectories(_harness.RepoRoot, "*", SearchOption.AllDirectories))
        {
            Directory.SetLastWriteTimeUtc(dir, PinnedDirTime);
        }
    }

    /// <summary>Replaces the default harness with one on a controllable clock.</summary>
    private AdvanceableClock UseFakeClock()
    {
        _harness.Dispose();
        var clock = new AdvanceableClock();
        _harness = new BootstrapHarness(clock);
        return clock;
    }

    /// <summary>
    /// Seeds one file, runs the cold pass that publishes the pruning baseline, then
    /// edits the file in place without disturbing any directory modification time.
    /// </summary>
    private async Task SeedAndEditInPlaceAsync()
    {
        _harness.WriteFile("src/a.cs", "class A { }");
        PinDirectoryMtimes();

        // Cold: no snapshot exists, so this pass is forced full whatever the request
        // says, and it publishes the baseline the next pass prunes from.
        var cold = await _harness.Service.RunAsync(PruneRequest(allowPrune: true), progress: null);
        Assert.That(cold.FilesAdded, Is.EqualTo(1), "cold pass should ingest the file");

        _harness.WriteFile("src/a.cs", "class A { int x; }");
        PinDirectoryMtimes();
    }

    [Test]
    public async Task A_consented_reconcile_inside_the_full_walk_pass_budget_prunes()
    {
        var clock = UseFakeClock();
        await SeedAndEditInPlaceAsync();

        clock.Advance(WidestReconcileGap);

        // Consent, a populated snapshot, and still inside the full-walk pass budget,
        // so the walk prunes: src/ is carried forward unstatted and the edit goes unseen.
        var second = await _harness.Service.RunAsync(PruneRequest(allowPrune: true), progress: null);

        Assert.That(
            second.FilesUpdated,
            Is.Zero,
            "the reconcile should have pruned src/, leaving the in-place edit undetected");
    }

    [Test]
    public async Task A_reconcile_without_prune_consent_still_observes_an_in_place_edit()
    {
        var clock = UseFakeClock();
        await SeedAndEditInPlaceAsync();

        clock.Advance(WidestReconcileGap);

        // The negative control: identical scenario, consent withheld, so the walk is
        // forced full and the same edit is caught.
        var second = await _harness.Service.RunAsync(PruneRequest(allowPrune: false), progress: null);

        Assert.That(second.FilesUpdated, Is.EqualTo(1));
    }

    [Test]
    public async Task A_consented_reconcile_past_the_full_walk_pass_budget_sweeps_in_full()
    {
        var clock = UseFakeClock();
        await SeedAndEditInPlaceAsync();

        // The cold pass was itself the full sweep, so the budget allows
        // PassesPerFullWalk - 1 pruned passes before the next forced one. Burn
        // exactly those, then the pass after them must sweep.
        for (var i = 0; i < Defaults.PassesPerFullWalk - 1; i++)
        {
            clock.Advance(WidestReconcileGap);
            var pruned = await _harness.Service.RunAsync(PruneRequest(allowPrune: true), progress: null);
            Assert.That(pruned.FilesUpdated, Is.Zero, $"pass {i + 2} is inside the budget and must prune");
        }

        clock.Advance(WidestReconcileGap);

        // The safety net pruning is allowed to have: once the budget is spent, a
        // consented reconcile sweeps anyway, so an in-place edit is never missed
        // indefinitely.
        var swept = await _harness.Service.RunAsync(PruneRequest(allowPrune: true), progress: null);

        Assert.That(swept.FilesUpdated, Is.EqualTo(1));
    }

    [Test]
    public async Task A_pass_that_outlives_the_full_walk_interval_still_prunes_the_next_one()
    {
        // Regression for issue #2048. The reconcile is single-flight, so the real
        // spacing between two walks is the previous pass's duration whenever that
        // exceeds the configured timer. A wall-clock full-walk deadline read against
        // that spacing is therefore always already past on a repository big enough
        // to matter, so pruning never engaged and the snapshot was written every run
        // and never read. Simulating a pass that takes many times the configured
        // full-walk interval must not defeat the very next prune.
        var clock = UseFakeClock();
        await SeedAndEditInPlaceAsync();

        clock.Advance(Defaults.FullWalkInterval * 10);

        var second = await _harness.Service.RunAsync(PruneRequest(allowPrune: true), progress: null);

        Assert.That(
            second.FilesUpdated,
            Is.Zero,
            "a long pass consumes one pass of the budget, not the whole wall-clock deadline");
    }

    [Test]
    public void Pruning_can_engage_under_the_shipped_defaults()
    {
        // The invariant issue #2048 says was necessary but not sufficient, restated
        // in the units the reconcile actually enforces: the defaults must leave at
        // least one pruned pass in every full-walk cycle, or RepoWalkPruning is dead
        // code with no other symptom.
        Assert.Multiple(() =>
        {
            Assert.That(Defaults.PruningCanEngage, Is.True);
            Assert.That(Defaults.PassesPerFullWalk, Is.GreaterThanOrEqualTo(2));
        });
    }

    /// <summary>
    /// A clock the test drives by hand, so a pass can be placed an exact interval
    /// after the previous one instead of microseconds after it. Only
    /// <see cref="GetUtcNow"/> is consulted by the service under test.
    /// </summary>
    private sealed class AdvanceableClock : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        internal void Advance(TimeSpan by) => _now += by;
    }
}