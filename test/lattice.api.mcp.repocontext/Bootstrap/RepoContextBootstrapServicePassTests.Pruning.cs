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
/// The clock is advanced between passes by the widest spacing the background
/// reconcile can produce (<c>ReconcileInterval</c> plus the whole of
/// <c>ReconcileIntervalJitter</c>), because that spacing is exactly what pruning
/// has to survive to be worth anything. A <c>FullWalkInterval</c> that does not
/// outlive it makes every reconcile a forced full sweep, which is a silent
/// whole-feature regression with no other symptom.
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
    public async Task A_consented_reconcile_within_the_full_walk_interval_prunes()
    {
        var clock = UseFakeClock();
        await SeedAndEditInPlaceAsync();

        clock.Advance(WidestReconcileGap);

        // Consent, a populated snapshot, and still inside the full-walk interval, so
        // the walk prunes: src/ is carried forward unstatted and the edit goes unseen.
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
    public async Task A_consented_reconcile_past_the_full_walk_interval_sweeps_in_full()
    {
        var clock = UseFakeClock();
        await SeedAndEditInPlaceAsync();

        clock.Advance(Defaults.FullWalkInterval + TimeSpan.FromMinutes(1));

        // The safety net pruning is allowed to have: once the full-walk interval has
        // elapsed, a consented reconcile sweeps anyway, so an in-place edit is never
        // missed indefinitely.
        var second = await _harness.Service.RunAsync(PruneRequest(allowPrune: true), progress: null);

        Assert.That(second.FilesUpdated, Is.EqualTo(1));
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