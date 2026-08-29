using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeFallOffLogDetector"/>. The detector
/// is the leaf activation gate that classifies the replay path before
/// the materialiser drives <c>ILeafProjection.Apply</c> over the WAL
/// slice. These tests pin the contract that the <c>-1</c> "nothing
/// applied" sentinel - emitted by the operator-driven projection
/// rebuild path and by a freshly persisted leaf with no observed
/// activations - is accepted as a valid checkpoint offset and does not
/// false-positive any of the three fall-off triggers (WAL trim,
/// replay budget, projection age).
/// </summary>
[TestFixture]
public sealed class LatticeFallOffLogDetectorTests
{
    private const string TreeId = "tree-detector";
    private const int ShardIndex = 0;

    private static (LatticeFallOffLogDetector Detector, ICommitLogReader Reader) CreateDetector(
        long head,
        long tail)
    {
        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(head));
        reader.GetTailOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(tail));

        var sc = new ServiceCollection();
        sc.AddSingleton(reader);
        var services = sc.BuildServiceProvider();

        return (new LatticeFallOffLogDetector(services), reader);
    }

    private static async Task<ResolvedLatticeOptions> BuildOptionsAsync(
        LatticeOptions? baseOptions = null)
    {
        var resolver = TestOptionsResolver.Create(
            baseOptions: baseOptions ?? new LatticeOptions(),
            maxLeafKeys: 128,
            shardCount: 1,
            factory: Substitute.For<IGrainFactory>());
        return await resolver.ResolveAsync(TreeId).ConfigureAwait(false);
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_returns_TailReplay_when_WAL_empty()
    {
        // Empty WAL: head == tail == 0. The "nothing applied" sentinel
        // is -1; the gap (head - checkpoint) == 1 must not exceed the
        // default replay budget, the WAL-trim check must short-circuit
        // because checkpoint <= 0, and the age check is disabled.
        var (detector, _) = CreateDetector(head: 0, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_returns_TailReplay_when_WAL_populated_and_within_budget()
    {
        // After an operator-driven rebuild, the leaf persists
        // checkpoint = -1 and re-activates against a populated WAL.
        // The detector must classify this as a tail replay so the
        // materialiser reads (-1, head] == [0, head] inclusive,
        // covering offset 0 (which the rebuild bug previously
        // skipped under checkpoint = 0).
        var (detector, _) = CreateDetector(head: 50, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_does_not_trigger_WAL_trim_path()
    {
        // The WAL-trim trigger fires when tail > checkpoint AND
        // checkpoint > 0. A checkpoint of -1 must NOT be treated as
        // "trimmed past": the leaf has not applied anything yet, so
        // every offset >= 0 is legitimately in-scope for replay.
        var (detector, _) = CreateDetector(head: 100, tail: 50);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        // Within budget (gap = 101 << default 10 000), so TailReplay.
        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_returns_TailReplay_even_when_gap_exceeds_budget()
    {
        // Reproduces the c2-vi production scenario (silo log 20260526-201857Z):
        // a leaf that has just been created by a split races its own
        // OnActivateAsync against the donor's SetCheckpointOffsetHintAsync.
        // If the activation wins, the sibling reads
        // ProjectionCheckpointOffset = -1 (default) against a shard
        // WAL partition whose head has been pushed past 10 000 by
        // sibling leaves' commits. With the prior logic, gap =
        // head - (-1) blew past MaxLeafReplayEntries and the activation
        // threw LeafProjectionStaleException, taking the leaf offline
        // and cascading into Orleans 'Unable to create local activation'.
        //
        // The corrected contract: the replay-budget trigger only counts
        // entries the leaf could plausibly have applied. The -1
        // sentinel means "nothing applied, nothing in cache to lose",
        // so the entry filter inside the materialiser (per-leaf range
        // check) drops every pre-existence WAL entry on iteration -
        // there is no projection state to recover, so the budget
        // semantically does not apply. The classifier returns
        // TailReplay and the materialiser handles bounding itself via
        // ReplaySliceBudget on the read side.
        var (detector, _) = CreateDetector(head: 50_000, tail: 0);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            MaxLeafReplayEntries = 10,
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_nothing_applied_sentinel_safe_against_trimmed_WAL()
    {
        // The trim trigger is a guard for leaves that previously
        // applied state and would now lose it if the WAL has been
        // trimmed past their checkpoint. A -1 sentinel means the
        // leaf has nothing in cache: there is no projection state
        // to lose, regardless of where the WAL tail sits. The
        // classifier must therefore return TailReplay even when
        // tail > 0, so a freshly-created sibling does not throw
        // LeafProjectionStaleException simply because its sibling
        // partition has been actively trimmed by background
        // compaction (the c2-vi production scenario).
        var (detector, _) = CreateDetector(head: 1000, tail: 100);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public void ClassifyAsync_rejects_checkpoint_below_minus_one()
    {
        // -1 is the lowest legal checkpoint (the "nothing applied"
        // sentinel). Anything lower is a programming error and must
        // surface as ArgumentOutOfRangeException so callers can't
        // silently downgrade to "always tail replay".
        var (detector, _) = CreateDetector(head: 0, tail: 0);

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            var options = await BuildOptionsAsync();
            await detector.ClassifyAsync(
                TreeId, ShardIndex,
                checkpointOffset: -2,
                checkpointAge: TimeSpan.Zero,
                options,
                CancellationToken.None);
        });
    }

    [Test]
    public async Task ClassifyAsync_accepts_zero_checkpoint_without_triggering_WAL_trim()
    {
        // The pre-existing default-0 path must remain a tail replay:
        // a leaf whose in-memory state already covers offset 0 (i.e.
        // the legacy semantics for materialiser-not-yet-engaged
        // grains) is the gate that protects against the off-by-one
        // the rebuild path hit. checkpoint > 0 is the only state
        // where the WAL-trim trigger should fire.
        var (detector, _) = CreateDetector(head: 0, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 0,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_positive_checkpoint_below_tail_triggers_WAL_trim()
    {
        // checkpoint > 0 and tail > checkpoint: WAL has been trimmed
        // past the leaf's last applied offset. Recovery routes via
        // the configured ProjectionRebuildPolicy.
        var (detector, _) = CreateDetector(head: 100, tail: 50);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.SnapshotThenWal,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 25,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.SnapshotThenWal));
    }

    [Test]
    public async Task ClassifyAsync_returns_SnapshotPending_when_checkpoint_inside_margin()
    {
        // head=1000, tail=0, checkpoint=200. Proximity = 200/1000 = 0.20,
        // within the default 0.30 margin and no hard trigger fires
        // (checkpoint within budget, no trim, no age). Advisory must fire.
        var (detector, _) = CreateDetector(head: 1000, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 200,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.SnapshotPending));
    }

    [Test]
    public async Task ClassifyAsync_returns_TailReplay_when_checkpoint_outside_margin()
    {
        // head=1000, tail=0, checkpoint=500. Proximity = 0.50, comfortably
        // outside the 0.30 default margin. Advisory must NOT fire.
        var (detector, _) = CreateDetector(head: 1000, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 500,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_advisory_disabled_when_margin_is_zero()
    {
        // LeafSnapshotMargin = 0.0 opts out of the proactive advisory.
        // Even a checkpoint right at the tail must remain TailReplay
        // (no hard trigger fires since checkpoint is at tail, not below).
        var (detector, _) = CreateDetector(head: 1000, tail: 0);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            LeafSnapshotMargin = 0.0,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_hard_trigger_silences_SnapshotPending()
    {
        // Replay-budget trigger fires (gap exceeds budget) while the WAL is
        // intact. The proximity advisory is silenced, but the decision is the
        // non-fatal over-budget replay - NOT the rebuild policy, which is
        // reserved for genuine loss (#1738).
        var (detector, _) = CreateDetector(head: 1000, tail: 0);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            MaxLeafReplayEntries = 100,
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.SnapshotThenWal,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 200,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        // Gap = 800 > budget 100, but tail(0) has not passed checkpoint+1(201),
        // so every offset the leaf needs is still readable.
        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplayOverBudget));
    }

    // ---------------------------------------------------------------------
    // Issue #1738: a cost trigger must never be fatal while the WAL is intact.
    // ---------------------------------------------------------------------

    [Test]
    public async Task ClassifyAsync_budget_overrun_with_intact_WAL_is_not_fatal()
    {
        // The exact production shape from #1738: repo-context-vector-membership
        // partition 0 had tail=13031, checkpoint=17288, head=27936. The gap
        // (10,648) exceeded the 10,000 default budget by 648 entries, but the
        // WAL still held every offset in (17288, 27936]. The old code mapped
        // this onto ProjectionRebuildPolicy and threw, bricking a tree whose
        // data was completely intact.
        var (detector, _) = CreateDetector(head: 27936, tail: 13031);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            MaxLeafReplayEntries = 10_000,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 17288,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplayOverBudget));
    }

    [Test]
    public async Task ClassifyAsync_budget_overrun_is_not_fatal_under_any_rebuild_policy()
    {
        // The rebuild policy governs genuine loss only. Whichever policy is
        // configured - including the operator-gated Fail - a budget overrun
        // against an intact WAL must stay a replay.
        foreach (var policy in new[]
        {
            ProjectionRebuildPolicy.SnapshotThenWal,
            ProjectionRebuildPolicy.FullRebuildFromWal,
            ProjectionRebuildPolicy.Fail,
        })
        {
            var (detector, _) = CreateDetector(head: 50_000, tail: 0);
            var options = await BuildOptionsAsync(new LatticeOptions
            {
                MaxLeafReplayEntries = 10,
                ProjectionRebuildPolicy = policy,
            });

            var decision = await detector.ClassifyAsync(
                TreeId, ShardIndex,
                checkpointOffset: 100,
                checkpointAge: TimeSpan.Zero,
                options,
                CancellationToken.None);

            Assert.That(
                decision,
                Is.EqualTo(FallOffLogDecision.TailReplayOverBudget),
                $"policy {policy} must not make a budget overrun fatal");
        }
    }

    [Test]
    public async Task ClassifyAsync_age_overrun_with_intact_WAL_is_not_fatal()
    {
        // An old checkpoint does not imply a trimmed WAL. With the log intact
        // the replay converges, so the age trigger degrades to the same
        // non-fatal over-budget replay rather than the rebuild policy.
        var (detector, _) = CreateDetector(head: 1000, tail: 0);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            LeafProjectionRetention = TimeSpan.FromDays(7),
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 500,
            checkpointAge: TimeSpan.FromDays(30),
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplayOverBudget));
    }

    [Test]
    public async Task ClassifyAsync_genuine_loss_still_fails_closed_even_when_budget_also_exceeded()
    {
        // The critical non-regression: relaxing the cost triggers must not
        // weaken the loss guard. Here the WAL HAS been trimmed past the
        // checkpoint (tail 5000 > checkpoint+1 1001), so the leaf would
        // silently rebuild over a lost prefix if this returned a replay.
        // It must still route to the configured rebuild policy.
        var (detector, _) = CreateDetector(head: 50_000, tail: 5_000);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            MaxLeafReplayEntries = 10,
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 1_000,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.Fail));
    }

    [Test]
    public async Task ClassifyAsync_trim_of_only_the_applied_checkpoint_entry_stays_a_replay()
    {
        // The tail == checkpoint + 1 boundary: only the already-applied entry
        // AT the checkpoint was trimmed, so the entire needed window
        // (checkpoint, head] survives. This is the legitimate coverage-gated
        // WAL GC steady state (#919) and must replay cleanly, not throw.
        var (detector, _) = CreateDetector(head: 2_000, tail: 1_001);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 1_000,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_advisory_skipped_for_nothing_applied_sentinel()
    {
        // checkpoint = -1 means "nothing applied yet" - there is no
        // cache content to snapshot, so the advisory must not fire.
        var (detector, _) = CreateDetector(head: 1000, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }
}
