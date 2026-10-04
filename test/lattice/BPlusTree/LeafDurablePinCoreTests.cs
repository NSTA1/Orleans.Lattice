using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="LeafDurablePinCore"/>, the rule behind every durable
/// materialiser pin a leaf publishes. Each arm is pinned on its own, and the
/// invariants the WAL durability model relies on are checked exhaustively over a
/// small input grid.
/// </summary>
[TestFixture]
public sealed class LeafDurablePinCoreTests
{
    private static LeafDurablePinDecision Resolve(
        long current = -1,
        long persisted = -1,
        long covered = -1,
        bool liveData = false,
        bool walEmpty = false,
        bool releaseNeverWritten = false) =>
        LeafDurablePinCore.Resolve(current, persisted, covered, liveData, walEmpty, releaseNeverWritten);

    [Test]
    public void An_empty_partition_with_nothing_applied_releases_with_its_sentinel_checkpoint()
    {
        Assert.That(Resolve(current: -1, liveData: false), Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseEmpty, -1)));
    }

    [Test]
    public void A_durably_checkpointed_partition_is_coverage_gated_however_empty_its_cache_looks()
    {
        Assert.That(
            Resolve(current: 4, persisted: 4, covered: -1, liveData: false),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)),
            "an empty cache must not release a partition whose durable checkpoint says its prefix was applied");
    }

    [Test]
    public void Live_never_checkpointed_rows_over_a_proven_empty_wal_release_the_block_issue_3103()
    {
        Assert.That(
            Resolve(current: -1, liveData: true, walEmpty: true),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseEmpty, -1)));
    }

    [Test]
    public void Live_never_checkpointed_rows_over_an_unproven_wal_keep_the_block()
    {
        Assert.That(
            Resolve(current: -1, liveData: true, walEmpty: false),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)));
    }

    [Test]
    public void A_never_written_scanned_through_partition_releases_its_persisted_checkpoint_issue_3453()
    {
        Assert.That(
            Resolve(current: 7, persisted: 5, covered: 6, liveData: false, releaseNeverWritten: true),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseNeverWritten, 5)),
            "the release carries the PERSISTED checkpoint, never the pending one");
    }

    /// <summary>
    /// The never-written condition the grain's flush paths opt in on and the one
    /// <see cref="LeafDurablePinCore.Resolve"/> takes its release arm on are the
    /// same predicate (issue #4433): no live row, and a persisted checkpoint
    /// of at least 0.
    /// </summary>
    [Test]
    public void The_never_written_predicate_is_the_one_the_release_arm_takes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafDurablePinCore.IsNeverWrittenScannedThrough(hasLiveData: false, persistedCheckpoint: 0), Is.True);
            Assert.That(LeafDurablePinCore.IsNeverWrittenScannedThrough(hasLiveData: false, persistedCheckpoint: 5), Is.True);
            Assert.That(LeafDurablePinCore.IsNeverWrittenScannedThrough(hasLiveData: false, persistedCheckpoint: -1), Is.False);
            Assert.That(LeafDurablePinCore.IsNeverWrittenScannedThrough(hasLiveData: true, persistedCheckpoint: 5), Is.False);
        });

        for (var persisted = -1L; persisted <= 3; persisted++)
        {
            foreach (var liveData in new[] { false, true })
            {
                var released = Resolve(current: 3, persisted: persisted, covered: 3, liveData: liveData, releaseNeverWritten: true).Kind
                    == LeafDurablePinKind.ReleaseNeverWritten;
                Assert.That(
                    released,
                    Is.EqualTo(LeafDurablePinCore.IsNeverWrittenScannedThrough(liveData, persisted)),
                    $"persisted {persisted}, live data {liveData}: the release arm and the predicate disagree");
            }
        }
    }

    [Test]
    public void The_never_written_release_needs_the_caller_to_opt_in()
    {
        Assert.That(
            Resolve(current: 5, persisted: 5, covered: -1, liveData: false, releaseNeverWritten: false),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)));
    }

    [Test]
    public void The_never_written_release_never_applies_to_a_partition_holding_rows()
    {
        Assert.That(
            Resolve(current: 5, persisted: 5, covered: -1, liveData: true, releaseNeverWritten: true),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)));
    }

    /// <summary>
    /// Issue #4456: a never-written leaf's release is bounded by the snapshot
    /// coverage it holds, so the GC never trims past what a rehydrate of that
    /// snapshot restarts from. Issue #4523: with no coverage there is no release
    /// at all - the partition keeps the block - because a snapshot created later
    /// (a cold rebuild's capture or #2280 bank) can land below any release
    /// published without one, and the pin store can never take it back.
    /// </summary>
    [Test]
    public void The_never_written_release_is_bounded_by_snapshot_coverage_issue_4456()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Resolve(current: 2, persisted: 2, covered: 1, liveData: false, releaseNeverWritten: true),
                Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseNeverWritten, 1)),
                "coverage below the persisted checkpoint bounds the release");
            Assert.That(
                Resolve(current: 2, persisted: 2, covered: 5, liveData: false, releaseNeverWritten: true),
                Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseNeverWritten, 2)),
                "coverage above it leaves the persisted checkpoint as the bound");
            Assert.That(
                Resolve(current: 2, persisted: 2, covered: 0, liveData: false, releaseNeverWritten: true),
                Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseNeverWritten, 0)),
                "coverage of offset 0 is real coverage and releases through it");
        });
    }

    /// <summary>
    /// Issue #4523: a never-written partition with no durable snapshot coverage
    /// keeps the block, however far its persisted checkpoint has scanned.
    /// </summary>
    [Test]
    public void The_never_written_release_needs_durable_snapshot_coverage_issue_4523()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Resolve(current: 2, persisted: 2, covered: -1, liveData: false, releaseNeverWritten: true),
                Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)),
                "no snapshot: a release at the persisted 2 could later stand above a cold capture's coverage");
            Assert.That(
                Resolve(current: 7, persisted: 5, covered: -1, liveData: false, releaseNeverWritten: true),
                Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)),
                "a pending advance changes nothing");
        });
    }

    [Test]
    public void A_covered_partition_is_entitled_to_the_lower_of_its_persisted_checkpoint_and_coverage()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Resolve(current: 9, persisted: 9, covered: 4, liveData: true),
                Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Covered, 4)));
            Assert.That(
                Resolve(current: 3, persisted: 3, covered: 8, liveData: true),
                Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Covered, 3)));
        });
    }

    [Test]
    public void A_pending_checkpoint_never_reaches_the_pin_issue_3476()
    {
        Assert.That(
            Resolve(current: 5, persisted: 2, covered: 5, liveData: true),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Covered, 2)),
            "coverage restamped over a pending advance must not lift the pin above the persisted checkpoint");
    }

    [Test]
    public void A_checkpointed_but_uncovered_partition_keeps_the_block()
    {
        Assert.That(
            Resolve(current: 3, persisted: 3, covered: -1, liveData: true),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)));
    }

    [Test]
    public void A_pending_first_checkpoint_over_a_persisted_sentinel_keeps_the_block()
    {
        Assert.That(
            Resolve(current: 3, persisted: -1, covered: 3, liveData: true),
            Is.EqualTo(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1)));
    }

    [Test]
    public void Only_the_block_and_the_never_written_release_carry_a_zero_frontier()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new LeafDurablePinDecision(LeafDurablePinKind.Block, -1).HasZeroFrontier, Is.True);
            Assert.That(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseNeverWritten, 3).HasZeroFrontier, Is.True);
            Assert.That(new LeafDurablePinDecision(LeafDurablePinKind.ReleaseEmpty, -1).HasZeroFrontier, Is.False);
            Assert.That(new LeafDurablePinDecision(LeafDurablePinKind.Covered, 3).HasZeroFrontier, Is.False);
        });
    }

    /// <summary>
    /// The invariants the WAL durability model's PublishPin relies on, over every
    /// input in a small grid: a trim entitlement never exceeds the persisted
    /// checkpoint, a covered entitlement never exceeds the coverage either, the
    /// block carries no offset, and a release carries no positive offset unless
    /// it is the never-written one, which needs durable coverage and is bounded
    /// by it (issues #4456 and #4523).
    /// </summary>
    [Test]
    public void Every_trim_entitlement_is_bounded_by_the_persisted_checkpoint_over_the_whole_grid()
    {
        var values = new long[] { -1, 0, 1, 2, 3 };
        var checkedCount = 0;
        Assert.Multiple(() =>
        {
            foreach (var current in values)
            foreach (var persisted in values)
            foreach (var covered in values)
            foreach (var liveData in new[] { false, true })
            foreach (var walEmpty in new[] { false, true })
            foreach (var release in new[] { false, true })
            {
                if (current < persisted)
                {
                    continue; // current is max(persisted, pending), never below persisted
                }

                checkedCount++;
                var d = LeafDurablePinCore.Resolve(current, persisted, covered, liveData, walEmpty, release);
                var label = $"current={current} persisted={persisted} covered={covered} live={liveData} empty={walEmpty} release={release}";
                switch (d.Kind)
                {
                    case LeafDurablePinKind.Covered:
                        Assert.That(d.Offset, Is.GreaterThanOrEqualTo(0).And.LessThanOrEqualTo(persisted).And.LessThanOrEqualTo(covered), label);
                        break;
                    case LeafDurablePinKind.ReleaseNeverWritten:
                        Assert.That(covered, Is.GreaterThanOrEqualTo(0), label + ": a release needs durable coverage (#4523)");
                        Assert.That(d.Offset, Is.EqualTo(Math.Min(persisted, covered)).And.GreaterThanOrEqualTo(0), label);
                        Assert.That(liveData || !release, Is.False, label);
                        break;
                    case LeafDurablePinKind.Block:
                        Assert.That(d.Offset, Is.EqualTo(-1), label);
                        break;
                    case LeafDurablePinKind.ReleaseEmpty:
                        Assert.That(d.Offset, Is.LessThan(0), label);
                        break;
                }
            }
        });

        Assert.That(checkedCount, Is.GreaterThan(500), "the grid must not be vacuous");
    }
}
