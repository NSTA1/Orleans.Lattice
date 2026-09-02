namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Pins how a compacted replica and an un-upgraded one behave against each other
/// during an active-active rollout of the dot-compaction fix (issue #1932).
/// <para>
/// These exist because the rollout guidance has to be precise. The risk is
/// narrower than "the clusters diverge": the replicated <b>state</b> still
/// converges, because a merge is still a union of dot sets. What can differ
/// mid-upgrade is only how the two builds <b>read</b> that state, and only for a
/// slot that was both re-asserted and then retracted.
/// </para>
/// </summary>
/// <remarks>
/// Marked <see cref="NonParallelizableAttribute"/> because one test toggles the
/// process-wide compaction gate. NUnit does not parallelise without an explicit
/// opt-in, so this is belt and braces against a future assembly-level
/// <c>[Parallelizable]</c> silently making a neighbouring fixture observe the
/// suppressed gate.
/// </remarks>
[TestFixture]
[NonParallelizable]
public class OrFlagMixedVersionRolloutTests
{
    /// <summary>
    /// How a build predating the fix decides liveness: exact dot equality
    /// against the tombstone list, with no coverage rule.
    /// </summary>
    private static bool IsEnabledTheOldWay(OrFlag flag)
    {
        foreach (var dot in flag.Enables)
        {
            var tombstoned = false;
            foreach (var tomb in flag.Tombstones)
            {
                if (tomb.Counter == dot.Counter
                    && string.Equals(tomb.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
                {
                    tombstoned = true;
                    break;
                }
            }

            if (!tombstoned)
            {
                return true;
            }
        }

        return false;
    }

    private static void EnableAsAccessorWould(OrFlag flag, string replicaId)
    {
        long max = 0;
        foreach (var dot in flag.Enables)
        {
            if (dot.ReplicaId == replicaId && dot.Counter > max) max = dot.Counter;
        }

        foreach (var dot in flag.Tombstones)
        {
            if (dot.ReplicaId == replicaId && dot.Counter > max) max = dot.Counter;
        }

        flag.Enable(replicaId, max + 1);
    }

    [Test]
    public void On_data_that_was_never_compacted_both_predicates_agree()
    {
        // The reassuring half, and the reason the cancellation change is safe to
        // roll out on its own: a retraction tombstones every live dot it
        // observed, so a replica never ends up holding a tombstone for one of its
        // own later dots without also holding one for the earlier dot it
        // supersedes. The coverage rule therefore has nothing extra to cancel.
        foreach (var enables in new[] { 1, 2, 5 })
        {
            var flag = new OrFlag();
            for (var i = 0; i < enables; i++)
            {
                EnableAsAccessorWould(flag, "A");
            }

            Assert.That(IsEnabledTheOldWay(flag), Is.EqualTo(flag.IsEnabled),
                $"enabled {enables} time(s), not retracted");

            flag.Disable();
            Assert.That(IsEnabledTheOldWay(flag), Is.EqualTo(flag.IsEnabled),
                $"enabled {enables} time(s), then retracted");
        }
    }

    [Test]
    public void An_upgraded_replica_reads_correctly_whether_or_not_it_holds_the_superseded_dot()
    {
        // The property that makes the mixed-version window self-correcting: the
        // coverage rule gives the same answer regardless of whether the older dot
        // is present. So an old peer echoing the dot back never changes what an
        // upgraded replica computes, and nothing needs repairing after upgrade.
        var compacted = new OrFlag();
        EnableAsAccessorWould(compacted, "A");
        EnableAsAccessorWould(compacted, "A");
        compacted.Disable();

        var withStaleDotEchoedBack = compacted.Clone();
        withStaleDotEchoedBack.Enables.Add(new OrSetDot { ReplicaId = "A", Counter = 1 });

        Assert.Multiple(() =>
        {
            Assert.That(compacted.IsEnabled, Is.False);
            Assert.That(withStaleDotEchoedBack.IsEnabled, Is.False,
                "The superseded dot arriving back from an un-upgraded peer is still covered by the "
                + "retraction, so an upgraded replica's answer is unchanged.");
        });
    }

    [Test]
    public void The_opt_out_switch_makes_an_upgraded_replica_retain_dots_like_the_old_build()
    {
        // The lever that removes the need for a synchronised upgrade: with it
        // set, an upgraded node keeps the same dot history the old build would,
        // so the two read the same slot identically.
        var wasDisabled = OrSetDotCompaction.CompactionDisabled;
        try
        {
            OrSetDotCompaction.CompactionDisabled = true;

            var flag = new OrFlag();
            for (var i = 0; i < 20; i++)
            {
                EnableAsAccessorWould(flag, "A");
            }

            flag.Disable();

            Assert.Multiple(() =>
            {
                Assert.That(flag.Enables, Has.Count.EqualTo(20),
                    "Suppressed compaction retains every dot, matching an un-upgraded peer exactly.");
                Assert.That(IsEnabledTheOldWay(flag), Is.EqualTo(flag.IsEnabled),
                    "So both builds read the slot the same way and no upgrade ordering is required.");
                Assert.That(flag.IsEnabled, Is.False);
            });
        }
        finally
        {
            OrSetDotCompaction.CompactionDisabled = wasDisabled;
        }
    }

    [Test]
    public void Compaction_is_on_by_default_so_the_fix_needs_no_opt_in()
    {
        Assert.That(OrSetDotCompaction.CompactionDisabled, Is.False,
            "A single-cluster deployment - the reported case - gets the fix without configuring anything.");
    }

    /// <summary>
    /// How a build predating the fix merges: a plain union of both dot lists,
    /// with no compaction. Simulated explicitly because merging through the new
    /// type would compact, which is precisely what an un-upgraded peer does not
    /// do.
    /// </summary>
    private static void MergeTheOldWay(OrFlag target, OrFlag source)
    {
        foreach (var dot in source.Enables)
        {
            if (!target.Enables.Contains(dot)) target.Enables.Add(dot);
        }

        foreach (var dot in source.Tombstones)
        {
            if (!target.Tombstones.Contains(dot)) target.Tombstones.Add(dot);
        }
    }

    [Test]
    public void The_state_still_converges_even_while_the_two_builds_read_it_differently()
    {
        // Worst case: a slot re-asserted (so a dot was compacted away) and then
        // retracted, held by a build that has not been upgraded yet.
        var upgraded = new OrFlag();
        EnableAsAccessorWould(upgraded, "A");
        var oldPeer = upgraded.Clone();

        EnableAsAccessorWould(upgraded, "A");
        upgraded.Disable();

        // The un-upgraded peer merges without compacting, so it keeps the
        // superseded dot the upgraded replica dropped.
        MergeTheOldWay(oldPeer, upgraded);

        Assert.Multiple(() =>
        {
            Assert.That(upgraded.IsEnabled, Is.False, "The upgraded replica reads the correct value.");
            Assert.That(IsEnabledTheOldWay(oldPeer), Is.True,
                "The un-upgraded peer reads its retained superseded dot as still live - the "
                + "documented, transient rollout anomaly.");

            // The point: this is a read-side disagreement, not a divergence of
            // stored state, and it evaporates on upgrade with no repair step -
            // the same bytes already read correctly under the new predicate.
            Assert.That(oldPeer.IsEnabled, Is.False,
                "Reading that same peer's state with the new predicate already yields the correct "
                + "value, so upgrading the peer resolves it without touching the data.");
        });
    }
}
