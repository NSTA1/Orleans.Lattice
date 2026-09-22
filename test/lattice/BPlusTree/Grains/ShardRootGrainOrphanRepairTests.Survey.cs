using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOrphanRepairTests
{
    [Test]
    public async Task Survey_counts_every_miss_on_two_orphans_and_is_repeatable_and_read_only()
    {
        var h = CreateHarness(
            orphanKeys: ["b1", "b2", "b3", "b4", "b5", "b6"],
            liveKeys: ["b1", "b3", "b5"]);
        var secondId = GrainId.Create("leaf", "second-orphan");
        var second = Substitute.For<IBPlusLeafGrain>();
        h.Leaves.Add(secondId, second);
        h.SetProbe(h.LeafB, h.Probes[h.LeafB] with { NextSibling = secondId });
        h.SetProbe(h.LeafC, h.Probes[h.LeafC] with { PrevSibling = secondId });
        second.GetReclaimProbeAsync().Returns(new LeafReclaimProbe
        {
            LowKeyInclusive = "b2", HighKeyExclusive = "c",
            PrevSibling = h.LeafB, NextSibling = h.LeafC, LiveRowCount = 3,
        });
        second.GetKeysAsync().Returns(new List<string> { "b2", "b3", "b4" });

        var first = await h.Grain.SurveyOrphanedLeavesAsync(null);
        var repeated = await h.Grain.SurveyOrphanedLeavesAsync(null);

        Assert.That(first.Findings, Has.Count.EqualTo(2));
        Assert.That(repeated.Findings, Is.EqualTo(first.Findings));
        Assert.Multiple(() =>
        {
            Assert.That(first.Gaps, Is.Empty);
            Assert.That(first.ResumeFromInclusive, Is.Null);
            Assert.That(first.Findings.Select(f => f.LeafId),
                Is.EqualTo(new[] { h.LeafB.ToString(), secondId.ToString() }));
            Assert.That(first.Findings.Select(f => f.SurveyMissingKeyCount), Is.EqualTo(new[] { 3, 2 }));
            Assert.That(first.Findings.Select(f => f.SurveyVerifiedKeyCount), Is.EqualTo(new[] { 3, 1 }));
            Assert.That(first.Findings.Select(f => f.SurveyRoutingContradictionKeyCount), Is.EqualTo(new[] { 0, 0 }));
            Assert.That(first.Findings.Select(f => f.VerifiedKeyCount), Is.EqualTo(new[] { 1, 0 }));
            Assert.That(first.Findings.Select(f => f.UnverifiedKey), Is.EqualTo(new[] { "b2", "b2" }));
            Assert.That(first.Findings.All(f => f.Disposition == OrphanedLeafDisposition.RefusedUnverifiedKeys), Is.True);
        });
        await h.A.Received(2).GetAsync("b6");
        foreach (var leaf in h.Leaves.Values)
        {
            await leaf.DidNotReceive().TryBeginOrphanRetirementAsync();
            await leaf.DidNotReceive().ClearGrainStateAsync();
            await leaf.DidNotReceive().TryUnlinkSuccessorAsync(
                Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>());
            await leaf.DidNotReceive().SetPrevSiblingAsync(Arg.Any<GrainId?>());
        }
    }

    [Test]
    public async Task Default_audit_and_repair_stop_at_first_miss_and_leave_later_keys_untested()
    {
        var h = CreateHarness(orphanKeys: ["b1", "b2", "b3", "b4"], liveKeys: ["b1", "b3"]);
        foreach (var dryRun in new[] { true, false })
        {
            var page = await h.Grain.RepairOrphanedLeavesAsync(null, dryRun);
            Assert.Multiple(() =>
            {
                Assert.That(page.Findings[0].VerifiedKeyCount, Is.EqualTo(1));
                Assert.That(page.Findings[0].UnverifiedKey, Is.EqualTo("b2"));
                Assert.That(page.Findings[0].SurveyMissingKeyCount, Is.Null);
                Assert.That(page.Findings[0].SurveyVerifiedKeyCount, Is.Null);
                Assert.That(page.Findings[0].SurveyRoutingContradictionKeyCount, Is.Null);
            });
        }
        await h.A.DidNotReceive().GetAsync("b3");
        await h.A.DidNotReceive().GetAsync("b4");
    }

    [Test]
    public async Task Survey_healthy_chain_has_zero_orphans_on_repeated_runs()
    {
        var h = CreateHarness();
        h.ChildIds.Insert(1, h.LeafB);
        h.Separators.Insert(1, "b");
        for (var pass = 0; pass < 2; pass++)
        {
            var page = await h.Grain.SurveyOrphanedLeavesAsync(null);
            Assert.That(page.Findings.Count, Is.Zero);
            Assert.That(page.Gaps, Is.Empty);
            Assert.That(page.LeavesWalked, Is.Positive);
            Assert.That(page.ResumeFromInclusive, Is.Null);
        }
    }

    [Test]
    public async Task Survey_fully_duplicated_orphan_has_explicit_zero_damage_and_same_repair_verdict()
    {
        var h = CreateHarness();
        var survey = await h.Grain.SurveyOrphanedLeavesAsync(null);
        var audit = await InspectAsync(h);
        Assert.Multiple(() =>
        {
            Assert.That(survey.Findings[0].SurveyMissingKeyCount, Is.Zero);
            Assert.That(survey.Findings[0].SurveyRoutingContradictionKeyCount, Is.Zero);
            Assert.That(survey.Findings[0].SurveyVerifiedKeyCount, Is.EqualTo(2));
            Assert.That(survey.Findings[0].Disposition, Is.EqualTo(audit.Findings[0].Disposition));
            Assert.That(survey.Findings[0].VerifiedKeyCount, Is.EqualTo(audit.Findings[0].VerifiedKeyCount));
        });
    }

    [Test]
    public async Task Survey_counts_routing_contradictions_separately_and_preserves_first_failure()
    {
        var h = CreateHarness(orphanKeys: ["b1", "b2", "b3", "b4"], liveKeys: ["b1"]);
        h.ChildIds.Insert(1, h.LeafB);
        h.Separators.Insert(1, "b3");

        var page = await h.Grain.SurveyOrphanedLeavesAsync(null);
        var finding = page.Findings.Single();
        Assert.Multiple(() =>
        {
            Assert.That(finding.SurveyVerifiedKeyCount, Is.EqualTo(1));
            Assert.That(finding.SurveyMissingKeyCount, Is.EqualTo(1));
            Assert.That(finding.SurveyRoutingContradictionKeyCount, Is.EqualTo(2));
            Assert.That(finding.VerifiedKeyCount, Is.EqualTo(1));
            Assert.That(finding.UnverifiedKey, Is.EqualTo("b2"));
            Assert.That(finding.Disposition, Is.EqualTo(OrphanedLeafDisposition.RefusedUnverifiedKeys));
        });
        await h.B.DidNotReceive().GetAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Survey_respects_key_budget_and_does_not_publish_a_false_zero()
    {
        var h = CreateHarness(orphanKeys: Enumerable.Range(0, 100_001).Select(i => $"b{i:D6}").ToArray());
        var page = await h.Grain.SurveyOrphanedLeavesAsync(null);
        Assert.That(page.Findings.Single().Disposition, Is.EqualTo(OrphanedLeafDisposition.RefusedKeyCountExceeded));
        Assert.That(page.Findings.Single().SurveyMissingKeyCount, Is.Null);
        await h.A.DidNotReceive().GetAsync(Arg.Any<string>());
    }

    [Test]
    public void Survey_observes_cancellation()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.SurveyOrphanedLeavesAsync(null, new CancellationToken(true)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Survey_blocked_orphan_is_unknown_but_empty_orphan_has_explicit_zero_counts()
    {
        var h = CreateHarness(orphanKeys: []);
        var empty = (await h.Grain.SurveyOrphanedLeavesAsync(null)).Findings.Single();
        Assert.That(empty.SurveyMissingKeyCount, Is.Zero);
        Assert.That(empty.SurveyVerifiedKeyCount, Is.Zero);
        Assert.That(empty.SurveyRoutingContradictionKeyCount, Is.Zero);
        h.SetProbe(h.LeafB, h.Probes[h.LeafB] with { HasBlockingState = true });
        var blocked = (await h.Grain.SurveyOrphanedLeavesAsync(null)).Findings.Single();
        Assert.That(blocked.Disposition, Is.EqualTo(OrphanedLeafDisposition.RefusedBlockingState));
        Assert.That(blocked.SurveyMissingKeyCount, Is.Null);
        Assert.That(blocked.SurveyVerifiedKeyCount, Is.Null);
        Assert.That(blocked.SurveyRoutingContradictionKeyCount, Is.Null);
    }
}
