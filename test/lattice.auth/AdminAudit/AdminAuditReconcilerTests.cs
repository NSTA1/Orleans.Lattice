using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth.Tests.AdminAudit;

/// <summary>
/// Unit tests for the admin audit reconciler and its evidence rule
/// (<see cref="AdminAuditReconciler"/>, <see cref="AdminAuditEvidence"/>, #3638): an
/// intent left pending is resolved from its anchor without ever claiming a commit -
/// not-committed only when the anchor proves no local write, indeterminate when a local
/// write exists or the evidence stays incomplete - and only by the region that wrote it.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AdminAuditReconcilerTests
{
    private static async Task<(AdminAuditHarness Harness, LatticeAdminAuditRecord Intent)> PendingAsync(
        string region = "", bool anchorExists = true)
    {
        var harness = new AdminAuditHarness(region: region);
        if (anchorExists)
        {
            harness.Anchor.Put(AdminAuditHarness.AnchorKey, [1]);
        }

        var intent = await harness.Trail().BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);
        return (harness, intent);
    }

    private static LatticeAdminAuditRecord Final(AdminAuditHarness harness) =>
        harness.Audit.Records().Single(r => !r.Key.StartsWith(AdminAuditKeys.PendingPrefix, StringComparison.Ordinal)).Value;

    [Test]
    public async Task Sweep_resolves_a_stale_intent_whose_anchor_never_moved_as_not_committed()
    {
        var (harness, _) = await PendingAsync();
        harness.Time.Advance(harness.Options.AdminAuditReconcileLease);

        var finalized = await harness.Reconciler().SweepAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(finalized, Is.EqualTo(1));
            Assert.That(harness.Audit.Records(), Has.Count.EqualTo(1), "the intent is removed");
            Assert.That(Final(harness).Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Failed));
            Assert.That(Final(harness).ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.FailedNotCommitted));
        });
    }

    [Test]
    public async Task Sweep_leaves_an_intent_younger_than_the_lease()
    {
        var (harness, intent) = await PendingAsync();

        var finalized = await harness.Reconciler().SweepAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(finalized, Is.Zero);
            Assert.That(harness.Audit.Records().Keys, Is.EqualTo(new[] { AdminAuditKeys.Pending(intent.OperationId) }));
        });
    }

    [Test]
    public async Task Sweep_never_resolves_an_intent_another_region_wrote()
    {
        var (harness, intent) = await PendingAsync(region: "us");
        harness.Replication.LocalReplicaId.Returns("eu");
        harness.Time.Advance(harness.Options.AdminAuditMaxPendingAge);

        var finalized = await harness.Reconciler().SweepAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(finalized, Is.Zero);
            Assert.That(harness.Audit.Records().Keys, Is.EqualTo(new[] { AdminAuditKeys.Pending(intent.OperationId) }));
        });
    }

    [Test]
    public async Task Sweep_records_indeterminate_not_committed_when_a_local_write_moved_the_anchor()
    {
        var (harness, _) = await PendingAsync();
        var landed = harness.Anchor.Put(AdminAuditHarness.AnchorKey, [2]);
        harness.Anchor.History = new EntryHistoryPage
        {
            Source = EntryHistorySource.View,
            Revisions = [new EntryRevision { Hlc = landed, SourceKey = AdminAuditHarness.AnchorKey, OriginClusterId = null }],
        };
        harness.Time.Advance(harness.Options.AdminAuditReconcileLease);

        await harness.Reconciler().SweepAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Final(harness).Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Indeterminate));
            Assert.That(Final(harness).ActorSubjectId, Is.EqualTo("alice"), "an indeterminate record still attributes the actor");
        });
    }

    [Test]
    public async Task Sweep_records_not_committed_when_complete_history_holds_only_replicated_writes()
    {
        var (harness, _) = await PendingAsync();
        var replicated = harness.Anchor.Put(AdminAuditHarness.AnchorKey, [3]);
        harness.Anchor.History = new EntryHistoryPage
        {
            Source = EntryHistorySource.View,
            Revisions = [new EntryRevision { Hlc = replicated, SourceKey = AdminAuditHarness.AnchorKey, OriginClusterId = "us" }],
        };
        harness.Time.Advance(harness.Options.AdminAuditReconcileLease);

        await harness.Reconciler().SweepAsync(CancellationToken.None);

        Assert.That(Final(harness).ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.FailedNotCommitted));
    }

    [Test]
    public async Task Sweep_waits_on_incomplete_history_then_records_indeterminate_at_the_max_pending_age()
    {
        var (harness, intent) = await PendingAsync();
        harness.Anchor.Put(AdminAuditHarness.AnchorKey, [4]);
        harness.Anchor.History = new EntryHistoryPage { Source = EntryHistorySource.View };
        var reconciler = harness.Reconciler();

        harness.Time.Advance(harness.Options.AdminAuditReconcileLease);
        await reconciler.SweepAsync(CancellationToken.None);
        var stillPending = harness.Audit.Records().ContainsKey(AdminAuditKeys.Pending(intent.OperationId));

        harness.Time.Advance(harness.Options.AdminAuditMaxPendingAge);
        await reconciler.SweepAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stillPending, Is.True, "a lagging history view proves nothing");
            Assert.That(Final(harness).Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Indeterminate));
        });
    }

    [Test]
    public async Task Sweep_only_removes_the_intent_when_the_caller_already_finalized_it()
    {
        var (harness, intent) = await PendingAsync();
        await AdminAuditStore.WriteFinalAsync(
            harness.GrainFactory, intent with { Outcome = LatticeAdminAuditOutcome.Committed }, CancellationToken.None);
        harness.Audit.Put(AdminAuditKeys.Pending(intent.OperationId), AdminAuditStore.Serialize(intent));
        harness.Time.Advance(harness.Options.AdminAuditReconcileLease);

        var finalized = await harness.Reconciler().SweepAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(finalized, Is.Zero);
            Assert.That(harness.Audit.Records().Single().Value.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Committed));
        });
    }

    [Test]
    public async Task Sweep_ages_out_final_records_past_the_time_to_live_and_keeps_pending_intents()
    {
        var harness = new AdminAuditHarness();
        harness.Options.AdminAuditTrailTimeToLive = TimeSpan.FromDays(1);
        var trail = harness.Trail();
        await trail.RecordDeniedAsync(AdminAuditHarness.Request(), LatticeAdminAuditReasonCodes.DeniedNotAuthorized, CancellationToken.None);
        harness.Time.Advance(TimeSpan.FromDays(2));
        var intent = await trail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);

        await harness.Reconciler().SweepAsync(CancellationToken.None);

        Assert.That(harness.Audit.Records().Keys, Is.EqualTo(new[] { AdminAuditKeys.Pending(intent.OperationId) }));
    }

    [Test]
    public async Task Evidence_for_a_tree_without_versions_proves_no_write_only_from_an_unchanged_value()
    {
        var harness = new AdminAuditHarness();
        harness.SystemAnchor.GetAsync("alias", Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>([7]));
        var anchor = new LatticeAdminAuditAnchor(LatticeConstants.RegistryTreeId, "alias");
        var intent = await harness.Trail().BeginAsync(AdminAuditHarness.Request(anchor), CancellationToken.None);

        var unchanged = await AdminAuditEvidence.ResolveAsync(harness.GrainFactory, intent, TimeSpan.FromMinutes(5), TimeSpan.FromHours(1), CancellationToken.None);
        harness.SystemAnchor.GetAsync("alias", Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>([8]));
        var changed = await AdminAuditEvidence.ResolveAsync(harness.GrainFactory, intent, TimeSpan.FromMinutes(5), TimeSpan.FromHours(1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unchanged!.Value.ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.FailedNotCommitted));
            Assert.That(changed!.Value.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Indeterminate));
        });
    }
}
