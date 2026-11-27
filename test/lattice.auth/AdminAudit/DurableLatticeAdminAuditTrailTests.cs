using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth.Tests.AdminAudit;

/// <summary>
/// Unit tests for <see cref="DurableLatticeAdminAuditTrail"/> over in-memory trees
/// (#3638): the intent records the authorized actor and the anchor's base version and
/// fails closed, a denial is one final record, and the finalize is exactly once,
/// points at the anchor revision, and never claims a commit the anchor disproves.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class DurableLatticeAdminAuditTrailTests
{
    [Test]
    public async Task BeginAsync_writes_a_pending_intent_attributing_the_resolved_actor_and_the_anchor_base()
    {
        var harness = new AdminAuditHarness(subjectId: "alice", region: "eu");
        var baseVersion = harness.Anchor.Put(AdminAuditHarness.AnchorKey, [1]);

        var intent = await harness.Trail().BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);

        var stored = harness.Audit.Records()[AdminAuditKeys.Pending(intent.OperationId)];
        Assert.Multiple(() =>
        {
            Assert.That(stored.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Pending));
            Assert.That(stored.ActorSubjectId, Is.EqualTo("alice"));
            Assert.That(stored.ActorKind, Is.EqualTo(LatticeAdminAuditActorKind.Subject));
            Assert.That(stored.Action, Is.EqualTo(LatticeAdminAuditAction.RuleUpsert));
            Assert.That(stored.AnchorTreeId, Is.EqualTo(AdminAuditHarness.AnchorTree));
            Assert.That(stored.AnchorBaseVersion, Is.EqualTo(baseVersion));
            Assert.That(stored.OriginRegion, Is.EqualTo("eu"));
            Assert.That(stored.Detail!["effect"], Is.EqualTo("Allow"));
        });
    }

    [Test]
    public async Task BeginAsync_marks_a_bootstrap_administrator()
    {
        var harness = new AdminAuditHarness(subjectId: "root");
        harness.Options.BootstrapAdministrators.Add("root");

        var intent = await harness.Trail().BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);

        Assert.That(intent.ActorKind, Is.EqualTo(LatticeAdminAuditActorKind.BootstrapAdministrator));
    }

    [Test]
    public void BeginAsync_fails_closed_when_the_intent_cannot_be_written()
    {
        var harness = new AdminAuditHarness();
        harness.Audit.FailWrites = true;

        Assert.ThrowsAsync<LatticeAdminAuditUnavailableException>(async () =>
            await harness.Trail().BeginAsync(AdminAuditHarness.Request(), CancellationToken.None));
    }

    [Test]
    public async Task RecordDeniedAsync_writes_one_final_denied_record_and_swallows_a_failed_write()
    {
        var harness = new AdminAuditHarness(subjectId: "mallory");
        var trail = harness.Trail();

        await trail.RecordDeniedAsync(AdminAuditHarness.Request(), LatticeAdminAuditReasonCodes.DeniedNotAuthorized, CancellationToken.None);
        harness.Audit.FailWrites = true;
        await trail.RecordDeniedAsync(AdminAuditHarness.Request(), LatticeAdminAuditReasonCodes.DeniedNotAuthorized, CancellationToken.None);

        var record = harness.Audit.Records().Single();
        Assert.Multiple(() =>
        {
            Assert.That(record.Key, Does.StartWith("cluster/"));
            Assert.That(record.Value.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Denied));
            Assert.That(record.Value.ActorSubjectId, Is.EqualTo("mallory"));
            Assert.That(record.Value.ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.DeniedNotAuthorized));
        });
    }

    [Test]
    public async Task CompleteAsync_committed_moves_the_intent_to_its_final_key_and_points_at_the_new_revision()
    {
        var harness = new AdminAuditHarness();
        harness.Anchor.Put(AdminAuditHarness.AnchorKey, [1]);
        var trail = harness.Trail();
        var intent = await trail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);
        var committed = harness.Anchor.Put(AdminAuditHarness.AnchorKey, [2]);

        await trail.CompleteAsync(intent, LatticeAdminAuditCompletion.Committed);

        var record = harness.Audit.Records().Single();
        Assert.Multiple(() =>
        {
            Assert.That(record.Key, Is.EqualTo(AdminAuditKeys.Final(intent)));
            Assert.That(record.Value.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Committed));
            Assert.That(record.Value.CommittedRevisionHlc, Is.EqualTo(committed));
            Assert.That(record.Value.AnchorValueHash, Is.EqualTo(AdminAuditAnchorReader.Hash([2])));
            Assert.That(record.Value.CompletedUtc, Is.Not.Null);
        });
    }

    [Test]
    public async Task CompleteAsync_no_change_is_recorded_when_nothing_wrote_the_anchor()
    {
        var harness = new AdminAuditHarness();
        harness.Anchor.Put(AdminAuditHarness.AnchorKey, [1]);
        var trail = harness.Trail();
        var intent = await trail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);

        await trail.CompleteAsync(intent, LatticeAdminAuditCompletion.NoChange);

        Assert.That(harness.Audit.Records().Single().Value.ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.FailedNoChange));
    }

    [Test]
    public async Task Trace1_a_committed_write_whose_anchor_a_concurrent_delete_removed_stays_committed()
    {
        // F9 trace 1: UpsertGroup commits, then a replicated or concurrent delete of the
        // same key lands before the finalize. Absent before and absent after must not
        // turn the call's own reported commit into "no change".
        var harness = new AdminAuditHarness();
        var trail = harness.Trail();
        var intent = await trail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);
        harness.Anchor.Put(AdminAuditHarness.AnchorKey, [1]);
        await harness.Anchor.Lattice.DeleteAsync(AdminAuditHarness.AnchorKey);

        await trail.CompleteAsync(intent, LatticeAdminAuditCompletion.Committed);

        Assert.That(harness.Audit.Records().Single().Value.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Committed));
    }

    [Test]
    public async Task Trace2_a_removal_pre_empted_by_a_concurrent_delete_is_indeterminate_not_committed()
    {
        // F9 trace 2: the store reports that this call removed nothing, because a
        // concurrent delete got there first. The anchor moved, but not by this call.
        var harness = new AdminAuditHarness();
        harness.Anchor.Put(AdminAuditHarness.AnchorKey, [1]);
        var trail = harness.Trail();
        var intent = await trail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);
        await harness.Anchor.Lattice.DeleteAsync(AdminAuditHarness.AnchorKey);

        await trail.CompleteAsync(intent, LatticeAdminAuditCompletion.NoChange);

        var record = harness.Audit.Records().Single().Value;
        Assert.Multiple(() =>
        {
            Assert.That(record.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Indeterminate));
            Assert.That(record.ActorSubjectId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public async Task CompleteAsync_no_change_on_an_absent_anchor_is_indeterminate_while_the_history_view_cannot_prove_it()
    {
        // Absent before and after: only the history tells a write-then-delete from no
        // write, and a lagging history view cannot prove an absence at finalize time.
        var harness = new AdminAuditHarness();
        var trail = harness.Trail();
        var intent = await trail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);

        await trail.CompleteAsync(intent, LatticeAdminAuditCompletion.NoChange);
        var viaView = harness.Audit.Records().Single().Value.Outcome;

        var walHarness = new AdminAuditHarness();
        walHarness.Anchor.History = new EntryHistoryPage { Source = EntryHistorySource.WalWindow };
        var walTrail = walHarness.Trail();
        var walIntent = await walTrail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);
        await walTrail.CompleteAsync(walIntent, LatticeAdminAuditCompletion.NoChange);

        Assert.Multiple(() =>
        {
            Assert.That(viaView, Is.EqualTo(LatticeAdminAuditOutcome.Indeterminate));
            Assert.That(walHarness.Audit.Records().Single().Value.ReasonCode, Is.EqualTo(LatticeAdminAuditReasonCodes.FailedNoChange),
                "an untruncated write-ahead-log window proves that nothing wrote the key");
        });
    }

    [Test]
    public async Task RecordDeniedAsync_bounds_caller_supplied_target_and_detail_text()
    {
        var harness = new AdminAuditHarness(subjectId: "mallory");
        var huge = new string('x', 4 * 1024 * 1024);
        var request = AdminAuditHarness.Request() with
        {
            TargetId = huge,
            Detail = Enumerable.Range(0, 50).ToDictionary(i => "k" + i, _ => huge),
        };

        await harness.Trail().RecordDeniedAsync(request, LatticeAdminAuditReasonCodes.DeniedNotAuthorized, CancellationToken.None);

        var record = harness.Audit.Records().Single();
        Assert.Multiple(() =>
        {
            Assert.That(record.Value.TargetId!.Length, Is.LessThanOrEqualTo(AdminAuditBounds.MaxLength));
            Assert.That(record.Value.TargetId, Does.Contain(AdminAuditBounds.TruncationMarker).And.Contain("len=4194304"));
            Assert.That(record.Value.Detail, Has.Count.EqualTo(AdminAuditBounds.MaxDetailEntries));
            Assert.That(record.Value.Detail!.Values, Has.All.Length.LessThanOrEqualTo(AdminAuditBounds.MaxLength));
            Assert.That(harness.Audit.Rows.Single().Value.Value.Length, Is.LessThan(16 * 1024), "the stored record stays small");
        });
    }

    [Test]
    public void Bound_keeps_short_values_and_marks_long_ones_with_their_length_and_hash()
    {
        var shortValue = new string('a', AdminAuditBounds.MaxLength);
        var longA = new string('a', AdminAuditBounds.MaxLength + 1);
        var longB = new string('a', AdminAuditBounds.MaxLength) + "b";

        Assert.Multiple(() =>
        {
            Assert.That(AdminAuditBounds.Bound(shortValue), Is.SameAs(shortValue));
            Assert.That(AdminAuditBounds.Bound((string?)null), Is.Null);
            Assert.That(AdminAuditBounds.Bound(longA), Is.Not.EqualTo(AdminAuditBounds.Bound(longB)), "the hash keeps distinct values distinct");
        });
    }

    [Test]
    public async Task CompleteAsync_pending_leaves_the_intent_for_the_reconciler()
    {
        var harness = new AdminAuditHarness();
        var trail = harness.Trail();
        var intent = await trail.BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);

        await trail.CompleteAsync(intent, new LatticeAdminAuditCompletion(LatticeAdminAuditOutcome.Pending, null));

        Assert.That(harness.Audit.Records().Keys, Is.EqualTo(new[] { AdminAuditKeys.Pending(intent.OperationId) }));
    }

    [Test]
    public async Task WriteFinalAsync_admits_exactly_one_final_record_per_intent()
    {
        var harness = new AdminAuditHarness();
        var intent = await harness.Trail().BeginAsync(AdminAuditHarness.Request(), CancellationToken.None);
        var first = intent with { Outcome = LatticeAdminAuditOutcome.Committed };
        var second = intent with { Outcome = LatticeAdminAuditOutcome.Indeterminate };

        var wroteFirst = await AdminAuditStore.WriteFinalAsync(harness.GrainFactory, first, CancellationToken.None);
        var wroteSecond = await AdminAuditStore.WriteFinalAsync(harness.GrainFactory, second, CancellationToken.None);

        var records = harness.Audit.Records();
        Assert.Multiple(() =>
        {
            Assert.That(wroteFirst, Is.True);
            Assert.That(wroteSecond, Is.False);
            Assert.That(records.Single().Value.Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Committed));
        });
    }

    [Test]
    public void TryReadTicks_reads_a_final_key_and_rejects_a_pending_key()
    {
        var record = new LatticeAdminAuditRecord
        {
            OperationId = "op",
            ActorSubjectId = "a",
            TargetScope = "t/acme",
            TimestampUtc = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
        };

        Assert.Multiple(() =>
        {
            Assert.That(AdminAuditKeys.TryReadTicks(AdminAuditKeys.Final(record), out var ticks), Is.True);
            Assert.That(ticks, Is.EqualTo(record.TimestampUtc.UtcTicks));
            Assert.That(AdminAuditKeys.TryReadTicks(AdminAuditKeys.Pending("op"), out _), Is.False);
            Assert.That(AdminAuditKeys.Final(record), Does.StartWith(AdminAuditKeys.ScopeStart("t/acme")));
        });
    }
}
