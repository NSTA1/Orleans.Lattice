using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="TypedGrainIndexEnroller{TGrain, TState}"/>: the four-step
/// enrolment shape - read the baseline, plan, record the intent, apply and
/// confirm - and what each step leaves behind when the next one fails.
/// </summary>
[TestFixture]
public sealed class TypedGrainIndexEnrollerTests
{
    private const string GrainKey = "alice";

    private static IndexedTestState StateOf(int age = 30, string country = "GB") =>
        new() { Age = age, Country = country };

    [Test]
    public void The_enroller_reports_the_declarations_index_name_and_the_configured_mode()
    {
        var enroller = EnrollmentTestIndex.Enroller(
            new RecordingEnrollmentStore(),
            mode: GrainIndexProjectionMode.Eventual);

        Assert.Multiple(() =>
        {
            Assert.That(enroller.IndexName, Is.EqualTo(EnrollmentTestIndex.IndexName));
            Assert.That(enroller.Mode, Is.EqualTo(GrainIndexProjectionMode.Eventual));
        });
    }

    [Test]
    public void The_mode_is_captured_at_construction_so_a_write_never_re_reads_options()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());

        Assert.That(enroller.Mode, Is.EqualTo(GrainIndexProjectionMode.Synchronous),
            "Reading the mode per write would put an options lookup on the hottest path in the "
            + "package, and would leave already-activated grains on a stale path anyway.");
    }

    [Test]
    public void An_index_applies_only_to_a_grain_implementing_the_interface_it_was_declared_over()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());

        Assert.Multiple(() =>
        {
            Assert.That(enroller.AppliesTo(EnrollmentTestIndex.GrainInstance()), Is.True);
            Assert.That(enroller.AppliesTo(Substitute.For<ITestGuidKeyedGrain>()), Is.False,
                "Several indexes can share a state type, so the grain interface is what tells them "
                + "apart.");
            Assert.That(enroller.AppliesTo(null), Is.False);
            Assert.That(enroller.AppliesTo("not a grain"), Is.False);
        });
    }

    [Test]
    public void A_grain_identity_is_encoded_by_the_definitions_own_codec()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());

        Assert.That(enroller.EncodeKey(EnrollmentTestIndex.Identity(GrainKey)), Is.EqualTo(GrainKey));
    }

    [Test]
    public void An_un_encodable_identity_is_reported_rather_than_silently_skipped()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());

        Assert.That(
            () => enroller.EncodeKey(default),
            Throws.TypeOf<GrainIndexKeyEncodingException>(),
            "A grain that cannot be keyed must fail loudly: quietly leaving it untracked is the "
            + "invisible drift this whole path exists to prevent.");
    }

    [Test]
    public async Task A_grain_that_was_never_enrolled_has_no_baseline()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());

        Assert.That(await enroller.ReadBaselineAsync(GrainKey, CancellationToken.None), Is.Null,
            "A first enrolment has to be distinguishable from an unchanged one, or the seen marker "
            + "would never be written.");
    }

    [Test]
    public async Task An_enrolled_grains_baseline_is_the_projection_the_index_holds()
    {
        var store = new RecordingEnrollmentStore();
        var projection = EnrollmentTestIndex.Project(GrainKey, StateOf());
        store.SeedEnrollment(EnrollmentTestIndex.IndexName, GrainKey, projection);

        var baseline = await EnrollmentTestIndex.Enroller(store)
            .ReadBaselineAsync(GrainKey, CancellationToken.None);

        Assert.That(baseline!.Entries, Is.EqualTo(projection.Entries));
    }

    [Test]
    public void Planning_unchanged_state_against_its_own_projection_yields_an_empty_plan()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());
        var state = StateOf();
        var baseline = EnrollmentTestIndex.Project(GrainKey, state);

        Assert.That(enroller.Plan(baseline, GrainKey, state).IsEmpty, Is.True,
            "This is what makes re-projecting on every activation free.");
    }

    [Test]
    public void Planning_a_moved_value_produces_both_the_upsert_and_the_tombstone()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());
        var baseline = EnrollmentTestIndex.Project(GrainKey, StateOf(17));

        var plan = enroller.Plan(baseline, GrainKey, StateOf(18));

        Assert.Multiple(() =>
        {
            Assert.That(plan.Upserts, Has.Count.EqualTo(1));
            Assert.That(plan.Deletes, Has.Count.EqualTo(1));
            Assert.That(plan.Deletes[0], Is.EqualTo(GrainIndexKeyEncoder.EncodeKey("Age", 17, GrainKey)));
        });
    }

    [Test]
    public async Task Beginning_a_write_records_the_plan_before_anything_is_applied()
    {
        var store = new RecordingEnrollmentStore();
        var enroller = EnrollmentTestIndex.Enroller(store);
        var plan = enroller.Plan(GrainIndexProjection.Empty(GrainKey), GrainKey, StateOf());

        var pending = await enroller.BeginAsync(plan, GrainKey, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pending.IndexName, Is.EqualTo(EnrollmentTestIndex.IndexName));
            Assert.That(pending.GrainKey, Is.EqualTo(GrainKey));
            Assert.That(pending.Plan, Is.SameAs(plan));
            Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.True);
            Assert.That(store.Log, Is.EqualTo(new[] { $"pending:{EnrollmentTestIndex.IndexName}/{GrainKey}" }));
        });
    }

    [Test]
    public async Task Each_recorded_write_gets_its_own_idempotency_key()
    {
        var store = new RecordingEnrollmentStore();
        var enroller = EnrollmentTestIndex.Enroller(store);
        var plan = enroller.Plan(GrainIndexProjection.Empty(GrainKey), GrainKey, StateOf());

        var first = await enroller.BeginAsync(plan, GrainKey, CancellationToken.None);
        var second = await enroller.BeginAsync(plan, GrainKey, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.OperationId, Is.Not.EqualTo(second.OperationId),
                "A content-derived key would make a grain that returns to a value it held before "
                + "re-attach to that earlier batch and silently do nothing.");
            Assert.That(first.OperationId, Does.Not.Contain("/"),
                "The atomic-batch seam reserves '/' as its grain-key separator.");
        });
    }

    [Test]
    public async Task Committing_applies_the_batch_and_then_confirms_it()
    {
        var store = new RecordingEnrollmentStore();
        var tree = EnrollmentTrees.Accepting();
        var enroller = EnrollmentTestIndex.Enroller(store, tree);
        var plan = enroller.Plan(GrainIndexProjection.Empty(GrainKey), GrainKey, StateOf());
        var pending = await enroller.BeginAsync(plan, GrainKey, CancellationToken.None);

        await enroller.CommitAsync(pending, CancellationToken.None);

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            pending.OperationId,
            Arg.Any<CancellationToken>());

        Assert.Multiple(() =>
        {
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.True);
            Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.False);
            Assert.That(
                store.Log,
                Is.EqualTo(new[]
                {
                    $"pending:{EnrollmentTestIndex.IndexName}/{GrainKey}",
                    $"complete:{EnrollmentTestIndex.IndexName}/{GrainKey}",
                }),
                "The outbox entry may only be cleared after the batch has committed.");
        });
    }

    [Test]
    public async Task A_failed_batch_leaves_the_outbox_entry_untouched()
    {
        var store = new RecordingEnrollmentStore();
        var enroller = EnrollmentTestIndex.Enroller(store, EnrollmentTrees.Faulting());
        var plan = enroller.Plan(GrainIndexProjection.Empty(GrainKey), GrainKey, StateOf());
        var pending = await enroller.BeginAsync(plan, GrainKey, CancellationToken.None);

        Assert.That(
            async () => await enroller.CommitAsync(pending, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.True,
                "The entry is the only durable record that the write is owed.");
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.False,
                "Marking the grain enrolled before its entries exist would have the backfill skip "
                + "a grain that is not in the index.");
        });
    }

    [Test]
    public async Task Committing_a_plan_with_tombstones_uses_the_mixed_atomic_batch()
    {
        var store = new RecordingEnrollmentStore();
        var tree = EnrollmentTrees.Accepting();
        var enroller = EnrollmentTestIndex.Enroller(store, tree);
        var baseline = EnrollmentTestIndex.Project(GrainKey, StateOf(17));
        var plan = enroller.Plan(baseline, GrainKey, StateOf(18));
        var pending = await enroller.BeginAsync(plan, GrainKey, CancellationToken.None);

        await enroller.CommitAsync(pending, CancellationToken.None);

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<IReadOnlyList<string>>(),
            pending.OperationId,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Marking_a_grain_enrolled_records_the_projection_without_an_index_write()
    {
        var store = new RecordingEnrollmentStore();
        var tree = EnrollmentTrees.Accepting();
        var enroller = EnrollmentTestIndex.Enroller(store, tree);

        await enroller.MarkEnrolledAsync(
            GrainKey,
            GrainIndexProjection.Empty(GrainKey),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.True,
                "A grain contributing no entries still has to be marked, or the backfill revisits "
                + "it on every pass.");
            Assert.That(tree.ReceivedCalls(), Is.Empty);
        });
    }

    [Test]
    public async Task Withdrawing_clears_both_the_marker_and_the_outbox_entry()
    {
        var store = new RecordingEnrollmentStore();
        var enroller = EnrollmentTestIndex.Enroller(store);
        var plan = enroller.Plan(GrainIndexProjection.Empty(GrainKey), GrainKey, StateOf());
        await enroller.BeginAsync(plan, GrainKey, CancellationToken.None);
        await enroller.MarkEnrolledAsync(GrainKey, plan.Projection, CancellationToken.None);

        await enroller.WithdrawAsync(GrainKey, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.False);
            Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.False);
        });
    }

    [Test]
    public void A_null_argument_is_rejected_at_construction()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new TypedGrainIndexEnroller<ITestStringKeyedGrain, IndexedTestState>(
                    null!,
                    EnrollmentTrees.Accepting(),
                    new RecordingEnrollmentStore(),
                    GrainIndexProjectionMode.Synchronous),
                Throws.ArgumentNullException);
            Assert.That(
                () => new TypedGrainIndexEnroller<ITestStringKeyedGrain, IndexedTestState>(
                    EnrollmentTestIndex.Definition(),
                    null!,
                    new RecordingEnrollmentStore(),
                    GrainIndexProjectionMode.Synchronous),
                Throws.ArgumentNullException);
            Assert.That(
                () => new TypedGrainIndexEnroller<ITestStringKeyedGrain, IndexedTestState>(
                    EnrollmentTestIndex.Definition(),
                    EnrollmentTrees.Accepting(),
                    null!,
                    GrainIndexProjectionMode.Synchronous),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_null_argument_is_rejected_on_the_recording_and_applying_steps()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());
        var plan = enroller.Plan(GrainIndexProjection.Empty(GrainKey), GrainKey, StateOf());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await enroller.BeginAsync(null!, GrainKey, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await enroller.BeginAsync(plan, null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await enroller.CommitAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }
}
