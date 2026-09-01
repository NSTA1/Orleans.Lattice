using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="IndexedPersistentState{TState}"/>: the activation hook, the
/// ordering of a tracked write, what a failure at each step leaves behind, and
/// the pass-through behaviour of a grain no index tracks.
/// </summary>
[TestFixture]
public sealed class IndexedPersistentStateTests
{
    private const string GrainKey = "alice";

    private static IndexedTestState StateOf(int age = 30, string country = "GB") =>
        new() { Age = age, Country = country };

    private static IndexedStateHarness<IndexedTestState> Harness(
        RecordingPersistentState<IndexedTestState> inner,
        params GrainIndexEnroller<IndexedTestState>[] enrollers) =>
        new(inner, GrainKey, EnrollmentTestIndex.GrainInstance(), enrollers);

    [Test]
    public async Task A_grain_no_index_tracks_passes_straight_through()
    {
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner);

        await harness.ActivateAsync();
        await harness.Indexed.WriteStateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Indexed.Enrollers, Is.Empty);
            Assert.That(inner.WriteCount, Is.EqualTo(1),
                "An untracked grain must pay nothing beyond the extra indirection.");
        });
    }

    [Test]
    public async Task The_state_object_forwards_the_inner_states_own_properties()
    {
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(41), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore()));
        await harness.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Indexed.State.Age, Is.EqualTo(41));
            Assert.That(harness.Indexed.RecordExists, Is.True);
            Assert.That(harness.Indexed.Etag, Is.EqualTo(inner.Etag));
        });

        var replacement = StateOf(42);
        harness.Indexed.State = replacement;
        Assert.That(inner.State, Is.SameAs(replacement));
    }

    [Test]
    public async Task Activating_a_grain_with_stored_state_enrols_it()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));

        await harness.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.True);
            Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.False);
            Assert.That(harness.Indexed.Slots[0].GrainKey, Is.EqualTo(GrainKey));
        });
    }

    [Test]
    public async Task Activating_a_grain_with_nothing_stored_files_no_entries()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf());
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));

        await harness.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.False,
                "Projecting a default state would file an entry for a grain that exists only "
                + "because somebody called it.");
            Assert.That(store.Log, Is.EqualTo(new[] { $"read:{EnrollmentTestIndex.IndexName}/{GrainKey}" }));
        });
    }

    [Test]
    public async Task Re_activating_an_unchanged_grain_writes_nothing_at_all()
    {
        var store = new RecordingEnrollmentStore();
        var state = StateOf();
        store.SeedEnrollment(
            EnrollmentTestIndex.IndexName,
            GrainKey,
            EnrollmentTestIndex.Project(GrainKey, state));

        var tree = EnrollmentTrees.Accepting();
        var inner = new RecordingPersistentState<IndexedTestState>(state, recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store, tree));

        await harness.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tree.ReceivedCalls(), Is.Empty,
                "An unchanged re-projection must never reach the tree, or every activation churns "
                + "the index.");
            Assert.That(store.Log, Is.EqualTo(new[] { $"read:{EnrollmentTestIndex.IndexName}/{GrainKey}" }),
                "The baseline read is the only registry traffic an idempotent re-activation may cost.");
        });
    }

    [Test]
    public async Task An_activation_whose_index_write_fails_still_activates()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store, EnrollmentTrees.Faulting()));

        Assert.That(async () => await harness.ActivateAsync(), Throws.Nothing,
            "Failing the activation would turn an index outage into a grain outage.");

        Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.True,
            "The write is still owed, so the record of it has to survive.");
    }

    [Test]
    public async Task An_unreadable_baseline_degrades_to_a_full_re_projection_rather_than_failing()
    {
        var store = new RecordingEnrollmentStore { ReadFault = new InvalidOperationException("registry down") };
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));

        Assert.That(async () => await harness.ActivateAsync(), Throws.Nothing,
            "Failing the activation would make an index-registry blip take the grain down.");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Indexed.Slots[0].Confirmed.Entries, Has.Count.EqualTo(2),
                "Losing the baseline is safe, only wasteful: the grain re-writes every entry it "
                + "already owns rather than diffing against what the index holds.");
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.True);
        });

        await Task.CompletedTask;
    }

    [Test]
    public async Task A_tracked_write_records_the_batch_before_committing_the_state_and_confirms_it_after()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));
        await harness.ActivateAsync();
        store.Log.Clear();

        harness.Indexed.State.Age = 31;
        await harness.Indexed.WriteStateAsync(CancellationToken.None);

        Assert.That(
            store.Log,
            Is.EqualTo(new[]
            {
                $"pending:{EnrollmentTestIndex.IndexName}/{GrainKey}",
                $"complete:{EnrollmentTestIndex.IndexName}/{GrainKey}",
            }),
            "Recording the intent has to precede the state commit, or a silo that stops in between "
            + "leaves a grain whose state and index disagree with nothing to say so.");
    }

    [Test]
    public async Task A_write_that_moves_nothing_the_index_projects_costs_no_registry_traffic()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));
        await harness.ActivateAsync();
        store.Log.Clear();

        harness.Indexed.State.Secret = "not projected";
        await harness.Indexed.WriteStateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.Log, Is.Empty,
                "A property no index projects must not drag the outbox onto the write path.");
            Assert.That(inner.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_failed_index_write_surfaces_but_leaves_the_grains_own_state_committed()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store, EnrollmentTrees.Faulting()));
        await harness.ActivateAsync();
        inner.State.Age = 32;

        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(inner.WriteCount, Is.EqualTo(1),
                "The state is committed before the index batch is attempted, so an index fault can "
                + "neither roll it back nor corrupt it.");
            Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.True);
        });
    }

    [Test]
    public async Task A_write_that_cannot_be_recorded_fails_before_the_state_is_committed()
    {
        var store = new RecordingEnrollmentStore
        {
            WritePendingFault = new InvalidOperationException("registry down"),
        };
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));
        await harness.ActivateAsync();
        inner.State.Age = 33;

        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(inner.WriteCount, Is.Zero,
            "Failing before the state commit is the safer failure: nothing has diverged, so there "
            + "is nothing to reconcile.");
    }

    [Test]
    public async Task A_write_after_a_failed_one_subsumes_it_rather_than_assuming_it_landed()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(17), recordExists: true);
        var faulting = EnrollmentTestIndex.Enroller(store, EnrollmentTrees.Faulting());
        var harness = Harness(inner, faulting);
        await harness.ActivateAsync();

        inner.State.Age = 18;
        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        inner.State.Age = 19;
        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        var pending = store.Pending[$"{EnrollmentTestIndex.IndexName}/{GrainKey}"];

        Assert.That(
            pending.Plan.Upserts.Select(u => u.Key),
            Does.Contain(GrainIndexKeyEncoder.EncodeKey("Age", 19, GrainKey)),
            "Because plans are diffed against the last confirmed projection, the newer entry "
            + "replaces the older one and covers strictly more than it did.");
    }

    [Test]
    public async Task A_write_that_projects_nothing_drops_the_previous_attempts_outbox_entry()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store, EnrollmentTrees.Faulting()));
        await harness.ActivateAsync();

        inner.State.Age = 34;
        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(harness.Indexed.Slots[0].Pending, Is.Not.Null);

        // A state the index cannot project at all must not leave the previous
        // attempt's batch attached to this write, or the commit step would
        // re-apply a batch this write never planned.
        inner.State = null!;
        await harness.Indexed.WriteStateAsync(CancellationToken.None);

        Assert.That(harness.Indexed.Slots[0].Pending, Is.Null);
    }

    [Test]
    public async Task An_eventual_write_records_the_batch_and_leaves_it_for_the_drain()
    {
        var store = new RecordingEnrollmentStore();
        var tree = EnrollmentTrees.Accepting();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(
            inner,
            EnrollmentTestIndex.Enroller(store, tree, GrainIndexProjectionMode.Eventual));
        await harness.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tree.ReceivedCalls(), Is.Empty,
                "Eventual mode defers the index batch, which is the whole point of choosing it.");
            Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.True,
                "Deferred is not lost: the batch is durable before the write returns.");
        });
    }

    [Test]
    public async Task An_eventual_write_never_advances_the_confirmed_baseline()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(17), recordExists: true);
        var harness = Harness(
            inner,
            EnrollmentTestIndex.Enroller(store, EnrollmentTrees.Accepting(), GrainIndexProjectionMode.Eventual));
        await harness.ActivateAsync();

        inner.State.Age = 18;
        await harness.Indexed.WriteStateAsync(CancellationToken.None);

        Assert.That(harness.Indexed.Slots[0].Confirmed.Entries, Is.Empty,
            "Until the drain confirms the batch, the next plan has to subsume it rather than "
            + "assume it landed.");
    }

    [Test]
    public async Task Clearing_the_state_withdraws_the_entries_and_the_marker()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));
        await harness.ActivateAsync();
        store.Log.Clear();

        await harness.Indexed.ClearStateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(inner.ClearCount, Is.EqualTo(1));
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.False,
                "Dropping the marker is what lets a later backfill reconsider the grain.");
            Assert.That(store.Log, Does.Contain($"withdraw:{EnrollmentTestIndex.IndexName}/{GrainKey}"));
            Assert.That(harness.Indexed.Slots[0].Confirmed.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task Clearing_an_untracked_grains_state_passes_straight_through()
    {
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner);
        await harness.ActivateAsync();

        await harness.Indexed.ClearStateAsync(CancellationToken.None);

        Assert.That(inner.ClearCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Re_reading_the_state_reconciles_the_index_against_what_storage_holds()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));
        await harness.ActivateAsync();
        store.Log.Clear();

        inner.State.Age = 35;
        await harness.Indexed.ReadStateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(inner.ReadCount, Is.EqualTo(1));
            Assert.That(
                store.Log,
                Is.EqualTo(new[]
                {
                    $"pending:{EnrollmentTestIndex.IndexName}/{GrainKey}",
                    $"complete:{EnrollmentTestIndex.IndexName}/{GrainKey}",
                }),
                "A re-read can bring in another silo's change, so the index is reconciled against it.");
        });
    }

    [Test]
    public async Task A_refresh_whose_index_write_fails_does_not_fail_the_read()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store, EnrollmentTrees.Faulting()));
        await harness.ActivateAsync();

        inner.State.Age = 36;
        Assert.That(
            async () => await harness.Indexed.ReadStateAsync(CancellationToken.None),
            Throws.Nothing,
            "A read is not a mutation, so it does not surface an index fault - the outbox entry is.");

        Assert.That(store.HasPending(EnrollmentTestIndex.IndexName, GrainKey), Is.True);
    }

    [Test]
    public async Task Several_indexes_over_one_state_are_each_kept_in_step()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(
            inner,
            EnrollmentTestIndex.Enroller(store),
            new TypedGrainIndexEnroller<ITestStringKeyedGrain, IndexedTestState>(
                EnrollmentTestIndex.Definition("Second"),
                EnrollmentTrees.Accepting(),
                store,
                GrainIndexProjectionMode.Synchronous));

        await harness.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.IsEnrolled(EnrollmentTestIndex.IndexName, GrainKey), Is.True);
            Assert.That(store.IsEnrolled("Second", GrainKey), Is.True);
        });
    }

    [Test]
    public async Task The_parameterless_overloads_behave_as_the_cancellable_ones_do()
    {
        var store = new RecordingEnrollmentStore();
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));
        await harness.ActivateAsync();

        await harness.Indexed.ReadStateAsync();
        harness.Indexed.State.Age = 37;
        await harness.Indexed.WriteStateAsync();
        await harness.Indexed.ClearStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(inner.ReadCount, Is.EqualTo(1));
            Assert.That(inner.WriteCount, Is.EqualTo(1));
            Assert.That(inner.ClearCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_activation_that_cannot_encode_the_grain_key_fails_loudly()
    {
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore()));

        // Orleans rejects an empty primary key outright, so an un-encodable
        // identity is modelled by the default one the codec also refuses.
        harness.Context.GrainId.Returns(default(Orleans.Runtime.GrainId));

        Assert.That(
            async () => await harness.ActivateAsync(),
            Throws.TypeOf<GrainIndexKeyEncodingException>(),
            "An un-keyable grain is a declaration error, not a transient fault; leaving it quietly "
            + "untracked is exactly the invisible drift this path prevents.");

        await Task.CompletedTask;
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf());
        var context = Substitute.For<Orleans.Runtime.IGrainContext>();
        var set = new GrainIndexEnrollmentSet<IndexedTestState>([]);

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new IndexedPersistentState<IndexedTestState>(
                    null!, context, set, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new IndexedPersistentState<IndexedTestState>(
                    inner, null!, set, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new IndexedPersistentState<IndexedTestState>(
                    inner, context, null!, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new IndexedPersistentState<IndexedTestState>(inner, context, set, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Participating_rejects_a_null_lifecycle()
    {
        var harness = Harness(new RecordingPersistentState<IndexedTestState>(StateOf()));

        Assert.That(() => harness.Indexed.Participate(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Stopping_the_activation_does_nothing()
    {
        var harness = Harness(new RecordingPersistentState<IndexedTestState>(StateOf()));

        Assert.That(async () => await harness.Indexed.OnStop(CancellationToken.None), Throws.Nothing);
        await Task.CompletedTask;
    }

    [Test]
    public async Task Read_state_on_an_untracked_grain_delegates_and_returns_immediately()
    {
        // Line 224: _enrollers.Length == 0 -> return immediately after inner read.
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(), recordExists: true);
        var harness = Harness(inner); // no enrollers

        await harness.Indexed.ReadStateAsync(CancellationToken.None);

        Assert.That(inner.ReadCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Read_state_that_cannot_record_the_batch_does_not_surface_the_failure()
    {
        // Lines 234-240: PlanAsync throws (WritePendingAsync faults) but
        // ReadStateAsync catches and logs rather than propagating - a read
        // must not fail the grain activation over an index outage.
        var store = new RecordingEnrollmentStore
        {
            WritePendingFault = new InvalidOperationException("storage down"),
        };
        var inner = new RecordingPersistentState<IndexedTestState>(StateOf(age: 30), recordExists: true);
        var harness = Harness(inner, EnrollmentTestIndex.Enroller(store));
        await harness.ActivateAsync();

        // Mutate so the next plan produces a non-empty diff, then fault the batch.
        inner.State.Age = 99;

        Assert.That(
            async () => await harness.Indexed.ReadStateAsync(CancellationToken.None),
            Throws.Nothing,
            "A plan failure during a read must be swallowed; the outbox entry already recorded is what converges it.");
    }
}
