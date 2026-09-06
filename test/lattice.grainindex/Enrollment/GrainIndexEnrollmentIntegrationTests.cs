using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// End-to-end coverage of the activation and mutation enrolment path against a
/// real silo, real grains, a real index tree, and the real registry tree.
/// </summary>
/// <remarks>
/// <para>
/// The acceptance for this path is inherently integration-level: it is about
/// what a query can see after a grain activates, mutates, re-activates, or
/// fails, and none of that is observable without a tree behind it.
/// </para>
/// <para>
/// Nothing here waits on wall-clock time. The outbox drain is disabled on the
/// fixture and invoked explicitly, so "the fault clears and the write converges"
/// is a sequence of awaited calls rather than a sleep.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class GrainIndexEnrollmentIntegrationTests
{
    private readonly GrainIndexEnrollmentClusterFixture _fixture = new();
    private ServiceProvider _serializerProvider = null!;
    private OrleansGrainIndexSerializer<GrainIndexEnrollmentRecord> _enrollmentSerializer = null!;
    private OrleansGrainIndexSerializer<GrainIndexPendingProjection> _pendingSerializer = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await _fixture.InitializeAsync();

        var services = new ServiceCollection();
        services.AddSerializer();
        _serializerProvider = services.BuildServiceProvider();
        _enrollmentSerializer = new OrleansGrainIndexSerializer<GrainIndexEnrollmentRecord>(
            _serializerProvider.GetRequiredService<Serializer<GrainIndexEnrollmentRecord>>());
        _pendingSerializer = new OrleansGrainIndexSerializer<GrainIndexPendingProjection>(
            _serializerProvider.GetRequiredService<Serializer<GrainIndexPendingProjection>>());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _serializerProvider.Dispose();
        await _fixture.DisposeAsync();
    }

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILattice IndexTree(string indexName) =>
        Grains.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex(indexName));

    private ILattice RegistryTree() =>
        Grains.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);

    private IGrainIndexEnrollmentStore Store() =>
        new GrainIndexEnrollmentStore(Grains, _enrollmentSerializer, _pendingSerializer);

    /// <summary>
    /// A drain built fresh over the live cluster, which is what a silo that has
    /// just restarted brings to an outbox it did not write.
    /// </summary>
    private GrainIndexOutboxDrainer Drainer()
    {
        var services = new ServiceCollection();
        services.AddOptions();
        services.Configure<GrainIndexOptions>(
            GrainIndexEnrollmentClusterFixture.UsersIndex,
            options => options.TreeName =
                GrainIndexTreeNames.ForIndex(GrainIndexEnrollmentClusterFixture.UsersIndex));
        services.Configure<GrainIndexOptions>(
            GrainIndexEnrollmentClusterFixture.EventualIndex,
            options => options.TreeName =
                GrainIndexTreeNames.ForIndex(GrainIndexEnrollmentClusterFixture.EventualIndex));

        var provider = services.BuildServiceProvider();
        return new GrainIndexOutboxDrainer(
            Store(),
            Grains,
            provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>(),
            NullLogger<GrainIndexOutboxDrainer>.Instance);
    }

    /// <summary>The grain keys an index holds for one value of one property.</summary>
    private async Task<List<string>> GrainsAtAsync<TValue>(string indexName, string property, TValue value)
    {
        var encoded = GrainIndexKeyEncoder.EncodeValue(value);
        var keys = await IndexTree(indexName)
            .KeysAsync(
                GrainIndexKeyEncoder.ValueRangeStartInclusive(property, encoded),
                GrainIndexKeyEncoder.ValueRangeEndExclusive(property, encoded),
                cancellationToken: CancellationToken.None)
            .ToListAsync();

        var grains = new List<string>();
        foreach (var key in keys)
        {
            if (GrainIndexKeyEncoder.TryParseKey(key, out _, out _, out var grainKey))
                grains.Add(grainKey);
        }

        return grains;
    }

    private async Task<bool> HasSeenMarkerAsync(string indexName, string grainKey)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await RegistryTree()
                .ExistsAsync(GrainIndexRegistryKeys.Seen(indexName, grainKey), CancellationToken.None);
        }
    }

    private async Task<bool> HasPendingAsync(string indexName, string grainKey)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await RegistryTree()
                .ExistsAsync(GrainIndexRegistryKeys.Pending(indexName, grainKey), CancellationToken.None);
        }
    }

    [Test]
    public async Task Activating_a_grain_with_existing_state_enrols_it_in_the_index()
    {
        const string Key = "activation-enrol";
        await Grains.GetGrain<IIndexedUserGrain>(Key).SetAsync(31, "GB");

        // A fresh activation of the same grain must find itself already enrolled
        // and leave the index exactly as it was.
        await Grains.GetGrain<IIndexedUserGrain>(Key).DeactivateAsync();
        Assert.That(await Grains.GetGrain<IIndexedUserGrain>(Key).GetAgeAsync(), Is.EqualTo(31));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 31),
                Does.Contain(Key),
                "A tracked grain must be discoverable by a projected property as soon as it has state.");
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Country", "GB"),
                Does.Contain(Key));
        });
    }

    [Test]
    public async Task The_grain_is_marked_seen_so_a_backfill_can_skip_it()
    {
        const string Key = "seen-marker";
        await Grains.GetGrain<IIndexedUserGrain>(Key).SetAsync(20, "FR");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await HasSeenMarkerAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, Key), Is.True,
                "The backfill skips enrolled grains by scanning these markers, so the activation "
                + "path has to leave one.");
            Assert.That(
                await HasPendingAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, Key), Is.False,
                "A confirmed write clears its own outbox entry, or the drain would re-apply it forever.");
        });
    }

    [Test]
    public async Task Mutating_state_tombstones_the_old_value_and_publishes_the_new_one()
    {
        const string Key = "mutation-update";
        var grain = Grains.GetGrain<IIndexedUserGrain>(Key);
        await grain.SetAsync(17, "GB");

        Assert.That(
            await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 17),
            Does.Contain(Key),
            "The before state has to be observable, or the after assertion proves nothing.");

        await grain.SetAsync(18, "GB");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 18),
                Does.Contain(Key));
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 17),
                Does.Not.Contain(Key),
                "Without the tombstone riding the same atomic batch the grain would answer a scan "
                + "for its old value forever.");
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Country", "GB"),
                Does.Contain(Key),
                "A property that did not move must not be disturbed.");
        });
    }

    [Test]
    public async Task Re_activating_without_a_state_change_does_not_rewrite_the_entries()
    {
        const string Key = "idempotent-reactivation";
        var grain = Grains.GetGrain<IIndexedUserGrain>(Key);
        await grain.SetAsync(44, "IE");

        var entryKey = GrainIndexKeyEncoder.EncodeKey("Age", 44, Key);
        var before = await IndexTree(GrainIndexEnrollmentClusterFixture.UsersIndex)
            .GetWithVersionAsync(entryKey, CancellationToken.None);

        await grain.DeactivateAsync();
        await Grains.GetGrain<IIndexedUserGrain>(Key).GetAgeAsync();

        var after = await IndexTree(GrainIndexEnrollmentClusterFixture.UsersIndex)
            .GetWithVersionAsync(entryKey, CancellationToken.None);

        Assert.That(after.Version, Is.EqualTo(before.Version),
            "Re-activation re-projects, and an unchanged projection must produce an empty plan that "
            + "never reaches the tree. A changed version would mean every activation churns the index.");
    }

    [Test]
    public async Task An_index_write_failure_surfaces_to_the_caller_without_corrupting_the_grains_state()
    {
        const string Key = "induced-failure";
        var harness = new FaultedEnrolmentHarness(this, Key);
        await harness.ActivateAsync();

        harness.State.State.Age = 55;
        harness.State.State.Country = "NZ";

        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>(),
            "A failed index write must reach the caller, or it becomes drift nobody can see.");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(harness.State.WriteCount, Is.EqualTo(1),
                "The grain's own state is committed before the index batch is attempted, so an "
                + "index fault can neither roll it back nor corrupt it.");
            Assert.That(harness.State.State.Age, Is.EqualTo(55));
            Assert.That(
                await HasPendingAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, Key), Is.True,
                "The outbox entry is what turns the failure from silent drift into bounded delay.");
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 55),
                Does.Not.Contain(Key));
        });
    }

    [Test]
    public async Task A_pending_projection_is_retried_to_completion_after_a_simulated_restart()
    {
        const string Key = "outbox-convergence";
        var harness = new FaultedEnrolmentHarness(this, Key);
        await harness.ActivateAsync();

        harness.State.State.Age = 66;
        harness.State.State.Country = "SE";
        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        // The fault clears and a silo that never saw the write picks the outbox
        // up: a drain built fresh over the real trees, with the real store.
        var result = await Drainer().DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.Applied, Is.GreaterThanOrEqualTo(1));
            Assert.That(result.Failed, Is.Zero);
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 66),
                Does.Contain(Key),
                "The index converges without the caller retrying anything.");
            Assert.That(
                await HasPendingAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, Key), Is.False,
                "A converged entry has to leave the outbox, or the drain would never finish.");
            Assert.That(
                await HasSeenMarkerAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, Key), Is.True,
                "Converging the write also enrols the grain, so the backfill skips it.");
        });
    }

    [Test]
    public async Task A_second_drain_after_convergence_finds_nothing_left_to_do()
    {
        const string Key = "outbox-idempotent";
        var harness = new FaultedEnrolmentHarness(this, Key);
        await harness.ActivateAsync();
        harness.State.State.Age = 77;
        Assert.That(
            async () => await harness.Indexed.WriteStateAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        await Drainer().DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);
        var second = await Drainer().DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        Assert.That(second.Scanned, Is.Zero,
            "Convergence has to be a terminal state, not a steady stream of repeated work.");
    }

    [Test]
    public async Task An_eventual_mode_write_lands_only_once_the_outbox_is_drained()
    {
        const string Key = "eventual-write";
        await Grains.GetGrain<IEventualUserGrain>(Key).SetAsync(88, "NO");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.EventualIndex, "Age", 88),
                Does.Not.Contain(Key),
                "Eventual mode defers the index batch, which is the whole point of choosing it.");
            Assert.That(
                await HasPendingAsync(GrainIndexEnrollmentClusterFixture.EventualIndex, Key), Is.True,
                "Deferred is not the same as lost: the batch is durable before the write returns.");
        });

        await Drainer().DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        Assert.That(
            await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.EventualIndex, "Age", 88),
            Does.Contain(Key));
    }

    [Test]
    public async Task Clearing_a_grains_state_withdraws_its_entries_and_its_marker()
    {
        const string Key = "withdrawal";
        var grain = Grains.GetGrain<IIndexedUserGrain>(Key);
        await grain.SetAsync(99, "DK");
        Assert.That(
            await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 99),
            Does.Contain(Key));

        await grain.ClearAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, "Age", 99),
                Does.Not.Contain(Key),
                "A grain with no state must stop answering queries, or the index outlives its subject.");
            Assert.That(
                await HasSeenMarkerAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, Key), Is.False,
                "Dropping the marker is what lets a later backfill reconsider the grain.");
        });
    }

    [Test]
    public async Task The_base_class_facade_enrols_exactly_as_the_attribute_alone_does()
    {
        const string Key = "base-class";
        await Grains.GetGrain<IBaseClassUserGrain>(Key).SetAsync(23, "PT");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await GrainsAtAsync(GrainIndexEnrollmentClusterFixture.BaseClassIndex, "Age", 23),
                Does.Contain(Key),
                "The base class is ergonomics over the same attribute, so it must enrol identically.");
            Assert.That(
                await HasSeenMarkerAsync(GrainIndexEnrollmentClusterFixture.BaseClassIndex, Key), Is.True);
        });
    }

    [Test]
    public async Task A_grain_whose_state_no_index_projects_is_left_alone()
    {
        const string Key = "untracked";
        await Grains.GetGrain<IUntrackedGrain>(Key).SetAsync(5);

        using (LatticeSystemOrigin.Enter())
        {
            var markers = await RegistryTree()
                .KeysAsync(
                    GrainIndexRegistryKeys.SeenPrefix("untracked"),
                    GrainIndexRegistryKeys.SeenPrefixEnd("untracked"),
                    cancellationToken: CancellationToken.None)
                .ToListAsync();

            Assert.That(markers, Is.Empty,
                "An attribute that currently matches no declared index must cost the grain nothing, "
                + "which is what makes it safe to annotate ahead of declaring the index.");
        }
    }

    [Test]
    public async Task Only_the_index_whose_grain_interface_the_grain_implements_tracks_it()
    {
        const string Key = "interface-scoped";
        await Grains.GetGrain<IIndexedUserGrain>(Key).SetAsync(41, "ES");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await HasSeenMarkerAsync(GrainIndexEnrollmentClusterFixture.UsersIndex, Key), Is.True);
            Assert.That(
                await HasSeenMarkerAsync(GrainIndexEnrollmentClusterFixture.BaseClassIndex, Key), Is.False,
                "Three indexes share this state type, so an index must only claim a grain that "
                + "actually implements the interface it was declared over.");
        });
    }

    /// <summary>
    /// The mutation path wired against the real registry tree but a deliberately
    /// broken index tree, which is how an index-write fault is induced without
    /// breaking anything the rest of the silo depends on.
    /// </summary>
    private sealed class FaultedEnrolmentHarness
    {
        public FaultedEnrolmentHarness(GrainIndexEnrollmentIntegrationTests owner, string grainKey)
        {
            State = new RecordingPersistentState<IndexedUserState>(new IndexedUserState());

            var definition = new GrainIndexDefinition<IIndexedUserGrain, IndexedUserState>(
                GrainIndexEnrollmentClusterFixture.UsersIndex,
                StringGrainKeyCodec<IIndexedUserGrain>.Instance,
                [
                    new TypedGrainIndexProperty<IndexedUserState, int>("Age", static s => s.Age),
                    new TypedGrainIndexProperty<IndexedUserState, string>("Country", static s => s.Country),
                ]);

            var enroller = new TypedGrainIndexEnroller<IIndexedUserGrain, IndexedUserState>(
                definition,
                EnrollmentTrees.Faulting(),
                owner.Store(),
                GrainIndexProjectionMode.Synchronous);

            Harness = new IndexedStateHarness<IndexedUserState>(
                State,
                grainKey,
                Substitute.For<IIndexedUserGrain>(),
                enroller);
        }

        public RecordingPersistentState<IndexedUserState> State { get; }

        public IndexedStateHarness<IndexedUserState> Harness { get; }

        public IndexedPersistentState<IndexedUserState> Indexed => Harness.Indexed;

        /// <summary>
        /// Runs the activation hook, which reads the (absent) baseline and
        /// leaves the grain unenrolled because it has no stored state yet.
        /// </summary>
        public Task ActivateAsync() => Harness.ActivateAsync();
    }
}
