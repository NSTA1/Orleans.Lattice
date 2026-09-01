using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="GrainIndexOutboxDrainer"/>: applying the writes the system
/// owes, clearing them once they land, leaving alone the ones it cannot place,
/// and never letting one bad entry stall the rest.
/// </summary>
[TestFixture]
public sealed class GrainIndexOutboxDrainerTests
{
    private const string IndexName = "users";
    private const string TreeName = GrainIndexTreeNames.ReservedPrefix + IndexName;

    private static IOptionsMonitor<GrainIndexOptions> Options(params string[] declaredIndexes)
    {
        var services = new ServiceCollection();
        services.AddOptions();
        foreach (var name in declaredIndexes)
        {
            services.Configure<GrainIndexOptions>(
                name,
                options => options.TreeName = GrainIndexTreeNames.ForIndex(name));
        }

        return services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<GrainIndexOptions>>();
    }

    private static IGrainFactory FactoryFor(ILattice tree)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(tree);
        return factory;
    }

    private static GrainIndexOutboxDrainer Drainer(
        RecordingEnrollmentStore store,
        ILattice tree,
        params string[] declaredIndexes) =>
        new(store, FactoryFor(tree), Options(declaredIndexes), NullLogger<GrainIndexOutboxDrainer>.Instance);

    private static GrainIndexPendingProjection PendingFor(
        string indexName,
        string grainKey,
        int age = 30)
    {
        var projection = EnrollmentTestIndex.Project(
            grainKey,
            new IndexedTestState { Age = age, Country = "GB" });

        return new GrainIndexPendingProjection(
            indexName,
            grainKey,
            $"op-{grainKey}",
            GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(grainKey), projection));
    }

    private static RecordingEnrollmentStore StoreWith(params GrainIndexPendingProjection[] pending)
    {
        var store = new RecordingEnrollmentStore();
        foreach (var entry in pending)
            store.Pending[$"{entry.IndexName}/{entry.GrainKey}"] = entry;
        return store;
    }

    [Test]
    public async Task An_empty_outbox_is_a_single_range_read_and_nothing_else()
    {
        var tree = EnrollmentTrees.Accepting();
        var result = await Drainer(new RecordingEnrollmentStore(), tree, IndexName)
            .DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsEmpty, Is.True);
            Assert.That(result.Scanned, Is.Zero);
            Assert.That(tree.ReceivedCalls(), Is.Empty);
        });
    }

    [Test]
    public async Task An_outstanding_write_is_applied_and_then_confirmed()
    {
        var store = StoreWith(PendingFor(IndexName, "alice"));
        var tree = EnrollmentTrees.Accepting();

        var result = await Drainer(store, tree, IndexName)
            .DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.EqualTo(1));
            Assert.That(result.Failed, Is.Zero);
            Assert.That(result.Skipped, Is.Zero);
            Assert.That(store.HasPending(IndexName, "alice"), Is.False,
                "A converged entry has to leave the outbox, or the drain never finishes.");
            Assert.That(store.IsEnrolled(IndexName, "alice"), Is.True,
                "Converging the write also enrols the grain, so the backfill skips it.");
        });
    }

    [Test]
    public async Task A_retry_reuses_the_entrys_original_idempotency_key()
    {
        var pending = PendingFor(IndexName, "alice");
        var store = StoreWith(pending);
        var tree = EnrollmentTrees.Accepting();

        await Drainer(store, tree, IndexName)
            .DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            pending.OperationId,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_entry_that_still_will_not_apply_stays_in_the_outbox()
    {
        var store = StoreWith(PendingFor(IndexName, "alice"));

        var result = await Drainer(store, EnrollmentTrees.Faulting(), IndexName)
            .DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.EqualTo(1));
            Assert.That(result.Applied, Is.Zero);
            Assert.That(store.HasPending(IndexName, "alice"), Is.True);
            Assert.That(store.IsEnrolled(IndexName, "alice"), Is.False);
        });
    }

    [Test]
    public async Task One_entry_that_will_not_apply_does_not_stall_the_rest()
    {
        var store = StoreWith(
            PendingFor(IndexName, "alice"),
            PendingFor(IndexName, "bob"));

        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromException(new InvalidOperationException("poison")),
                _ => Task.CompletedTask);

        var result = await Drainer(store, tree, IndexName)
            .DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Scanned, Is.EqualTo(2));
            Assert.That(result.Applied, Is.EqualTo(1));
            Assert.That(result.Failed, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_entry_for_an_index_this_silo_does_not_declare_is_left_alone()
    {
        var store = StoreWith(PendingFor("elsewhere", "alice"));

        var result = await Drainer(store, EnrollmentTrees.Accepting(), IndexName)
            .DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Skipped, Is.EqualTo(1));
            Assert.That(store.HasPending("elsewhere", "alice"), Is.True,
                "This silo cannot know which tree the entry belongs to, and discarding it would "
                + "lose the write outright.");
        });
    }

    [Test]
    public async Task A_pass_stops_at_its_batch_size_and_leaves_the_remainder()
    {
        var store = StoreWith(
            PendingFor(IndexName, "a"),
            PendingFor(IndexName, "b"),
            PendingFor(IndexName, "c"));

        var result = await Drainer(store, EnrollmentTrees.Accepting(), IndexName)
            .DrainAsync(2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Scanned, Is.EqualTo(2));
            Assert.That(store.Pending, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_batch_size_below_one_still_makes_progress()
    {
        var store = StoreWith(PendingFor(IndexName, "alice"));

        var result = await Drainer(store, EnrollmentTrees.Accepting(), IndexName)
            .DrainAsync(0, CancellationToken.None);

        Assert.That(result.Applied, Is.EqualTo(1),
            "A pass that could apply nothing would let a misconfigured batch size stall convergence "
            + "forever.");
    }

    [Test]
    public async Task The_trees_resolved_once_are_reused_across_entries()
    {
        var store = StoreWith(
            PendingFor(IndexName, "a"),
            PendingFor(IndexName, "b"));

        var factory = FactoryFor(EnrollmentTrees.Accepting());
        var drainer = new GrainIndexOutboxDrainer(
            store,
            factory,
            Options(IndexName),
            NullLogger<GrainIndexOutboxDrainer>.Instance);

        await drainer.DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, CancellationToken.None);

        factory.Received(1).GetGrain<ILattice>(TreeName, Arg.Any<string?>());
    }

    [Test]
    public void A_cancelled_pass_stops_rather_than_finishing_the_batch()
    {
        var store = StoreWith(PendingFor(IndexName, "alice"));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await Drainer(store, EnrollmentTrees.Accepting(), IndexName)
                .DrainAsync(GrainIndexOutboxOptions.DefaultMaxBatchSize, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        var store = new RecordingEnrollmentStore();
        var factory = Substitute.For<IGrainFactory>();
        var options = Options(IndexName);

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexOutboxDrainer(
                    null!, factory, options, NullLogger<GrainIndexOutboxDrainer>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexOutboxDrainer(
                    store, null!, options, NullLogger<GrainIndexOutboxDrainer>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexOutboxDrainer(
                    store, factory, null!, NullLogger<GrainIndexOutboxDrainer>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexOutboxDrainer(store, factory, options, null!),
                Throws.ArgumentNullException);
        });
    }
}
