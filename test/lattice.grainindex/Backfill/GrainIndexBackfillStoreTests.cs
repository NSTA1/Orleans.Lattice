using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Covers <see cref="GrainIndexBackfillStore"/>: the key a checkpoint is filed
/// under, the tree it addresses, and the round trip through the Orleans-backed
/// serializer.
/// </summary>
[TestFixture]
public sealed class GrainIndexBackfillStoreTests
{
    private static readonly DateTimeOffset Origin = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private ServiceProvider _provider = null!;
    private OrleansGrainIndexSerializer<GrainIndexBackfillCheckpoint> _serializer = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        _provider = services.BuildServiceProvider();
        _serializer = new OrleansGrainIndexSerializer<GrainIndexBackfillCheckpoint>(
            _provider.GetRequiredService<Serializer<GrainIndexBackfillCheckpoint>>());
    }

    [TearDown]
    public void TearDown() => _provider.Dispose();

    private static GrainIndexBackfillCheckpoint SampleCheckpoint() =>
        GrainIndexBackfillCheckpoint
            .Start(new GrainIndexFingerprint("0123456789ABCDEF0123456789ABCDEF"), revisitsEnrolled: true, Origin)
            .Advance("cursor", visited: 3, enrolled: 2, skipped: 1, failed: 0, Origin.AddMinutes(1));

    private static IGrainFactory FactoryFor(ILattice lattice)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(lattice);
        return factory;
    }

    private static ILattice AbsentTree()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<byte[]?>(null));
        return lattice;
    }

    private static ILattice EchoingTree()
    {
        var lattice = Substitute.For<ILattice>();
        byte[]? written = null;
        lattice
            .SetAsync(Arg.Any<string>(), Arg.Do<byte[]>(bytes => written = bytes), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(written));
        return lattice;
    }

    [Test]
    public async Task An_index_whose_crawl_never_started_reads_as_null()
    {
        var store = new GrainIndexBackfillStore(FactoryFor(AbsentTree()), _serializer);

        Assert.That(await store.ReadAsync("users", CancellationToken.None), Is.Null,
            "A crawl that never started must be distinguishable from one at the head of its range.");
    }

    [Test]
    public async Task A_written_checkpoint_round_trips_through_the_orleans_wire_format()
    {
        var store = new GrainIndexBackfillStore(FactoryFor(EchoingTree()), _serializer);
        var checkpoint = SampleCheckpoint();

        await store.WriteAsync("users", checkpoint, CancellationToken.None);
        var read = await store.ReadAsync("users", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.Not.Null,
                "The checkpoint has get-only properties, so a JSON round trip would have lost them.");
            Assert.That(read!.State, Is.EqualTo(checkpoint.State));
            Assert.That(read.Fingerprint, Is.EqualTo(checkpoint.Fingerprint));
            Assert.That(read.ResumeAfterKey, Is.EqualTo(checkpoint.ResumeAfterKey));
            Assert.That(read.Visited, Is.EqualTo(checkpoint.Visited));
            Assert.That(read.Enrolled, Is.EqualTo(checkpoint.Enrolled));
            Assert.That(read.Skipped, Is.EqualTo(checkpoint.Skipped));
            Assert.That(read.Failed, Is.EqualTo(checkpoint.Failed));
            Assert.That(read.Passes, Is.EqualTo(checkpoint.Passes));
            Assert.That(read.RevisitsEnrolled, Is.EqualTo(checkpoint.RevisitsEnrolled));
            Assert.That(read.StartedUtc, Is.EqualTo(checkpoint.StartedUtc));
            Assert.That(read.UpdatedUtc, Is.EqualTo(checkpoint.UpdatedUtc));
            Assert.That(read.CompletedUtc, Is.EqualTo(checkpoint.CompletedUtc));
            Assert.That(read.FailureMessage, Is.EqualTo(checkpoint.FailureMessage));
        });
    }

    [Test]
    public async Task A_checkpoint_is_written_under_the_index_names_checkpoint_key()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await new GrainIndexBackfillStore(FactoryFor(lattice), _serializer)
            .WriteAsync("users", SampleCheckpoint(), CancellationToken.None);

        await lattice.Received(1).SetAsync(
            GrainIndexRegistryKeys.Checkpoint("users"),
            Arg.Any<byte[]>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_checkpoint_is_read_from_the_index_names_checkpoint_key()
    {
        var lattice = AbsentTree();

        await new GrainIndexBackfillStore(FactoryFor(lattice), _serializer)
            .ReadAsync("users", CancellationToken.None);

        await lattice.Received(1).GetAsync(
            GrainIndexRegistryKeys.Checkpoint("users"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_store_addresses_the_internal_registry_tree()
    {
        var factory = FactoryFor(AbsentTree());

        await new GrainIndexBackfillStore(factory, _serializer)
            .ReadAsync("users", CancellationToken.None);

        factory.Received().GetGrain<ILattice>(
            GrainIndexRegistryTrees.RegistryTree,
            Arg.Any<string?>());
    }

    [Test]
    public async Task The_cancellation_token_is_passed_to_the_tree()
    {
        using var cts = new CancellationTokenSource();
        var lattice = AbsentTree();

        await new GrainIndexBackfillStore(FactoryFor(lattice), _serializer)
            .ReadAsync("users", cts.Token);

        await lattice.Received(1).GetAsync(Arg.Any<string>(), cts.Token);
    }

    [Test]
    public void A_null_argument_is_rejected_on_both_operations()
    {
        var store = new GrainIndexBackfillStore(FactoryFor(AbsentTree()), _serializer);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await store.ReadAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.WriteAsync(null!, SampleCheckpoint(), CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.WriteAsync("users", null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexBackfillStore(null!, _serializer),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexBackfillStore(Substitute.For<IGrainFactory>(), null!),
                Throws.ArgumentNullException);
        });
    }
}
