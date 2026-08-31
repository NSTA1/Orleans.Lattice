using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexRegistryStore"/>: the mapping from an index name
/// to a registry key, the tree it addresses, and the round trip through the
/// Orleans-backed serializer.
/// <para>
/// The <see cref="ILattice"/> tree is substituted, so this stays a unit test:
/// the point is that the store addresses the right tree under the right key and
/// that the record survives the wire format, not that a real tree stores bytes.
/// </para>
/// </summary>
[TestFixture]
public sealed class GrainIndexRegistryStoreTests
{
    private ServiceProvider _provider = null!;
    private OrleansGrainIndexSerializer<GrainIndexRegistryRecord> _serializer = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        _provider = services.BuildServiceProvider();
        _serializer = new OrleansGrainIndexSerializer<GrainIndexRegistryRecord>(
            _provider.GetRequiredService<Serializer<GrainIndexRegistryRecord>>());
    }

    [TearDown]
    public void TearDown() => _provider.Dispose();

    private static GrainIndexRegistryRecord SampleRecord(bool needsBackfill = true)
    {
        var descriptor = DescriptorFactory.Create();
        return new GrainIndexRegistryRecord(
            descriptor,
            DescriptorFactory.DefaultKeyCodecId,
            GrainIndexFingerprint.Compute(descriptor, DescriptorFactory.DefaultKeyCodecId),
            needsBackfill);
    }

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
    public async Task Reading_an_index_that_was_never_registered_returns_null()
    {
        var store = new GrainIndexRegistryStore(FactoryFor(AbsentTree()), _serializer);

        Assert.That(await store.ReadAsync("users", CancellationToken.None), Is.Null,
            "A first run has to be distinguishable from a stored record, or every silo would "
            + "look like a first run.");
    }

    [Test]
    public async Task A_written_record_round_trips_through_the_orleans_wire_format()
    {
        var store = new GrainIndexRegistryStore(FactoryFor(EchoingTree()), _serializer);
        var record = SampleRecord();

        await store.WriteAsync("users", record, CancellationToken.None);
        var read = await store.ReadAsync("users", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.Not.Null,
                "The record has get-only properties, so a JSON round trip would have lost them; "
                + "the Orleans wire format is what keeps it whole.");
            Assert.That(read!.Descriptor.Name, Is.EqualTo(record.Descriptor.Name));
            Assert.That(read.Descriptor.TreeName, Is.EqualTo(record.Descriptor.TreeName));
            Assert.That(read.Descriptor.GrainInterfaceTypeName, Is.EqualTo(record.Descriptor.GrainInterfaceTypeName));
            Assert.That(read.Descriptor.StateTypeName, Is.EqualTo(record.Descriptor.StateTypeName));
            Assert.That(read.Descriptor.Properties, Is.EqualTo(record.Descriptor.Properties));
            Assert.That(read.Descriptor.AllowReplication, Is.EqualTo(record.Descriptor.AllowReplication));
            Assert.That(read.KeyCodecId, Is.EqualTo(record.KeyCodecId));
            Assert.That(read.Fingerprint, Is.EqualTo(record.Fingerprint));
            Assert.That(read.NeedsBackfill, Is.EqualTo(record.NeedsBackfill));
        });
    }

    [Test]
    public async Task A_round_tripped_record_still_compares_as_unchanged_to_the_drift_detector()
    {
        var store = new GrainIndexRegistryStore(FactoryFor(EchoingTree()), _serializer);
        var record = SampleRecord();
        await store.WriteAsync("users", record, CancellationToken.None);

        var read = await store.ReadAsync("users", CancellationToken.None);

        Assert.That(
            GrainIndexDriftDetector.Detect(read!, record.Descriptor, record.KeyCodecId).HasDrift,
            Is.False,
            "Serialization must not itself look like drift, or every restart would reject.");
    }

    [Test]
    public async Task A_record_is_written_under_the_index_names_definition_key()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var store = new GrainIndexRegistryStore(FactoryFor(lattice), _serializer);
        await store.WriteAsync("users", SampleRecord(), CancellationToken.None);

        await lattice.Received(1).SetAsync(
            GrainIndexRegistryKeys.Definition("users"),
            Arg.Any<byte[]>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_record_is_read_from_the_index_names_definition_key()
    {
        var lattice = AbsentTree();

        await new GrainIndexRegistryStore(FactoryFor(lattice), _serializer)
            .ReadAsync("users", CancellationToken.None);

        await lattice.Received(1).GetAsync(
            GrainIndexRegistryKeys.Definition("users"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_store_addresses_the_internal_registry_tree()
    {
        var factory = FactoryFor(AbsentTree());

        await new GrainIndexRegistryStore(factory, _serializer)
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

        await new GrainIndexRegistryStore(FactoryFor(lattice), _serializer)
            .ReadAsync("users", cts.Token);

        await lattice.Received(1).GetAsync(Arg.Any<string>(), cts.Token);
    }

    [Test]
    public void A_null_index_name_is_rejected_on_both_operations()
    {
        var store = new GrainIndexRegistryStore(FactoryFor(AbsentTree()), _serializer);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await store.ReadAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.WriteAsync(null!, SampleRecord(), CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_null_record_is_rejected()
    {
        var store = new GrainIndexRegistryStore(FactoryFor(AbsentTree()), _serializer);

        Assert.That(
            async () => await store.WriteAsync("users", null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexRegistryStore(null!, _serializer),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexRegistryStore(Substitute.For<IGrainFactory>(), null!),
                Throws.ArgumentNullException);
        });
    }
}
