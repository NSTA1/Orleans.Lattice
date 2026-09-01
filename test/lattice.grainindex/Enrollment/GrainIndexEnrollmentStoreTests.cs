using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="GrainIndexEnrollmentStore"/>: the keys it addresses, the
/// tree it addresses them in, and the atomicity of the step that confirms a
/// write.
/// <para>
/// The tree is substituted, so this stays a unit test: the point is which key
/// and which batch shape, not that a real tree stores bytes.
/// </para>
/// </summary>
[TestFixture]
public sealed class GrainIndexEnrollmentStoreTests
{
    private const string IndexName = "users";
    private const string GrainKey = "alice";

    private ServiceProvider _provider = null!;
    private OrleansGrainIndexSerializer<GrainIndexEnrollmentRecord> _enrollmentSerializer = null!;
    private OrleansGrainIndexSerializer<GrainIndexPendingProjection> _pendingSerializer = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        _provider = services.BuildServiceProvider();
        _enrollmentSerializer = new OrleansGrainIndexSerializer<GrainIndexEnrollmentRecord>(
            _provider.GetRequiredService<Serializer<GrainIndexEnrollmentRecord>>());
        _pendingSerializer = new OrleansGrainIndexSerializer<GrainIndexPendingProjection>(
            _provider.GetRequiredService<Serializer<GrainIndexPendingProjection>>());
    }

    [TearDown]
    public void TearDown() => _provider.Dispose();

    private static IGrainFactory FactoryFor(ILattice lattice)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(lattice);
        return factory;
    }

    private GrainIndexEnrollmentStore StoreOver(ILattice tree) =>
        new(FactoryFor(tree), _enrollmentSerializer, _pendingSerializer);

    private static GrainIndexProjection Projection() =>
        EnrollmentTestIndex.Project(GrainKey, new IndexedTestState { Age = 30, Country = "GB" });

    private static GrainIndexPendingProjection Pending() =>
        new(
            IndexName,
            GrainKey,
            "operation",
            GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection()));

    private static ILattice AbsentTree()
    {
        var tree = Substitute.For<ILattice>();
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<byte[]?>(null));
        return tree;
    }

    [Test]
    public async Task The_store_addresses_the_internal_registry_tree()
    {
        var factory = FactoryFor(AbsentTree());

        await new GrainIndexEnrollmentStore(factory, _enrollmentSerializer, _pendingSerializer)
            .ReadEnrollmentAsync(IndexName, GrainKey, CancellationToken.None);

        factory.Received().GetGrain<ILattice>(
            GrainIndexRegistryTrees.RegistryTree,
            Arg.Any<string?>());
    }

    [Test]
    public async Task A_grain_that_was_never_enrolled_reads_back_as_absent()
    {
        var store = StoreOver(AbsentTree());

        Assert.That(
            await store.ReadEnrollmentAsync(IndexName, GrainKey, CancellationToken.None),
            Is.Null);
    }

    [Test]
    public async Task An_enrolment_is_read_from_the_grains_seen_key()
    {
        var tree = AbsentTree();

        await StoreOver(tree).ReadEnrollmentAsync(IndexName, GrainKey, CancellationToken.None);

        await tree.Received(1).GetAsync(
            GrainIndexRegistryKeys.Seen(IndexName, GrainKey),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_outbox_entry_is_written_to_the_grains_pending_key()
    {
        var tree = Substitute.For<ILattice>();
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await StoreOver(tree).WritePendingAsync(Pending(), CancellationToken.None);

        await tree.Received(1).SetAsync(
            GrainIndexRegistryKeys.Pending(IndexName, GrainKey),
            Arg.Any<byte[]>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_outbox_entry_round_trips_through_the_orleans_wire_format()
    {
        byte[]? written = null;
        var tree = Substitute.For<ILattice>();
        tree.SetAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => written = b), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var pending = Pending();
        await StoreOver(tree).WritePendingAsync(pending, CancellationToken.None);
        var read = _pendingSerializer.Deserialize(written!);

        Assert.Multiple(() =>
        {
            Assert.That(read.IndexName, Is.EqualTo(pending.IndexName));
            Assert.That(read.GrainKey, Is.EqualTo(pending.GrainKey));
            Assert.That(read.OperationId, Is.EqualTo(pending.OperationId));
            Assert.That(read.Plan.Upserts, Has.Count.EqualTo(pending.Plan.Upserts.Count));
            Assert.That(read.Plan.Projection.GrainKey, Is.EqualTo(GrainKey));
        });
    }

    [Test]
    public async Task Confirming_a_write_upserts_the_marker_and_clears_the_outbox_in_one_batch()
    {
        List<KeyValuePair<string, byte[]>>? upserts = null;
        IReadOnlyList<string>? deletes = null;
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Do<List<KeyValuePair<string, byte[]>>>(u => upserts = u),
                Arg.Do<IReadOnlyList<string>>(d => deletes = d),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await StoreOver(tree).CompleteAsync(IndexName, GrainKey, Projection(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(upserts!.Select(u => u.Key), Is.EqualTo(new[] { GrainIndexRegistryKeys.Seen(IndexName, GrainKey) }));
            Assert.That(deletes, Is.EqualTo(new[] { GrainIndexRegistryKeys.Pending(IndexName, GrainKey) }));
        });
    }

    [Test]
    public async Task The_confirmation_batch_carries_an_idempotency_key_the_seam_accepts()
    {
        string? operationId = null;
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(),
                Arg.Do<string>(id => operationId = id),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await StoreOver(tree).CompleteAsync(IndexName, GrainKey, Projection(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(operationId, Is.Not.Null.And.Not.Empty);
            Assert.That(operationId, Does.Not.Contain("/"),
                "The atomic-batch seam reserves '/' as its grain-key separator.");
        });
    }

    [Test]
    public async Task Withdrawing_deletes_both_of_the_grains_registry_keys_in_one_batch()
    {
        List<KeyValuePair<string, byte[]>>? upserts = null;
        IReadOnlyList<string>? deletes = null;
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Do<List<KeyValuePair<string, byte[]>>>(u => upserts = u),
                Arg.Do<IReadOnlyList<string>>(d => deletes = d),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await StoreOver(tree).WithdrawAsync(IndexName, GrainKey, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(upserts, Is.Empty);
            Assert.That(
                deletes,
                Is.EqualTo(new[]
                {
                    GrainIndexRegistryKeys.Seen(IndexName, GrainKey),
                    GrainIndexRegistryKeys.Pending(IndexName, GrainKey),
                }));
        });
    }

    [Test]
    public async Task The_outbox_scan_covers_every_index_in_one_range_read()
    {
        string? start = null;
        string? end = null;
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync(
                Arg.Do<string?>(s => start = s),
                Arg.Do<string?>(e => end = e),
                Arg.Any<bool>(),
                Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(Empty());

        await foreach (var _ in StoreOver(tree).ScanPendingAsync(CancellationToken.None))
        {
            // Draining the enumerator is what issues the range read.
        }

        Assert.Multiple(() =>
        {
            Assert.That(start, Is.EqualTo(GrainIndexRegistryKeys.PendingPrefix()));
            Assert.That(end, Is.EqualTo(GrainIndexRegistryKeys.PendingPrefixEnd()));
        });
    }

    [Test]
    public async Task The_outbox_scan_deserialises_every_entry_it_finds()
    {
        var pending = Pending();
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync(
                Arg.Any<string?>(),
                Arg.Any<string?>(),
                Arg.Any<bool>(),
                Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(One(
                GrainIndexRegistryKeys.Pending(IndexName, GrainKey),
                _pendingSerializer.Serialize(pending)));

        var scanned = new List<GrainIndexPendingProjection>();
        await foreach (var entry in StoreOver(tree).ScanPendingAsync(CancellationToken.None))
            scanned.Add(entry);

        Assert.Multiple(() =>
        {
            Assert.That(scanned, Has.Count.EqualTo(1));
            Assert.That(scanned[0].GrainKey, Is.EqualTo(GrainKey));
            Assert.That(scanned[0].OperationId, Is.EqualTo(pending.OperationId));
        });
    }

    [Test]
    public async Task The_cancellation_token_reaches_the_tree()
    {
        using var cts = new CancellationTokenSource();
        var tree = AbsentTree();

        await StoreOver(tree).ReadEnrollmentAsync(IndexName, GrainKey, cts.Token);

        await tree.Received(1).GetAsync(Arg.Any<string>(), cts.Token);
    }

    [Test]
    public void A_null_argument_is_rejected_on_every_operation()
    {
        var store = StoreOver(AbsentTree());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await store.ReadEnrollmentAsync(null!, GrainKey, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.ReadEnrollmentAsync(IndexName, null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.WritePendingAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.CompleteAsync(null!, GrainKey, Projection(), CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.CompleteAsync(IndexName, null!, Projection(), CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.CompleteAsync(IndexName, GrainKey, null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.WithdrawAsync(null!, GrainKey, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await store.WithdrawAsync(IndexName, null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexEnrollmentStore(null!, _enrollmentSerializer, _pendingSerializer),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexEnrollmentStore(
                    Substitute.For<IGrainFactory>(), null!, _pendingSerializer),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexEnrollmentStore(
                    Substitute.For<IGrainFactory>(), _enrollmentSerializer, null!),
                Throws.ArgumentNullException);
        });
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> One(string key, byte[] value)
    {
        await Task.CompletedTask;
        yield return new KeyValuePair<string, byte[]>(key, value);
    }
}
