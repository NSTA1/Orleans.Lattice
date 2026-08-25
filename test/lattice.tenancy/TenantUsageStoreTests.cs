using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic unit coverage for <see cref="TenantUsageStore"/>: the argument
/// guards and the optimistic-concurrency retry loop in
/// <see cref="TenantUsageStore.PutMergeAsync"/>, driven directly against a
/// substituted <see cref="ILattice"/> whose conditional write is scripted to
/// conflict, so the "concurrent publishes converge, no slot is dropped" contract is
/// proven without any timing. Reads and writes are scripted through the exact
/// <see cref="OrleansLatticeSerializer{T}"/> the store uses in production. The full
/// grain round-trip is covered by the integration convergence fixture.
/// </summary>
[TestFixture]
public sealed class TenantUsageStoreTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static (TenantUsageStore Store, OrleansLatticeSerializer<TenantUsageRecord> Serializer) Create()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var serializer = TestSerializers.For<TenantUsageRecord>();
        return (new TenantUsageStore(grainFactory, serializer), serializer);
    }

    [Test]
    public void GetAsync_with_the_no_tenant_value_throws()
    {
        var (store, _) = Create();

        Assert.That(async () => await store.GetAsync(default), Throws.ArgumentException);
    }

    [Test]
    public void PublishAsync_null_record_throws()
    {
        var (store, _) = Create();

        Assert.That(async () => await store.PublishAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task PutMergeAsync_retries_on_version_conflict_and_re_merges_the_committed_slots()
    {
        var (store, serializer) = Create();
        var lattice = Substitute.For<ILattice>();

        // The first read sees this cluster's east slot; the write loses the race.
        var read0 = UsageRecord("acme", ("east", Sample(100, 1, 10, 1)));
        var v0 = new VersionedValue { Value = serializer.Serialize(read0), Version = Clock(1) };

        // The re-read now sees a competing cluster's committed west slot.
        var read1 = UsageRecord("acme", ("east", Sample(100, 1, 10, 1)), ("west", Sample(200, 2, 20, 1)));
        var v1 = new VersionedValue { Value = serializer.Serialize(read1), Version = Clock(2) };

        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns(v0, v1);
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false, true);

        // This cluster publishes a fresher north slot; it must survive alongside the
        // competitor's west slot and this record's east slot.
        var incoming = UsageRecord("acme", ("north", Sample(300, 3, 30, 1)));

        var merged = await store.PutMergeAsync(lattice, Acme.Value!, incoming, CancellationToken.None);

        Assert.That(merged.Fold(), Is.EqualTo(Sample(600, 6, 60, 3)), "all three cluster slots survive the retry");
        await lattice.Received(2).GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>());
    }

    [Test]
    public void PutMergeAsync_throws_after_exhausting_the_retry_budget()
    {
        var (store, serializer) = Create();
        var lattice = Substitute.For<ILattice>();

        var stored = new VersionedValue
        {
            Value = serializer.Serialize(UsageRecord("acme", ("east", Sample(100, 1, 10, 1)))),
            Version = Clock(1),
        };
        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns(stored);
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var incoming = UsageRecord("acme", ("north", Sample(300, 3, 30, 1)));

        var ex = Assert.ThrowsAsync<TenantUsageConcurrencyException>(
            async () => await store.PutMergeAsync(lattice, Acme.Value!, incoming, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Tenant, Is.EqualTo(Acme));
            Assert.That(ex.Attempts, Is.EqualTo(8));
        });
    }

    [Test]
    public async Task PutMergeAsync_creates_the_record_when_the_key_is_absent()
    {
        var (store, _) = Create();
        var lattice = Substitute.For<ILattice>();

        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero });
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var incoming = UsageRecord("acme", ("east", Sample(100, 1, 10, 1)));

        var merged = await store.PutMergeAsync(lattice, Acme.Value!, incoming, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Id, Is.EqualTo(Acme));
            Assert.That(merged.Fold(), Is.EqualTo(Sample(100, 1, 10, 1)));
        });
        await lattice.Received(1).SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
    }
}
