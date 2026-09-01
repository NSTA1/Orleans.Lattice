using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic unit coverage for <see cref="TenantOverageStore"/>: the argument
/// guards, the empty-increment no-op, and the optimistic-concurrency retry loop in
/// <see cref="TenantOverageStore.MeterMergeAsync"/>, driven directly against a
/// substituted <see cref="ILattice"/> whose conditional write is scripted to
/// conflict, so the "concurrent meters converge, no component is dropped or
/// doubled" contract is proven without any timing. Reads and writes are scripted
/// through the exact <see cref="OrleansLatticeSerializer{T}"/> the store uses in
/// production; the full grain round-trip is covered by the integration convergence
/// fixture.
/// </summary>
[TestFixture]
public sealed class TenantOverageStoreTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static (TenantOverageStore Store, OrleansLatticeSerializer<TenantOverageRecord> Serializer) Create()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var serializer = TestSerializers.For<TenantOverageRecord>();
        return (new TenantOverageStore(grainFactory, serializer), serializer);
    }

    [Test]
    public void GetAsync_with_the_no_tenant_value_throws()
    {
        var (store, _) = Create();

        Assert.That(async () => await store.GetAsync(default), Throws.ArgumentException);
    }

    [Test]
    public void MeterAsync_with_the_no_tenant_value_throws()
    {
        var (store, _) = Create();

        Assert.That(async () => await store.MeterAsync(default, "east", Overage(1)), Throws.ArgumentException);
    }

    [Test]
    public void MeterAsync_null_or_empty_cluster_throws()
    {
        var (store, _) = Create();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await store.MeterAsync(Acme, null!, Overage(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await store.MeterAsync(Acme, string.Empty, Overage(1)), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public async Task MeterAsync_an_empty_increment_is_a_read_only_no_op()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var serializer = TestSerializers.For<TenantOverageRecord>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TenantTreeNames.OverageTree).Returns(lattice);
        lattice.GetAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        var store = new TenantOverageStore(grainFactory, serializer);

        var result = await store.MeterAsync(Acme, "east", TenantOverageSample.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(result.Id, Is.EqualTo(Acme));
            Assert.That(result.Fold(), Is.EqualTo(TenantOverageSample.Empty), "a within-quota observation neither writes nor creates a metered component");
        });
        await lattice.DidNotReceive().SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MeterMergeAsync_retries_on_version_conflict_and_re_applies_the_increment_once()
    {
        var (store, serializer) = Create();
        var lattice = Substitute.For<ILattice>();

        // The first read sees this cluster's east component; the write loses the race.
        var read0 = OverageRecord("acme", ("east", Overage(100, 1, 10, 1)));
        var v0 = new VersionedValue { Value = serializer.Serialize(read0), Version = Clock(1) };

        // The re-read now sees a competing cluster's committed west component.
        var read1 = OverageRecord("acme", ("east", Overage(100, 1, 10, 1)), ("west", Overage(200, 2, 20, 2)));
        var v1 = new VersionedValue { Value = serializer.Serialize(read1), Version = Clock(2) };

        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns(v0, v1);
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false, true);

        // This cluster meters a north increment; it must survive alongside the
        // competitor's west component and this record's east component, and the lost
        // race must re-apply exactly one north increment (never double it).
        var merged = await store.MeterMergeAsync(lattice, Acme, Acme.Value!, "north", Overage(300, 3, 30, 3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Fold(), Is.EqualTo(Overage(600, 6, 60, 6)), "all three components survive the retry with no double-count");
            Assert.That(merged.LocalOverage("north"), Is.EqualTo(Overage(300, 3, 30, 3)), "the lost race re-applies exactly one increment");
        });
        await lattice.Received(2).GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetAsync_with_a_valid_tenant_returns_null_when_the_key_is_absent()
    {
        // Covers lines 50-54: the happy-path body of GetAsync for a valid TenantId.
        var grainFactory = Substitute.For<IGrainFactory>();
        var serializer = TestSerializers.For<TenantOverageRecord>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TenantTreeNames.OverageTree).Returns(lattice);
        lattice.GetAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        var store = new TenantOverageStore(grainFactory, serializer);

        var result = await store.GetAsync(Acme);

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task MeterAsync_a_non_empty_increment_calls_through_to_the_merge_loop()
    {
        // Covers line 105: MeterAsync with a non-empty increment delegates to MeterMergeAsync.
        var grainFactory = Substitute.For<IGrainFactory>();
        var serializer = TestSerializers.For<TenantOverageRecord>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TenantTreeNames.OverageTree).Returns(lattice);

        // Absent key => zero-version; write succeeds first try.
        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero });
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var store = new TenantOverageStore(grainFactory, serializer);
        var increment = Overage(bytes: 50);

        var result = await store.MeterAsync(Acme, "east", increment);

        Assert.That(result.Fold(), Is.EqualTo(increment));
    }

    [Test]
    public void MeterMergeAsync_throws_after_exhausting_the_retry_budget()
    {
        var (store, serializer) = Create();
        var lattice = Substitute.For<ILattice>();

        var stored = new VersionedValue
        {
            Value = serializer.Serialize(OverageRecord("acme", ("east", Overage(100, 1, 10, 1)))),
            Version = Clock(1),
        };
        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns(stored);
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var ex = Assert.ThrowsAsync<TenantOverageConcurrencyException>(
            async () => await store.MeterMergeAsync(lattice, Acme, Acme.Value!, "north", Overage(300, 3, 30, 3), CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Tenant, Is.EqualTo(Acme));
            Assert.That(ex.Attempts, Is.EqualTo(8));
        });
    }

    [Test]
    public async Task MeterMergeAsync_creates_the_record_when_the_key_is_absent()
    {
        var (store, _) = Create();
        var lattice = Substitute.For<ILattice>();

        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero });
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var merged = await store.MeterMergeAsync(lattice, Acme, Acme.Value!, "east", Overage(100, 1, 10, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Id, Is.EqualTo(Acme));
            Assert.That(merged.Fold(), Is.EqualTo(Overage(100, 1, 10, 1)));
        });
        await lattice.Received(1).SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
    }
}
