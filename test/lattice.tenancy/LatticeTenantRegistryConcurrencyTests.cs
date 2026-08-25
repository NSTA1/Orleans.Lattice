using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic unit coverage for the optimistic-concurrency retry loop in
/// <see cref="LatticeTenantRegistry.PutMergeAsync"/>. The loop is driven directly
/// against a substituted <see cref="ILattice"/> whose conditional write is scripted
/// to conflict, so the "concurrent writers converge, no field is dropped" contract
/// is proven without any timing, ordering, or <c>Task.Delay</c>. Reads and writes
/// are scripted through the exact <see cref="OrleansLatticeSerializer{T}"/> the
/// registry uses in production.
/// </summary>
[TestFixture]
public sealed class LatticeTenantRegistryConcurrencyTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static readonly CrossTenantGrant GrantA =
        CrossTenantGrant.Create("tenant-a", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read);

    private static readonly CrossTenantGrant GrantB =
        CrossTenantGrant.Create("tenant-b", TenantGranteeKind.Tenant, "tree-y", TenantGrantOperations.ReadWrite);

    private static TenantRecord Base() =>
        TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 100 },
            TenantPlacement.Shared,
            Clock(10),
            "seed");

    private static (LatticeTenantRegistry Registry, OrleansLatticeSerializer<TenantRecord> Serializer) Create()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var services = new ServiceCollection().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeTenancyOptions>>();
        options.CurrentValue.Returns(new LatticeTenancyOptions());
        var cluster = Options.Create(new ClusterOptions { ClusterId = "test-cluster" });
        var serializer = TestSerializers.TenantRecords;
        var initializer = new TenantRegistryInitializer(grainFactory, services, options, cluster, serializer);
        return (new LatticeTenantRegistry(grainFactory, initializer, serializer), serializer);
    }

    [Test]
    public async Task PutMergeAsync_retries_on_version_conflict_and_re_merges_the_committed_state()
    {
        var (registry, serializer) = Create();
        var lattice = Substitute.For<ILattice>();

        // The first read sees the seeded base; the write loses the version race.
        var read0 = Base();
        var v0 = new VersionedValue { Value = serializer.Serialize(read0), Version = Clock(1) };

        // The re-read now sees a competing writer's committed grant A.
        var read1 = Base();
        read1.AddGrant(GrantA, Clock(20), "competitor");
        var v1 = new VersionedValue { Value = serializer.Serialize(read1), Version = Clock(2) };

        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns(v0, v1);
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false, true);

        // This writer adds grant B; it must survive alongside the competitor's grant A.
        var incoming = Base();
        incoming.AddGrant(GrantB, Clock(30), "me");

        var merged = await registry.PutMergeAsync(lattice, Acme.Value!, incoming, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(merged.TryGetGrant(GrantA.GrantId, out _), Is.True, "the competitor's grant A survives the retry");
            Assert.That(merged.TryGetGrant(GrantB.GrantId, out _), Is.True, "this writer's grant B survives the retry");
        });
        await lattice.Received(2).GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>());
    }

    [Test]
    public void PutMergeAsync_throws_after_exhausting_the_retry_budget_when_writes_keep_conflicting()
    {
        var (registry, serializer) = Create();
        var lattice = Substitute.For<ILattice>();

        var stored = new VersionedValue { Value = serializer.Serialize(Base()), Version = Clock(1) };
        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>()).Returns(stored);
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var incoming = Base();
        incoming.AddGrant(GrantB, Clock(30), "me");

        var ex = Assert.ThrowsAsync<TenantRegistryConcurrencyException>(
            async () => await registry.PutMergeAsync(lattice, Acme.Value!, incoming, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Tenant, Is.EqualTo(Acme));
            Assert.That(ex.Attempts, Is.EqualTo(8));
        });
    }

    [Test]
    public async Task PutMergeAsync_creates_the_record_when_the_key_is_absent()
    {
        var (registry, _) = Create();
        var lattice = Substitute.For<ILattice>();

        // An absent key returns a null value at HybridLogicalClock.Zero.
        lattice.GetWithVersionAsync(Acme.Value!, Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero });
        lattice.SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var incoming = Base();
        incoming.AddGrant(GrantB, Clock(30), "me");

        var merged = await registry.PutMergeAsync(lattice, Acme.Value!, incoming, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Id, Is.EqualTo(Acme));
            Assert.That(merged.TryGetGrant(GrantB.GrantId, out _), Is.True);
        });
        await lattice.Received(1).SetIfVersionAsync(Acme.Value!, Arg.Any<byte[]>(), HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
    }
}
