using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using System.Collections.Immutable;
using System.Net;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the argument guards on <see cref="LatticeTenantRegistry"/>. The
/// guards run before any grain call, so they are exercised without a live silo;
/// the read-merge-write, list, delete, and default-seeding behaviour is covered
/// by the integration convergence fixture.
/// </summary>
[TestFixture]
public sealed class LatticeTenantRegistryTests
{
    private static LatticeTenantRegistry CreateRegistry(
        IGrainFactory? grainFactory = null,
        IClusterMembershipService? membership = null,
        ILocalSiloDetails? localSilo = null)
    {
        grainFactory ??= Substitute.For<IGrainFactory>();
        var services = new ServiceCollection().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeTenancyOptions>>();
        options.CurrentValue.Returns(new LatticeTenancyOptions());
        var cluster = Options.Create(new ClusterOptions { ClusterId = "test-cluster" });
        var serializer = TestSerializers.TenantRecords;
        var initializer = new TenantRegistryInitializer(grainFactory, services, options, cluster, serializer);
        return new LatticeTenantRegistry(grainFactory, initializer, serializer, Substitute.For<ITenantAccessDataPurge>(), membership, localSilo);
    }

    private static readonly SiloAddress Local = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 1);

    private static (IClusterMembershipService Membership, ILocalSiloDetails LocalSilo) Standing(SiloStatus status)
    {
        var membership = Substitute.For<IClusterMembershipService>();
        membership.CurrentSnapshot.Returns(new ClusterMembershipSnapshot(
            ImmutableDictionary<SiloAddress, ClusterMember>.Empty.Add(Local, new ClusterMember(Local, status, "local")),
            new MembershipVersion(1)));
        var localSilo = Substitute.For<ILocalSiloDetails>();
        localSilo.SiloAddress.Returns(Local);
        return (membership, localSilo);
    }

    [Test]
    public void A_silo_membership_reports_dead_refuses_registry_writes_before_touching_the_registry()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var (membership, localSilo) = Standing(SiloStatus.Dead);
        var registry = CreateRegistry(grainFactory, membership, localSilo);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await registry.PutAsync(TenantPolicyTestData.Record("acme")), Throws.InvalidOperationException);
            Assert.That(async () => await registry.PutGuardedAsync(TenantPolicyTestData.Record("acme"), _ => { }), Throws.InvalidOperationException);
            Assert.That(async () => await registry.DeleteAsync(TenantPolicyTestData.Record("acme").Id), Throws.InvalidOperationException);
        });
        Assert.That(grainFactory.ReceivedCalls(), Is.Empty, "nothing reaches the registry tree");
    }

    [TestCase(SiloStatus.Active)]
    [TestCase(SiloStatus.ShuttingDown)]
    public void A_silo_membership_does_not_report_dead_may_write(SiloStatus status)
    {
        var (membership, localSilo) = Standing(status);

        Assert.That(() => CreateRegistry(membership: membership, localSilo: localSilo).EnsureNotDeclaredDead(), Throws.Nothing);
    }

    [Test]
    public void Off_a_silo_there_is_no_standing_to_check()
    {
        Assert.That(() => CreateRegistry().EnsureNotDeclaredDead(), Throws.Nothing);
    }

    [Test]
    public void GetAsync_with_the_no_tenant_value_throws()
    {
        var registry = CreateRegistry();

        Assert.That(async () => await registry.GetAsync(default), Throws.ArgumentException);
    }

    [Test]
    public void ExistsAsync_with_the_no_tenant_value_throws()
    {
        var registry = CreateRegistry();

        Assert.That(async () => await registry.ExistsAsync(default), Throws.ArgumentException);
    }

    [Test]
    public void DeleteAsync_with_the_no_tenant_value_throws()
    {
        var registry = CreateRegistry();

        Assert.That(async () => await registry.DeleteAsync(default), Throws.ArgumentException);
    }

    [Test]
    public void PutAsync_null_record_throws()
    {
        var registry = CreateRegistry();

        Assert.That(async () => await registry.PutAsync(null!), Throws.ArgumentNullException);
    }
}
