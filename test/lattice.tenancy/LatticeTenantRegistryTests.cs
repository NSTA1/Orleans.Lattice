using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
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
    private static LatticeTenantRegistry CreateRegistry()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var services = new ServiceCollection().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeTenancyOptions>>();
        options.CurrentValue.Returns(new LatticeTenancyOptions());
        var cluster = Options.Create(new ClusterOptions { ClusterId = "test-cluster" });
        var serializer = TestSerializers.TenantRecords;
        var initializer = new TenantRegistryInitializer(grainFactory, services, options, cluster, serializer);
        return new LatticeTenantRegistry(grainFactory, initializer, serializer);
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
