using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the access-data purge hook in
/// <see cref="LatticeTenantRegistry.DeleteAsync"/> (epic #4154, D12): the purge
/// runs before the tenant record is removed, a failed purge leaves the record in
/// place, and the uninitialised tenant is refused before either. Driven against a
/// substituted <see cref="ILattice"/> registry tree, with no live silo.
/// </summary>
[TestFixture]
public sealed class LatticeTenantRegistryDeletePurgeTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static (LatticeTenantRegistry Registry, ILattice Tree, ITenantAccessDataPurge Purge) Create()
    {
        var tree = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(tree);

        var services = new ServiceCollection().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeTenancyOptions>>();
        options.CurrentValue.Returns(new LatticeTenancyOptions { SeedDefaultTenant = false });
        var cluster = Options.Create(new ClusterOptions { ClusterId = "test-cluster" });
        var serializer = TestSerializers.TenantRecords;
        var initializer = new TenantRegistryInitializer(grainFactory, services, options, cluster, serializer);
        var purge = Substitute.For<ITenantAccessDataPurge>();
        return (new LatticeTenantRegistry(grainFactory, initializer, serializer, purge), tree, purge);
    }

    [Test]
    public async Task DeleteAsync_purges_the_tenants_access_data_before_removing_the_record()
    {
        var (registry, tree, purge) = Create();
        tree.DeleteAsync("acme", Arg.Any<CancellationToken>()).Returns(true);

        var removed = await registry.DeleteAsync(Acme);

        Assert.That(removed, Is.True);
        Received.InOrder(() =>
        {
            purge.PurgeAsync(Acme, Arg.Any<CancellationToken>());
            tree.DeleteAsync("acme", Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public void DeleteAsync_leaves_the_record_in_place_when_the_purge_fails()
    {
        var (registry, tree, purge) = Create();
        purge.PurgeAsync(Acme, Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("purge failed"));

        Assert.That(
            async () => await registry.DeleteAsync(Acme),
            Throws.InvalidOperationException.With.Message.EqualTo("purge failed"));
        _ = tree.DidNotReceive().DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteAsync_passes_the_callers_cancellation_token_to_the_purge()
    {
        var (registry, _, purge) = Create();
        using var cts = new CancellationTokenSource();

        await registry.DeleteAsync(Acme, cts.Token);

        _ = purge.Received(1).PurgeAsync(Acme, cts.Token);
    }

    [Test]
    public void DeleteAsync_with_the_no_tenant_value_throws_before_the_purge()
    {
        var (registry, _, purge) = Create();

        Assert.That(async () => await registry.DeleteAsync(default), Throws.ArgumentException);
        _ = purge.DidNotReceive().PurgeAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteAsync_of_an_unknown_tenant_still_purges_so_a_rerun_clears_stragglers()
    {
        var (registry, tree, purge) = Create();
        tree.DeleteAsync("acme", Arg.Any<CancellationToken>()).Returns(false);

        var removed = await registry.DeleteAsync(Acme);

        Assert.That(removed, Is.False);
        _ = purge.Received(1).PurgeAsync(Acme, Arg.Any<CancellationToken>());
    }
}
