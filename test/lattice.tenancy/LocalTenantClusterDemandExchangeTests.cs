namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="LocalTenantClusterDemandExchange"/>.</summary>
public sealed class LocalTenantClusterDemandExchangeTests
{
    [Test]
    public async Task ExchangeAsync_always_returns_null_so_the_coordinator_falls_back_to_static_even()
    {
        var exchange = new LocalTenantClusterDemandExchange();

        var total = await exchange.ExchangeAsync(TenantId.Parse("acme"), localDemand: 42);

        Assert.That(total, Is.Null);
    }

    [Test]
    public async Task ExchangeAsync_returns_null_for_the_uninitialised_tenant_too()
    {
        var exchange = new LocalTenantClusterDemandExchange();

        var total = await exchange.ExchangeAsync(default, localDemand: 0);

        Assert.That(total, Is.Null);
    }
}
