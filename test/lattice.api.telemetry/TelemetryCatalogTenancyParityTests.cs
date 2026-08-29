namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Pins the epic's D3 decision: there is exactly <b>one</b> catalogue and one set of
/// panels across both deployment modes. A cluster with the tenancy add-on and one
/// without are served byte-identical entries, differing only in the tenant the
/// facade derives - which is why the derived <c>tenant</c> dimension is always
/// emitted in the first place.
/// </summary>
[TestFixture]
public sealed class TelemetryCatalogTenancyParityTests
{
    /// <summary>
    /// A resolver standing in for the tenancy add-on: it reports a real tenant, the
    /// way <c>TenantContextResolver</c> does once <c>lattice.tenancy</c> is
    /// registered.
    /// </summary>
    private static ITenantContextResolver TenancyOn => new StubTenantContextResolver(TenantId.Parse("acme"));

    /// <summary>
    /// The core no-op path taken when the tenancy add-on is absent: every tree id is
    /// bare, so every caller resolves to the reserved default tenant.
    /// </summary>
    private static ITenantContextResolver TenancyOff => NullTelemetryTenantContext.Instance;

    [Test]
    public async Task The_same_entries_are_served_with_and_without_tenancy_registered()
    {
        var withTenancy = await Catalog(TenancyOn);
        var withoutTenancy = await Catalog(TenancyOff);

        Assert.Multiple(() =>
        {
            Assert.That(
                withoutTenancy.Queries.Select(q => q.QueryId),
                Is.EqualTo(withTenancy.Queries.Select(q => q.QueryId)));
            Assert.That(withoutTenancy.Version, Is.EqualTo(withTenancy.Version));
            Assert.That(withoutTenancy.Count, Is.EqualTo(withTenancy.Count));
        });
    }

    [Test]
    public async Task Every_entry_is_described_identically_in_both_deployment_modes()
    {
        var withTenancy = await Catalog(TenancyOn);
        var withoutTenancy = await Catalog(TenancyOff);

        for (var i = 0; i < withTenancy.Count; i++)
        {
            Assert.That(withoutTenancy.Queries[i], Is.EqualTo(withTenancy.Queries[i]),
                "There are no tenancy-on / tenancy-off catalogue variants, so a client renders the "
                + "same panels either way.");
        }
    }

    [Test]
    public async Task The_tenancy_meter_entries_are_offered_even_when_the_add_on_is_absent()
    {
        var withoutTenancy = await Catalog(TenancyOff);

        Assert.Multiple(() =>
        {
            Assert.That(withoutTenancy.Contains("tenant.usage.bytes"), Is.True);
            Assert.That(withoutTenancy.Contains("tenant.quota.byte_utilization"), Is.True);
        });
    }

    [Test]
    public async Task The_same_query_text_is_rendered_in_both_modes_apart_from_the_derived_tenant()
    {
        const string queryId = "tree.read.operation_rate";

        var withTenancy = new TelemetryFacadeHarness().WithTenantResolver(TenancyOn);
        await withTenancy.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(queryId));

        var withoutTenancy = new TelemetryFacadeHarness().WithTenantResolver(TenancyOff);
        await withoutTenancy.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(queryId));

        Assert.That(
            withoutTenancy.Backend.SingleQuery.Replace(
                $$"""tenant="{{LatticeTenantLabel.DefaultTenant}}",""", """tenant="acme",""",
                StringComparison.Ordinal),
            Is.EqualTo(withTenancy.Backend.SingleQuery),
            "The only difference between the two modes is the tenant the facade derived.");
    }

    private static Task<TelemetryQueryCatalog> Catalog(ITenantContextResolver tenants) =>
        new TelemetryFacadeHarness().WithTenantResolver(tenants).Build().GetCatalogAsync();
}
