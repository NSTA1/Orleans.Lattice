using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace Orleans.Lattice.Membership.Oidc.Tests;

/// <summary>
/// Unit tests for <see cref="OidcConfigurationSource"/>, the production OIDC
/// configuration source. These exercise only its caching, its refresh-interval
/// wiring, and its argument contract; no network fetch happens because
/// construction is lazy and the configuration manager is not driven here.
/// </summary>
public class OidcConfigurationSourceTests
{
    private const string Metadata = "https://idp.example.com/oauth2/default/.well-known/openid-configuration";

    private static OidcConfigurationSource CreateSource() =>
        new(TimeSpan.FromHours(12), TimeSpan.FromMinutes(5));

    [Test]
    public void GetOrCreate_returns_configuration_manager_for_address()
    {
        var source = CreateSource();

        var manager = source.GetOrCreate(Metadata);

        Assert.That(manager, Is.Not.Null);
        Assert.That(manager, Is.InstanceOf<ConfigurationManager<OpenIdConnectConfiguration>>());
    }

    [Test]
    public void GetOrCreate_applies_the_configured_refresh_intervals()
    {
        var source = new OidcConfigurationSource(TimeSpan.FromHours(3), TimeSpan.FromMinutes(7));

        var manager = (ConfigurationManager<OpenIdConnectConfiguration>)source.GetOrCreate(Metadata);

        Assert.That(manager.AutomaticRefreshInterval, Is.EqualTo(TimeSpan.FromHours(3)));
        Assert.That(manager.RefreshInterval, Is.EqualTo(TimeSpan.FromMinutes(7)));
    }

    [Test]
    public void GetOrCreate_caches_one_manager_per_address()
    {
        var source = CreateSource();

        var first = source.GetOrCreate(Metadata);
        var second = source.GetOrCreate(Metadata);

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void GetOrCreate_distinct_addresses_return_distinct_managers()
    {
        var source = CreateSource();

        var first = source.GetOrCreate(Metadata);
        var second = source.GetOrCreate("https://other-idp.example.com/.well-known/openid-configuration");

        Assert.That(second, Is.Not.SameAs(first));
    }

    [Test]
    public void GetOrCreate_null_address_throws()
    {
        var source = CreateSource();

        Assert.That(() => source.GetOrCreate(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void GetOrCreate_empty_address_throws()
    {
        var source = CreateSource();

        Assert.That(() => source.GetOrCreate(string.Empty), Throws.ArgumentException);
    }
}
