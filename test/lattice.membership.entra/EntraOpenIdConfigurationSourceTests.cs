using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Unit tests for <see cref="EntraOpenIdConfigurationSource"/>, the production
/// OIDC configuration source. These exercise only its caching and argument
/// contract; no network fetch happens because construction is lazy and the
/// configuration manager is not driven here.
/// </summary>
public class EntraOpenIdConfigurationSourceTests
{
    private const string Metadata =
        "https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration";

    private static EntraOpenIdConfigurationSource CreateSource() =>
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
        var second = source.GetOrCreate(
            "https://login.microsoftonline.com/other/v2.0/.well-known/openid-configuration");

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
