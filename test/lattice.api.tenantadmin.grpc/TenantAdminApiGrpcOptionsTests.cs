using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAdminApiGrpcOptions"/> defaults - the
/// fail-closed posture (<see cref="LatticeTenantAdminApiGrpcOptions.RequireAuthorization"/>
/// defaults to <see langword="true"/>), the default credential header / scheme,
/// and an empty advertisement set - and for the options-backed
/// <see cref="OptionsLatticeTenantAdminApiAuthSchemeSource"/>.
/// </summary>
[TestFixture]
public sealed class TenantAdminApiGrpcOptionsTests
{
    [Test]
    public void Defaults_are_fail_closed_and_bearer_authorization()
    {
        var options = new LatticeTenantAdminApiGrpcOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("authorization"));
            Assert.That(options.CredentialScheme, Is.EqualTo("Bearer"));
            Assert.That(options.AdvertisedAuthSchemes, Is.Empty);
        });
    }

    [Test]
    public void AuthSchemeSource_advertises_nothing_by_default()
    {
        var source = new OptionsLatticeTenantAdminApiAuthSchemeSource(
            new StaticOptionsMonitor(new LatticeTenantAdminApiGrpcOptions()));

        Assert.That(source.GetAdvertisement().Schemes, Is.Empty);
    }

    [Test]
    public void AuthSchemeSource_advertises_the_configured_schemes()
    {
        var options = new LatticeTenantAdminApiGrpcOptions();
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "basic", DisplayName = "Basic" });
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" });

        var source = new OptionsLatticeTenantAdminApiAuthSchemeSource(new StaticOptionsMonitor(options));

        var advertised = source.GetAdvertisement().Schemes;
        Assert.Multiple(() =>
        {
            Assert.That(advertised, Has.Count.EqualTo(2));
            Assert.That(advertised[0].SchemeId, Is.EqualTo("basic"));
            Assert.That(advertised[1].SchemeId, Is.EqualTo("entra"));
        });
    }

    [Test]
    public void AuthSchemeSource_rejects_a_null_options_monitor()
    {
        Assert.That(() => new OptionsLatticeTenantAdminApiAuthSchemeSource(null!), Throws.ArgumentNullException);
    }

    /// <summary>A trivial fixed <see cref="IOptionsMonitor{T}"/> over a single value.</summary>
    private sealed class StaticOptionsMonitor(LatticeTenantAdminApiGrpcOptions value)
        : IOptionsMonitor<LatticeTenantAdminApiGrpcOptions>
    {
        public LatticeTenantAdminApiGrpcOptions CurrentValue { get; } = value;

        public LatticeTenantAdminApiGrpcOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<LatticeTenantAdminApiGrpcOptions, string?> listener) => null;
    }
}
