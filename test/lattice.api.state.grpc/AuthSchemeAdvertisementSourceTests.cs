using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Verifies the options-backed auth-scheme advertisement source and the
/// interceptor's unauthenticated-method exemption. Together these prove the
/// probe advertises exactly what the host configured (nothing by default) and
/// that the advertisement RPC alone is reachable without a credential.
/// </summary>
[TestFixture]
public sealed class AuthSchemeAdvertisementSourceTests
{
    private static OptionsLatticeStateApiAuthSchemeSource CreateSource(LatticeStateApiGrpcOptions options)
        => new(new StaticOptionsMonitor<LatticeStateApiGrpcOptions>(options));

    [Test]
    public void Constructor_nullOptions_throws()
        => Assert.That(() => new OptionsLatticeStateApiAuthSchemeSource(null!), Throws.ArgumentNullException);

    [Test]
    public void GetAdvertisement_default_isEmpty()
    {
        var source = CreateSource(new LatticeStateApiGrpcOptions());

        Assert.That(source.GetAdvertisement().Schemes, Is.Empty);
    }

    [Test]
    public void GetAdvertisement_returnsConfiguredSchemes_inOrder()
    {
        var options = new LatticeStateApiGrpcOptions();
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" });
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "basic", DisplayName = "Basic" });

        var advertisement = CreateSource(options).GetAdvertisement();

        Assert.That(advertisement.Schemes.Select(s => s.SchemeId), Is.EqualTo(new[] { "entra", "basic" }));
    }

    [Test]
    public void GetAuthScheme_isExemptFromAuthorization()
    {
        var method = $"/{LatticeStateGrpcMethods.ServiceName}/{LatticeStateGrpcMethods.GetAuthSchemeMethodName}";

        Assert.That(LatticeStateApiGrpcAuthInterceptor.IsUnauthenticatedMethod(method), Is.True);
    }

    [Test]
    public void EveryOtherStateApiMethod_isEnforced()
    {
        var enforced = new[]
        {
            LatticeStateGrpcMethods.GetEntryMethodName,
            LatticeStateGrpcMethods.ScanEntriesMethodName,
            LatticeStateGrpcMethods.GetClusterInfoMethodName,
            LatticeStateGrpcMethods.ListTreesMethodName,
        };

        Assert.Multiple(() =>
        {
            foreach (var name in enforced)
            {
                var method = $"/{LatticeStateGrpcMethods.ServiceName}/{name}";
                Assert.That(
                    LatticeStateApiGrpcAuthInterceptor.IsUnauthenticatedMethod(method),
                    Is.False,
                    $"{name} must require authorization");
            }
        });
    }

    private sealed class StaticOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; } = value;

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
