using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure.Tests;

/// <summary>
/// Tests for
/// <see cref="LatticeAzureTelemetryTokenServiceCollectionExtensions.AddAzureTelemetryBackendToken"/>:
/// it registers the Azure provider as the single
/// <see cref="ITelemetryBackendTokenProvider"/>, binds the options, and rejects
/// null arguments.
/// </summary>
[TestFixture]
public sealed class LatticeAzureTelemetryTokenServiceCollectionExtensionsTests
{
    [Test]
    public void Registers_the_azure_provider_as_the_backend_token_provider()
    {
        var services = new ServiceCollection();
        services.AddAzureTelemetryBackendToken(o =>
            o.Credential = new FakeTokenCredential(_ => new("t", DateTimeOffset.MaxValue)));

        using var provider = services.BuildServiceProvider();
        var tokenProvider = provider.GetRequiredService<ITelemetryBackendTokenProvider>();

        Assert.That(tokenProvider, Is.InstanceOf<AzureTelemetryBackendTokenProvider>());
    }

    [Test]
    public void The_provider_is_a_singleton()
    {
        var services = new ServiceCollection();
        services.AddAzureTelemetryBackendToken(o =>
            o.Credential = new FakeTokenCredential(_ => new("t", DateTimeOffset.MaxValue)));

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ITelemetryBackendTokenProvider>();
        var second = provider.GetRequiredService<ITelemetryBackendTokenProvider>();

        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void The_configure_delegate_binds_the_options()
    {
        var services = new ServiceCollection();
        services.AddAzureTelemetryBackendToken(o =>
        {
            o.Credential = new FakeTokenCredential(_ => new("t", DateTimeOffset.MaxValue));
            o.Scope = "https://custom.example/.default";
            o.RefreshSkew = TimeSpan.FromMinutes(2);
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<AzureTelemetryBackendTokenOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.Scope, Is.EqualTo("https://custom.example/.default"));
            Assert.That(options.RefreshSkew, Is.EqualTo(TimeSpan.FromMinutes(2)));
        });
    }

    [Test]
    public void A_host_registered_provider_is_not_overridden()
    {
        var services = new ServiceCollection();
        var custom = new StubProvider();
        services.AddSingleton<ITelemetryBackendTokenProvider>(custom);
        services.AddAzureTelemetryBackendToken(o =>
            o.Credential = new FakeTokenCredential(_ => new("t", DateTimeOffset.MaxValue)));

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ITelemetryBackendTokenProvider>(), Is.SameAs(custom));
    }

    [Test]
    public void Null_services_are_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => LatticeAzureTelemetryTokenServiceCollectionExtensions.AddAzureTelemetryBackendToken(
                services: null!, o => { }));

    [Test]
    public void A_null_configure_delegate_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAzureTelemetryBackendToken(configure: null!));

    private sealed class StubProvider : ITelemetryBackendTokenProvider
    {
        public ValueTask<string> GetAccessTokenAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult("stub");
    }
}
