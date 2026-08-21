using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Unit coverage for the smaller server-side seams of the binding: the
/// header-to-credential bridge's disabled-header short circuit, the options-backed
/// auth-scheme advertisement source, the no-configure registration overload, and
/// the non-null branch of the static service binder.
/// </summary>
[TestFixture]
public sealed class LatticeBackupApiGrpcBindingUnitTests
{
    [Test]
    public void HeaderBridge_returns_null_when_the_credential_header_name_is_disabled()
    {
        var bridge = new HeaderLatticeBackupApiCredentialBridge(
            Options.Create(new LatticeBackupApiGrpcOptions { CredentialHeaderName = string.Empty }));
        var headers = new global::Grpc.Core.Metadata { { "authorization", "Bearer tok" } };

        var credential = bridge.Resolve(new FakeServerCallContext("unit", headers));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void HeaderBridge_null_context_throws()
    {
        var bridge = new HeaderLatticeBackupApiCredentialBridge(
            Options.Create(new LatticeBackupApiGrpcOptions()));

        Assert.That(() => bridge.Resolve(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AuthSchemeSource_returns_an_empty_advertisement_when_none_configured()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeBackupApiGrpcOptions());
        var source = new OptionsLatticeBackupApiAuthSchemeSource(monitor);

        var advertisement = source.GetAdvertisement();

        Assert.That(advertisement.Schemes, Is.Empty);
    }

    [Test]
    public void AuthSchemeSource_advertises_the_configured_schemes()
    {
        var options = new LatticeBackupApiGrpcOptions();
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "bearer", DisplayName = "Bearer" });
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "basic" });
        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupApiGrpcOptions>>();
        monitor.CurrentValue.Returns(options);
        var source = new OptionsLatticeBackupApiAuthSchemeSource(monitor);

        var advertisement = source.GetAdvertisement();

        Assert.That(advertisement.Schemes.Select(s => s.SchemeId), Is.EqualTo(new[] { "bearer", "basic" }));
    }

    [Test]
    public void AddLatticeBackupApiGrpc_without_a_configure_delegate_registers_default_options()
    {
        var services = new ServiceCollection();
        services.AddSerializer();

        services.AddLatticeBackupApiGrpc();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeBackupApiGrpcOptions>>().Value;
        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("authorization"));
        });
    }

    [Test]
    public void BindService_with_a_service_instance_binds_every_method_handler()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeBackupGrpcMethods.FromServiceProvider(provider);
        LatticeBackupGrpcMethodsHolder.Current = methods;

        var service = new LatticeBackupGrpcService(
            methods,
            Substitute.For<ILatticeBackupControl>(),
            Substitute.For<ILatticeBackupApiCredentialBridge>(),
            Substitute.For<ILatticeBackupApiAuthSchemeSource>(),
            Substitute.For<Microsoft.Extensions.Logging.ILogger<LatticeBackupGrpcService>>());
        var binder = new CountingServiceBinder();

        LatticeBackupGrpcServiceBase.BindService(binder, service);

        Assert.That(binder.AddedMethods, Is.EqualTo(19));
    }

    [Test]
    public void BindService_null_binder_throws()
    {
        Assert.That(
            () => LatticeBackupGrpcServiceBase.BindService(null!, null),
            Throws.ArgumentNullException);
    }

    private sealed class CountingServiceBinder : ServiceBinderBase
    {
        public int AddedMethods { get; private set; }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler) => AddedMethods++;

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            ServerStreamingServerMethod<TRequest, TResponse>? handler) => AddedMethods++;
    }
}
