using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Coverage for the binding's DI registration: what it registers, that it is
/// idempotent, that it fails closed by default, and that a host-supplied
/// replacement registered first is preserved rather than clobbered.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryApiGrpcRegistrationTests
{
    private static IServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.ClearProviders());
        services.AddSerializer();
        services.AddSingleton<ILatticeTelemetry>(new FakeTelemetry());
        return services;
    }

    [Test]
    public void AddLatticeTelemetryApiGrpc_rejects_a_null_service_collection()
        => Assert.That(
            () => LatticeTelemetryApiGrpcServiceCollectionExtensions.AddLatticeTelemetryApiGrpc(null!),
            Throws.ArgumentNullException);

    [Test]
    public void MapLatticeTelemetryApiGrpc_rejects_a_null_endpoint_builder()
        => Assert.That(
            () => LatticeTelemetryApiGrpcServiceCollectionExtensions.MapLatticeTelemetryApiGrpc(null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_default_authorizer_is_the_deny_all_one()
    {
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeTelemetryApiAuthorizer>(),
            Is.InstanceOf<DenyTelemetryApiAuthorizer>(),
            "A host that maps the surface without configuring authorization must fail closed.");
    }

    [Test]
    public void A_host_registered_authorizer_is_preserved()
    {
        var services = BaseServices();
        services.AddSingleton<ILatticeTelemetryApiAuthorizer, AllowAllTelemetryApiAuthorizer>();
        using var provider = services.AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeTelemetryApiAuthorizer>(),
            Is.InstanceOf<AllowAllTelemetryApiAuthorizer>());
    }

    [Test]
    public void The_default_credential_bridge_is_the_header_one()
    {
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeTelemetryApiCredentialBridge>(),
            Is.InstanceOf<HeaderLatticeTelemetryApiCredentialBridge>());
    }

    [Test]
    public void A_host_registered_credential_bridge_is_preserved()
    {
        var services = BaseServices();
        services.AddSingleton<ILatticeTelemetryApiCredentialBridge, NullCredentialBridge>();
        using var provider = services.AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeTelemetryApiCredentialBridge>(),
            Is.InstanceOf<NullCredentialBridge>());
    }

    [Test]
    public void The_default_auth_scheme_source_advertises_nothing()
    {
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        var source = provider.GetRequiredService<ILatticeTelemetryApiAuthSchemeSource>();

        Assert.Multiple(() =>
        {
            Assert.That(source, Is.InstanceOf<OptionsLatticeTelemetryApiAuthSchemeSource>());
            Assert.That(source.GetAdvertisement().Schemes, Is.Empty);
        });
    }

    [Test]
    public void The_options_backed_auth_scheme_source_serves_configured_schemes()
    {
        using var provider = BaseServices()
            .AddLatticeTelemetryApiGrpc(o => o.AdvertisedAuthSchemes.Add(
                new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" }))
            .BuildServiceProvider();

        var advertisement = provider.GetRequiredService<ILatticeTelemetryApiAuthSchemeSource>().GetAdvertisement();

        Assert.Multiple(() =>
        {
            Assert.That(advertisement.Schemes, Has.Count.EqualTo(1));
            Assert.That(advertisement.Schemes[0].SchemeId, Is.EqualTo("entra"));
        });
    }

    [Test]
    public void The_empty_advertisement_is_served_from_a_cached_singleton()
    {
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();
        var source = provider.GetRequiredService<ILatticeTelemetryApiAuthSchemeSource>();

        Assert.That(
            source.GetAdvertisement(),
            Is.SameAs(source.GetAdvertisement()),
            "The unauthenticated probe is the cheapest call on the surface; it must not allocate.");
    }

    [Test]
    public void The_configure_delegate_is_applied()
    {
        using var provider = BaseServices()
            .AddLatticeTelemetryApiGrpc(o =>
            {
                o.RequireAuthorization = false;
                o.CredentialHeaderName = "x-token";
            })
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeTelemetryApiGrpcOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.False);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("x-token"));
        });
    }

    [Test]
    public void The_service_and_its_base_resolve_to_the_same_instance()
    {
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<LatticeTelemetryGrpcServiceBase>(),
            Is.SameAs(provider.GetRequiredService<LatticeTelemetryGrpcService>()));
    }

    [Test]
    public void Resolving_the_methods_singleton_populates_the_bind_service_holder()
    {
        LatticeTelemetryGrpcMethodsHolder.Current = null;
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        var methods = provider.GetRequiredService<LatticeTelemetryGrpcMethods>();

        Assert.That(LatticeTelemetryGrpcMethodsHolder.Current, Is.SameAs(methods));
    }

    [Test]
    public void Resolving_the_service_forces_the_holder_to_be_populated_first()
    {
        LatticeTelemetryGrpcMethodsHolder.Current = null;
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        _ = provider.GetRequiredService<LatticeTelemetryGrpcService>();

        Assert.That(
            LatticeTelemetryGrpcMethodsHolder.Current,
            Is.Not.Null,
            "BindService is static and cannot take DI dependencies, so the service constructor is "
            + "what guarantees the holder is populated before the binder reflects on the type.");
    }

    [Test]
    public void Registration_is_idempotent()
    {
        using var provider = BaseServices()
            .AddLatticeTelemetryApiGrpc()
            .AddLatticeTelemetryApiGrpc()
            .BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetServices<ILatticeTelemetryApiAuthorizer>().Count(), Is.EqualTo(1));
            Assert.That(provider.GetServices<ILatticeTelemetryApiCredentialBridge>().Count(), Is.EqualTo(1));
            Assert.That(provider.GetServices<ILatticeTelemetryApiAuthSchemeSource>().Count(), Is.EqualTo(1));
            Assert.That(provider.GetServices<LatticeTelemetryGrpcMethods>().Count(), Is.EqualTo(1));
        });
    }

    [Test]
    public void The_auth_interceptor_is_resolvable()
    {
        using var provider = BaseServices().AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        Assert.That(provider.GetService<LatticeTelemetryApiGrpcAuthInterceptor>(), Is.Not.Null);
    }

    [Test]
    public void The_binding_resolves_on_a_minimal_host_with_only_the_facade_present()
    {
        // The facade's own registration resolves the access gate, membership
        // context, and tenant-context resolver optionally, so a minimal host still
        // gets a working fail-closed facade. The binding must make the same
        // assumption: ILatticeTelemetry and nothing else.
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.ClearProviders());
        services.AddSerializer();
        services.AddSingleton<ILatticeTelemetry>(new FakeTelemetry());

        using var provider = services.AddLatticeTelemetryApiGrpc().BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<LatticeTelemetryGrpcService>(), Is.Not.Null);
            Assert.That(provider.GetRequiredService<LatticeTelemetryGrpcMethods>(), Is.Not.Null);
        });
    }

    [Test]
    public void The_service_depends_on_the_facade_contract_and_nothing_else_from_the_host()
    {
        // A guard on the constructor rather than on the container: the only host
        // collaborator the service takes is ILatticeTelemetry. Adding an
        // ILatticeAccessGate / ILatticeMembershipContext / ITenantContextResolver
        // dependency here would both break a minimal host and start re-implementing
        // enforcement outside the facade.
        var hostDependencies = typeof(LatticeTelemetryGrpcService)
            .GetConstructors()
            .SelectMany(ctor => ctor.GetParameters())
            .Select(parameter => parameter.ParameterType.Name)
            .Where(name => name.StartsWith("ILattice", StringComparison.Ordinal))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            hostDependencies,
            Is.EqualTo(new[]
            {
                nameof(ILatticeTelemetry),
                nameof(ILatticeTelemetryApiAuthSchemeSource),
                nameof(ILatticeTelemetryApiCredentialBridge),
            }),
            "Beyond the facade contract, the service may only take seams this binding itself owns.");
    }

    [Test]
    public void BindService_without_a_populated_holder_fails_loudly()
    {
        LatticeTelemetryGrpcMethodsHolder.Current = null;

        Assert.That(
            () => LatticeTelemetryGrpcServiceBase.BindService(new NoOpServiceBinder(), null),
            Throws.InvalidOperationException);
    }

    [Test]
    public void BindService_rejects_a_null_binder()
        => Assert.That(
            () => LatticeTelemetryGrpcServiceBase.BindService(null!, null),
            Throws.ArgumentNullException);

    [Test]
    public void BindService_records_every_rpc_during_metadata_discovery()
    {
        using var serializers = TelemetryGrpcTestSupport.Serializers();
        LatticeTelemetryGrpcMethodsHolder.Current = TelemetryGrpcTestSupport.Methods(serializers);
        var binder = new NoOpServiceBinder();

        LatticeTelemetryGrpcServiceBase.BindService(binder, null);

        Assert.That(
            binder.Bound.OrderBy(name => name, StringComparer.Ordinal),
            Is.EqualTo(new[]
            {
                LatticeTelemetryGrpcMethods.GetAuthSchemeMethodName,
                LatticeTelemetryGrpcMethods.GetCatalogMethodName,
                LatticeTelemetryGrpcMethods.QueryMethodName,
            }));
    }

    [Test]
    public void BindService_binds_every_rpc_to_the_service_instance()
    {
        using var serializers = TelemetryGrpcTestSupport.Serializers();
        var service = TelemetryGrpcTestSupport.Service(serializers, new FakeTelemetry());
        LatticeTelemetryGrpcMethodsHolder.Current = TelemetryGrpcTestSupport.Methods(serializers);
        var binder = new NoOpServiceBinder();

        LatticeTelemetryGrpcServiceBase.BindService(binder, service);

        Assert.That(binder.Bound, Has.Count.EqualTo(3));
    }

    /// <summary>
    /// A <see cref="global::Grpc.Core.ServiceBinderBase"/> that records which method
    /// names were bound, so the binding hook can be exercised without a host.
    /// </summary>
    private sealed class NoOpServiceBinder : global::Grpc.Core.ServiceBinderBase
    {
        public List<string> Bound { get; } = [];

        public override void AddMethod<TRequest, TResponse>(
            global::Grpc.Core.Method<TRequest, TResponse> method,
            global::Grpc.Core.UnaryServerMethod<TRequest, TResponse>? handler)
            => Bound.Add(method.Name);
    }
}
