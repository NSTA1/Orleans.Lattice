using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for the binding's composition root -
/// <see cref="LatticeTenantAdminApiGrpcServiceCollectionExtensions"/> - and the
/// static <c>BindService</c> hook <c>Grpc.AspNetCore</c> reflects at startup.
/// Proves the registration is fail-closed by default (the default-deny
/// authorizer and <c>RequireAuthorization=true</c>), that every
/// host-overridable seam is registered with <c>TryAdd</c> so a host-supplied
/// implementation wins, that the method-definition factory populates the static
/// holder the binder depends on, and that the binder registers a handler for
/// every RPC on the service contract.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class LatticeTenantAdminApiGrpcRegistrationTests
{
    private LatticeTenantAdminGrpcMethods? _priorHolder;

    [SetUp]
    public void SetUp() => _priorHolder = LatticeTenantAdminGrpcMethodsHolder.Current;

    [TearDown]
    public void TearDown() => LatticeTenantAdminGrpcMethodsHolder.Current = _priorHolder;

    private static ServiceCollection HostServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSerializer();

        // The facade the binding adapts onto; a real host supplies it via
        // AddLatticeTenantAdminApi on a co-hosted Orleans silo.
        services.AddSingleton(Substitute.For<ILatticeTenantAdmin>());
        services.AddSingleton(Substitute.For<ILatticeTenantSelfService>());
        return services;
    }

    [Test]
    public void AddLatticeTenantAdminApiGrpc_rejects_a_null_service_collection()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddLatticeTenantAdminApiGrpc(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void MapLatticeTenantAdminApiGrpc_rejects_a_null_endpoint_builder()
    {
        Assert.That(
            () => ((Microsoft.AspNetCore.Routing.IEndpointRouteBuilder)null!).MapLatticeTenantAdminApiGrpc(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeTenantAdminApiGrpc_returns_the_same_collection_for_chaining()
    {
        var services = HostServices();

        var returned = services.AddLatticeTenantAdminApiGrpc();

        Assert.That(returned, Is.SameAs(services));
    }

    [Test]
    public void Registration_without_a_configure_delegate_uses_the_fail_closed_defaults()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeTenantAdminApiGrpcOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True,
                "the tenant lifecycle surface must default to enforcing authorization");
            Assert.That(options.CredentialHeaderName, Is.EqualTo("authorization"));
            Assert.That(options.CredentialScheme, Is.EqualTo("Bearer"));
            Assert.That(options.AdvertisedAuthSchemes, Is.Empty);
        });
    }

    [Test]
    public void Registration_applies_a_supplied_configure_delegate()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialHeaderName = "x-lattice-cred";
            o.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "bearer" });
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeTenantAdminApiGrpcOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.False);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("x-lattice-cred"));
            Assert.That(options.AdvertisedAuthSchemes.Select(s => s.SchemeId), Is.EqualTo(new[] { "bearer" }));
        });
    }

    [Test]
    public void Registration_defaults_to_the_deny_authorizer()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<ILatticeTenantAdminApiAuthorizer>();

        Assert.That(authorizer, Is.TypeOf<DenyTenantAdminApiAuthorizer>(),
            "the binding must fail closed until a host opts in");
    }

    [Test]
    public void A_host_registered_authorizer_is_preserved()
    {
        var services = HostServices();
        services.AddSingleton<ILatticeTenantAdminApiAuthorizer, AllowAllTenantAdminApiAuthorizer>();

        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        Assert.That(
            provider.GetRequiredService<ILatticeTenantAdminApiAuthorizer>(),
            Is.TypeOf<AllowAllTenantAdminApiAuthorizer>(),
            "TryAdd must not displace a host-supplied authorizer");
    }

    [Test]
    public void Registration_defaults_the_credential_bridge_and_auth_scheme_source()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(
                provider.GetRequiredService<ILatticeTenantAdminApiCredentialBridge>(),
                Is.TypeOf<HeaderLatticeTenantAdminApiCredentialBridge>());
            Assert.That(
                provider.GetRequiredService<ILatticeTenantAdminApiAuthSchemeSource>(),
                Is.TypeOf<OptionsLatticeTenantAdminApiAuthSchemeSource>());
        });
    }

    [Test]
    public void A_host_registered_credential_bridge_and_scheme_source_are_preserved()
    {
        var services = HostServices();
        services.AddSingleton<ILatticeTenantAdminApiCredentialBridge, NullCredentialBridge>();
        services.AddSingleton<ILatticeTenantAdminApiAuthSchemeSource>(
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()));

        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(
                provider.GetRequiredService<ILatticeTenantAdminApiCredentialBridge>(),
                Is.TypeOf<NullCredentialBridge>());
            Assert.That(
                provider.GetRequiredService<ILatticeTenantAdminApiAuthSchemeSource>(),
                Is.TypeOf<FixedAuthSchemeSource>());
        });
    }

    [Test]
    public void Registration_is_idempotent()
    {
        var services = HostServices();

        services.AddLatticeTenantAdminApiGrpc();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(
                services.Count(d => d.ServiceType == typeof(ILatticeTenantAdminApiAuthorizer)),
                Is.EqualTo(1));
            Assert.That(
                services.Count(d => d.ServiceType == typeof(LatticeTenantAdminGrpcMethods)),
                Is.EqualTo(1));
            Assert.That(provider.GetRequiredService<LatticeTenantAdminGrpcService>(), Is.Not.Null);
        });
    }

    [Test]
    public void Resolving_the_service_resolves_the_abstract_base_to_the_same_singleton()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        var concrete = provider.GetRequiredService<LatticeTenantAdminGrpcService>();
        var asBase = provider.GetRequiredService<LatticeTenantAdminGrpcServiceBase>();

        Assert.That(asBase, Is.SameAs(concrete),
            "Grpc.AspNetCore resolves the attribute-bearing base type per request");
    }

    /// <summary>
    /// The region-residency facade is a separate opt-in registration. A host that
    /// was binding tenant administration before the region RPCs existed registers
    /// only <c>ILatticeTenantAdmin</c> and <c>ILatticeTenantSelfService</c>, so the
    /// binding must still compose - a required constructor dependency here would
    /// turn an additive change into a startup break for every existing deployment.
    /// </summary>
    [Test]
    public void The_service_composes_without_the_optional_region_residency_facade()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetService<ILatticeTenantRegionAdmin>(), Is.Null,
            "the fixture models a host that never opted the region facade in.");
        Assert.That(
            () => provider.GetRequiredService<LatticeTenantAdminGrpcService>(),
            Throws.Nothing);
    }

    /// <summary>
    /// And the three region RPCs then answer honestly rather than faulting: an
    /// <c>Unimplemented</c> status is what a gRPC caller is specified to receive
    /// for a method this server does not serve.
    /// </summary>
    [Test]
    public void A_region_rpc_reports_unimplemented_when_the_facade_is_absent()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        var service = provider.GetRequiredService<LatticeTenantAdminGrpcService>();
        var context = new FakeServerCallContext(
            LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName);

        var fault = Assert.ThrowsAsync<RpcException>(async () =>
            await service.SetTenantResidency(
                new TenantAdminRegionSetRequest { TenantId = "acme", Regions = ["eu"] },
                context));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void Registration_registers_the_auth_interceptor()
    {
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<LatticeTenantAdminApiGrpcAuthInterceptor>(), Is.Not.Null);
    }

    [Test]
    public void Resolving_the_method_definitions_publishes_them_to_the_static_holder()
    {
        LatticeTenantAdminGrpcMethodsHolder.Current = null;
        var services = HostServices();
        services.AddLatticeTenantAdminApiGrpc();

        using var provider = services.BuildServiceProvider();
        var methods = provider.GetRequiredService<LatticeTenantAdminGrpcMethods>();

        Assert.That(LatticeTenantAdminGrpcMethodsHolder.Current, Is.SameAs(methods),
            "BindService reads the holder, so the DI factory must publish into it");
    }

    [Test]
    public void The_method_definitions_carry_the_reserved_service_name_on_every_rpc()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(provider);

        Assert.Multiple(() =>
        {
            Assert.That(methods.CreateTenant.ServiceName, Is.EqualTo(LatticeTenantAdminGrpcMethods.ServiceName));
            Assert.That(methods.CreateTenant.Type, Is.EqualTo(MethodType.Unary));
            Assert.That(methods.SuspendTenant.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.SuspendTenantMethodName));
            Assert.That(methods.ResumeTenant.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.ResumeTenantMethodName));
            Assert.That(methods.DeleteTenant.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName));
            Assert.That(methods.SetTenantQuotas.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.SetTenantQuotasMethodName));
            Assert.That(methods.GetAuthScheme.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.GetAuthSchemeMethodName));
            Assert.That(methods.GetCurrentTenant.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.GetCurrentTenantMethodName));
            Assert.That(methods.ListAccessibleTenants.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.ListAccessibleTenantsMethodName));
            Assert.That(methods.GetTenant.Name, Is.EqualTo(LatticeTenantAdminGrpcMethods.GetTenantMethodName));
        });
    }

    [Test]
    public void FromServiceProvider_rejects_a_null_provider()
    {
        Assert.That(
            () => LatticeTenantAdminGrpcMethods.FromServiceProvider(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_records_metadata_for_every_rpc_when_no_instance_is_supplied()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        LatticeTenantAdminGrpcMethodsHolder.Current =
            LatticeTenantAdminGrpcMethods.FromServiceProvider(provider);
        var binder = new CountingServiceBinder();

        LatticeTenantAdminGrpcServiceBase.BindService(binder, null);

        Assert.Multiple(() =>
        {
            Assert.That(binder.AddedMethods, Is.EqualTo(15));
            Assert.That(binder.BoundHandlers, Is.Zero,
                "the startup metadata pass binds no handler instance");
        });
    }

    [Test]
    public void BindService_binds_a_handler_for_every_rpc_when_an_instance_is_supplied()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(provider);
        LatticeTenantAdminGrpcMethodsHolder.Current = methods;
        var service = new LatticeTenantAdminGrpcService(
            methods,
            new FakeTenantAdmin(),
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance,
            new FakeTenantRegionAdmin(),
            new FakeTenantAccessAdmin());
        var binder = new CountingServiceBinder();

        LatticeTenantAdminGrpcServiceBase.BindService(binder, service);

        Assert.Multiple(() =>
        {
            Assert.That(binder.AddedMethods, Is.EqualTo(15));
            Assert.That(binder.BoundHandlers, Is.EqualTo(15));
        });
    }

    [Test]
    public void BindService_rejects_a_null_binder()
    {
        Assert.That(
            () => LatticeTenantAdminGrpcServiceBase.BindService(null!, null),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_fails_loudly_when_the_holder_was_never_populated()
    {
        LatticeTenantAdminGrpcMethodsHolder.Current = null;

        Assert.That(
            () => LatticeTenantAdminGrpcServiceBase.BindService(new CountingServiceBinder(), null),
            Throws.InvalidOperationException.With.Message.Contains("LatticeTenantAdminGrpcMethodsHolder"));
    }

    private sealed class CountingServiceBinder : ServiceBinderBase
    {
        public int AddedMethods { get; private set; }

        public int BoundHandlers { get; private set; }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
        {
            AddedMethods++;
            if (handler is not null)
            {
                BoundHandlers++;
            }
        }
    }
}
