using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

/// <summary>
/// Default-deny, classification and registration coverage for the catalogue and workspace gRPC bindings, plus
/// the bounded asset marshaller.
/// </summary>
[TestFixture]
public sealed class AppCatalogWorkspaceGrpcSecurityTests
{
    [TestCase(LatticeAppCatalogGrpcMethods.ServicePrefix + "ListSources")]
    [TestCase(LatticeAppWorkspaceGrpcMethods.ServicePrefix + "ListMyApps")]
    public async Task Registration_is_default_deny_for_the_new_services(string method)
    {
        using var services = new ServiceCollection().AddLogging().AddLatticeAppCatalogApiGrpc().AddLatticeAppWorkspaceApiGrpc().BuildServiceProvider();
        var authorizer = services.GetRequiredService<ILatticeAppsApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<DenyAppsApiAuthorizer>());
        Assert.That(await authorizer.IsAuthorizedAsync(default, default), Is.False);

        var interceptor = services.GetRequiredService<LatticeAppsApiGrpcAuthInterceptor>();
        var error = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new AppsEmptyRequest(), new TestCallContext(method), (_, _) => Task.FromResult(new AppsSourcesResponse())));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Registration_is_idempotent_and_keeps_a_custom_authorizer()
    {
        var custom = Substitute.For<ILatticeAppsApiAuthorizer>();
        var services = new ServiceCollection().AddLogging().AddSingleton(custom);

        services.AddLatticeAppCatalogApiGrpc().AddLatticeAppCatalogApiGrpc().AddLatticeAppWorkspaceApiGrpc().AddLatticeAppWorkspaceApiGrpc();

        Assert.That(services.Count(d => d.ServiceType == typeof(LatticeAppCatalogGrpcService)), Is.EqualTo(1));
        Assert.That(services.Count(d => d.ServiceType == typeof(LatticeAppWorkspaceGrpcService)), Is.EqualTo(1));
        Assert.That(services.Count(d => d.ServiceType == typeof(LatticeAppCatalogGrpcMethods)), Is.EqualTo(1));
        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeAppsApiAuthorizer>(), Is.SameAs(custom));
        var catalogOptions = provider.GetRequiredService<IOptions<global::Grpc.AspNetCore.Server.GrpcServiceOptions<LatticeAppCatalogGrpcService>>>().Value;
        var workspaceOptions = provider.GetRequiredService<IOptions<global::Grpc.AspNetCore.Server.GrpcServiceOptions<LatticeAppWorkspaceGrpcService>>>().Value;
        Assert.That(catalogOptions.Interceptors.Count(i => i.Type == typeof(LatticeAppsApiGrpcAuthInterceptor)), Is.EqualTo(1));
        Assert.That(workspaceOptions.Interceptors.Count(i => i.Type == typeof(LatticeAppsApiGrpcAuthInterceptor)), Is.EqualTo(1));
    }

    [Test]
    public void Registration_and_mapping_reject_null_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppCatalogApiGrpc());
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppWorkspaceApiGrpc());
        Assert.Throws<ArgumentNullException>(() => ((Microsoft.AspNetCore.Routing.IEndpointRouteBuilder)null!).MapLatticeAppCatalogApiGrpc());
        Assert.Throws<ArgumentNullException>(() => ((Microsoft.AspNetCore.Routing.IEndpointRouteBuilder)null!).MapLatticeAppWorkspaceApiGrpc());
    }

    [TestCase(LatticeAppCatalogGrpcMethods.ServicePrefix + "ListSources", typeof(AppsSlugRequest))]
    [TestCase(LatticeAppCatalogGrpcMethods.ServicePrefix + "DescribeFromSource", typeof(AppsSlugRequest))]
    [TestCase(LatticeAppCatalogGrpcMethods.ServicePrefix + "Unknown", typeof(AppsEmptyRequest))]
    [TestCase(LatticeAppWorkspaceGrpcMethods.ServicePrefix + "GetUiAsset", typeof(AppsSlugRequest))]
    [TestCase(LatticeAppWorkspaceGrpcMethods.ServicePrefix + "GetAuthScheme", typeof(AuthSchemeAdvertisementRequest))]
    public void Unknown_or_mismatched_requests_fail_closed_even_when_enforcement_is_disabled(string method, Type requestType)
    {
        var request = requestType == typeof(AppsSlugRequest)
            ? (object)new AppsSlugRequest { Slug = "demo" }
            : Activator.CreateInstance(requestType)!;
        var interceptor = Create(Substitute.For<ILatticeAppsApiAuthorizer>(), required: false);

        var error = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            request, new TestCallContext(method), (_, _) => Task.FromResult(new AppsSourcesResponse())));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Every_new_method_is_classified_from_its_bound_method_and_request_shape()
    {
        var cases = new (string Method, object Request, LatticeAppsApiOperation Operation, string? Slug)[]
        {
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "ListSources", new AppsEmptyRequest(), LatticeAppsApiOperation.ListSources, null),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "ListAvailable", new AvailableAppQuery(), LatticeAppsApiOperation.ListAvailable, null),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "DescribeFromSource", new AppsSourceAppRequest { SourceKey = "feed", Slug = "demo" }, LatticeAppsApiOperation.DescribeFromSource, "demo"),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "GetIcon", new AppsSourceAppRequest { SourceKey = "feed", Slug = "demo" }, LatticeAppsApiOperation.GetSourceIcon, "demo"),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "GetCapabilities", new AppsEmptyRequest(), LatticeAppsApiOperation.GetCatalogCapabilities, null),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "ListMyApps", new AppsEmptyRequest(), LatticeAppsApiOperation.ListMyApps, null),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "DescribeMyApp", new AppsSlugRequest { Slug = "demo" }, LatticeAppsApiOperation.DescribeMyApp, "demo"),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "GetIcon", new AppsSlugRequest { Slug = "demo" }, LatticeAppsApiOperation.GetMyAppIcon, "demo"),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "GetUiAsset", new AppsUiAssetRequest { Slug = "demo", Path = "a.js" }, LatticeAppsApiOperation.GetUiAsset, "demo"),
        };

        foreach (var (method, request, operation, slug) in cases)
        {
            Assert.That(LatticeAppsApiGrpcAuthInterceptor.DescribeCall(method, request), Is.EqualTo((operation, slug)), method);
        }
    }

    [Test]
    public void The_bounded_marshaller_round_trips_a_message_within_the_bound_and_refuses_one_over_it()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<AppsUiAssetResponse>>();
        var marshaller = LatticeAppsGrpcMarshallers.CreateBounded(serializer, 1024);
        var small = new AppsUiAssetResponse { Asset = new AppUiAsset { Path = "a.js", Bytes = new byte[16], MediaType = "text/javascript", Sha256 = new string('f', 64) } };
        var large = small with { Asset = small.Asset! with { Bytes = new byte[4096] } };

        var writer = new BufferSerializationContext();
        marshaller.ContextualSerializer(small, writer);
        var bytes = writer.Written.ToArray();
        Assert.That(writer.Completed, Is.True);
        Assert.That(marshaller.ContextualDeserializer(new BufferDeserializationContext(bytes)).Asset!.Bytes.Length, Is.EqualTo(16));
        var tooLarge = Assert.Throws<RpcException>(() => marshaller.ContextualSerializer(large, new BufferSerializationContext()));
        Assert.That(tooLarge!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
        var tooLargeIn = Assert.Throws<RpcException>(() => marshaller.ContextualDeserializer(new BufferDeserializationContext(new byte[2048])));
        Assert.That(tooLargeIn!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
    }

    [Test]
    public void The_bounded_marshaller_rejects_invalid_arguments()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<AppsUiAssetResponse>>();

        Assert.Throws<ArgumentNullException>(() => LatticeAppsGrpcMarshallers.CreateBounded<AppsUiAssetResponse>(null!, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => LatticeAppsGrpcMarshallers.CreateBounded(serializer, 0));
    }

    [Test]
    public void The_asset_bound_is_the_bundle_asset_cap_plus_a_small_envelope() =>
        Assert.That(LatticeAppsGrpcMarshallers.MaxAssetMessageBytes, Is.InRange(2 * 1024 * 1024, (2 * 1024 * 1024) + (64 * 1024)));

    private static LatticeAppsApiGrpcAuthInterceptor Create(ILatticeAppsApiAuthorizer authorizer, bool required = true)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeAppsApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeAppsApiGrpcOptions { RequireAuthorization = required });
        return new(authorizer, options, NullLogger<LatticeAppsApiGrpcAuthInterceptor>.Instance);
    }

    private sealed class BufferSerializationContext : global::Grpc.Core.SerializationContext
    {
        private readonly System.Buffers.ArrayBufferWriter<byte> _writer = new();

        public ReadOnlyMemory<byte> Written => _writer.WrittenMemory;

        public bool Completed { get; private set; }

        public override void Complete(byte[] payload) => throw new NotSupportedException();

        public override System.Buffers.IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void SetPayloadLength(int payloadLength)
        {
        }

        public override void Complete() => Completed = true;
    }

    private sealed class BufferDeserializationContext(byte[] payload) : global::Grpc.Core.DeserializationContext
    {
        public override int PayloadLength => payload.Length;

        public override byte[] PayloadAsNewBuffer() => [.. payload];

        public override System.Buffers.ReadOnlySequence<byte> PayloadAsReadOnlySequence() => new(payload);
    }
}