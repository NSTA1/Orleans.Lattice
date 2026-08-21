using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Unit tests for the schema control-API gRPC binding's small collaborators that
/// need neither a cluster nor a server: the default authorizers, the
/// options-backed auth-scheme source, the binding options, the authorization
/// context value, and additional credential-bridge decode branches. Kept
/// separate from the transport-level integration fixtures so they run in the
/// Tier 2 fast loop.
/// </summary>
[TestFixture]
public sealed class SchemaGrpcCollaboratorsUnitTests
{
    private static LatticeSchemaApiAuthorizationContext Ctx()
    {
        var call = new FakeServerCallContext(SchemaGrpcTestDoubles.FullMethod(LatticeSchemaGrpcMethods.GetPolicyMethodName));
        return new LatticeSchemaApiAuthorizationContext(call, LatticeSchemaApiOperation.GetPolicy, "orders");
    }

    [Test]
    public async Task DenySchemaApiAuthorizer_rejects_every_call()
    {
        var authorizer = new DenySchemaApiAuthorizer();

        Assert.That(await authorizer.IsAuthorizedAsync(Ctx(), CancellationToken.None), Is.False);
    }

    [Test]
    public async Task AllowAllSchemaApiAuthorizer_permits_every_call()
    {
        var authorizer = new AllowAllSchemaApiAuthorizer();

        Assert.That(await authorizer.IsAuthorizedAsync(Ctx(), CancellationToken.None), Is.True);
    }

    [Test]
    public void LatticeSchemaApiAuthorizationContext_rejects_a_null_call()
    {
        Assert.That(
            () => new LatticeSchemaApiAuthorizationContext(null!, LatticeSchemaApiOperation.GetPolicy, "orders"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void LatticeSchemaApiAuthorizationContext_exposes_its_operation_and_target()
    {
        var context = Ctx();

        Assert.Multiple(() =>
        {
            Assert.That(context.Operation, Is.EqualTo(LatticeSchemaApiOperation.GetPolicy));
            Assert.That(context.TargetId, Is.EqualTo("orders"));
            Assert.That(context.Call, Is.Not.Null);
        });
    }

    [Test]
    public void LatticeSchemaApiGrpcOptions_has_fail_closed_defaults()
    {
        var options = new LatticeSchemaApiGrpcOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("authorization"));
            Assert.That(options.CredentialScheme, Is.EqualTo("Bearer"));
            Assert.That(options.AdvertisedAuthSchemes, Is.Empty);
        });
    }

    private static OptionsLatticeSchemaApiAuthSchemeSource SourceFor(LatticeSchemaApiGrpcOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeSchemaApiGrpcOptions>>();
        monitor.CurrentValue.Returns(options);
        return new OptionsLatticeSchemaApiAuthSchemeSource(monitor);
    }

    [Test]
    public void OptionsAuthSchemeSource_rejects_a_null_monitor()
    {
        Assert.That(() => new OptionsLatticeSchemaApiAuthSchemeSource(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void OptionsAuthSchemeSource_advertises_nothing_when_none_configured()
    {
        var source = SourceFor(new LatticeSchemaApiGrpcOptions());

        Assert.That(source.GetAdvertisement().Schemes, Is.Empty);
    }

    [Test]
    public void OptionsAuthSchemeSource_advertises_the_configured_schemes()
    {
        var options = new LatticeSchemaApiGrpcOptions();
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" });
        var source = SourceFor(options);

        var advertisement = source.GetAdvertisement();

        Assert.Multiple(() =>
        {
            Assert.That(advertisement.Schemes, Has.Count.EqualTo(1));
            Assert.That(advertisement.Schemes[0].SchemeId, Is.EqualTo("entra"));
        });
    }

    private static HeaderLatticeSchemaApiCredentialBridge BridgeFor(LatticeSchemaApiGrpcOptions options) =>
        new(Options.Create(options));

    private static FakeServerCallContext ContextWithHeader(string headerName, string headerValue)
    {
        var headers = new global::Grpc.Core.Metadata { { headerName, headerValue } };
        return new FakeServerCallContext(SchemaGrpcTestDoubles.FullMethod(LatticeSchemaGrpcMethods.GetPolicyMethodName), headers);
    }

    [Test]
    public void CredentialBridge_rejects_a_null_options()
    {
        Assert.That(() => new HeaderLatticeSchemaApiCredentialBridge(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void CredentialBridge_resolve_rejects_a_null_context()
    {
        var bridge = BridgeFor(new LatticeSchemaApiGrpcOptions());
        Assert.That(() => bridge.Resolve(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void CredentialBridge_strips_the_scheme_prefix_and_keeps_the_token()
    {
        var bridge = BridgeFor(new LatticeSchemaApiGrpcOptions());

        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearer abc123"));

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("abc123"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void CredentialBridge_returns_null_when_the_header_is_absent()
    {
        var bridge = BridgeFor(new LatticeSchemaApiGrpcOptions());
        var context = new FakeServerCallContext(SchemaGrpcTestDoubles.FullMethod(LatticeSchemaGrpcMethods.GetPolicyMethodName));

        Assert.That(bridge.Resolve(context), Is.Null);
    }

    [Test]
    public void CredentialBridge_returns_null_for_a_bare_scheme_with_no_token()
    {
        var bridge = BridgeFor(new LatticeSchemaApiGrpcOptions());

        Assert.That(bridge.Resolve(ContextWithHeader("authorization", "Bearer")), Is.Null);
    }

    [Test]
    public void CredentialBridge_returns_null_when_the_header_name_is_empty()
    {
        var bridge = BridgeFor(new LatticeSchemaApiGrpcOptions { CredentialHeaderName = "" });

        Assert.That(bridge.Resolve(ContextWithHeader("authorization", "Bearer abc")), Is.Null);
    }

    [Test]
    public void CredentialBridge_keeps_the_whole_value_when_no_scheme_is_configured()
    {
        var bridge = BridgeFor(new LatticeSchemaApiGrpcOptions { CredentialScheme = "" });

        var credential = bridge.Resolve(ContextWithHeader("authorization", "raw-token"));

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("raw-token"));
            Assert.That(credential.Value.Scheme, Is.Null);
        });
    }
}

/// <summary>
/// Unit tests for the static <see cref="LatticeSchemaGrpcServiceBase.BindService"/>
/// binder hook, asserted directly against a recording
/// <see cref="ServiceBinderBase"/> - without an ASP.NET Core gRPC host - for both
/// the metadata-only (null service instance) and concrete-instance binding
/// passes, plus its fail-closed guards.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaGrpcServiceBaseBindServiceTests
{
    // 15 unary RPCs + 1 server-streaming (StreamDeadLetters) = 16 total.
    private const int ExpectedMethodCount = 16;

    private ServiceProvider _serializerProvider = null!;
    private LatticeSchemaGrpcMethods _methods = null!;
    private LatticeSchemaGrpcMethods? _savedHolder;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeSchemaGrpcMethods.FromServiceProvider(_serializerProvider);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    [SetUp]
    public void SetUp() => _savedHolder = LatticeSchemaGrpcMethodsHolder.Current;

    [TearDown]
    public void TearDown() => LatticeSchemaGrpcMethodsHolder.Current = _savedHolder;

    [Test]
    public void BindService_rejects_a_null_binder()
    {
        LatticeSchemaGrpcMethodsHolder.Current = _methods;

        Assert.That(
            () => LatticeSchemaGrpcServiceBase.BindService(null!, null),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_throws_when_the_methods_holder_is_uninitialised()
    {
        LatticeSchemaGrpcMethodsHolder.Current = null;
        var binder = new RecordingServiceBinder();

        Assert.That(
            () => LatticeSchemaGrpcServiceBase.BindService(binder, null),
            Throws.InvalidOperationException);
    }

    [Test]
    public void BindService_records_metadata_for_a_null_service_instance()
    {
        LatticeSchemaGrpcMethodsHolder.Current = _methods;
        var binder = new RecordingServiceBinder();

        LatticeSchemaGrpcServiceBase.BindService(binder, null);

        Assert.That(binder.AddedMethodCount, Is.EqualTo(ExpectedMethodCount));
    }

    [Test]
    public void BindService_binds_the_concrete_instance_handlers()
    {
        LatticeSchemaGrpcMethodsHolder.Current = _methods;
        var binder = new RecordingServiceBinder();
        var service = new LatticeSchemaGrpcService(
            _methods,
            Substitute.For<global::Orleans.Lattice.Api.Schema.ILatticeSchemaControl>(),
            Substitute.For<ILatticeSchemaApiCredentialBridge>(),
            Substitute.For<ILatticeSchemaApiAuthSchemeSource>(),
            NullLogger<LatticeSchemaGrpcService>.Instance);

        LatticeSchemaGrpcServiceBase.BindService(binder, service);

        Assert.That(binder.AddedMethodCount, Is.EqualTo(ExpectedMethodCount));
    }

    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public int AddedMethodCount { get; private set; }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method, UnaryServerMethod<TRequest, TResponse>? handler) =>
            AddedMethodCount++;

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method, ServerStreamingServerMethod<TRequest, TResponse>? handler) =>
            AddedMethodCount++;
    }
}
