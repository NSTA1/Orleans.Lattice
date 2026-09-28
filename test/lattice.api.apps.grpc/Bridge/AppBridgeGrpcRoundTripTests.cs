using System.Collections.Immutable;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ClearExtensions;
using NSubstitute.ExceptionExtensions;
using NSubstitute.Extensions;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests.Bridge;

/// <summary>
/// Round trips every app bridge RPC through the real gRPC service, bounded marshallers, interceptor and client
/// against a substitute facade: the values, absence, the caller's credential reaching the facade, default deny,
/// the one-to-one failure mapping, sanitised messages, and the per-method size bound.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Measured under 1s for all 23 cases including TestServer lifecycle and a 1 MiB response; a substitute facade, no sockets or silo.")]
public sealed class AppBridgeGrpcRoundTripTests
{
    private static readonly AppBridgeTarget Target = new() { AppSlug = "crm", InstallRevision = 7, LogicalTree = "notes" };

    private WebApplication _app = null!;
    private GrpcChannel _channel = null!;
    private ServiceProvider _serializers = null!;
    private ILatticeAppBridge _bridge = null!;
    private ILatticeAppsApiAuthorizer _authorizer = null!;
    private LatticeAppBridgeApiGrpcClient _client = null!;
    private readonly List<(LatticeAppsApiOperation Operation, string? Slug, string Method)> _authorized = [];

    [OneTimeSetUp]
    public async Task Start()
    {
        _bridge = Substitute.For<ILatticeAppBridge>();
        _authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        var services = builder.Services;
        services.AddRouting();
        services.AddSerializer();
        services.AddSingleton(_bridge);
        services.AddSingleton(_authorizer);
        services.AddLatticeAppBridgeApiGrpc();
        services.AddLatticeAppBridgeApiGrpc();
        _app = builder.Build();
        _app.MapLatticeAppBridgeApiGrpc();
        await _app.StartAsync();
        _channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpHandler = _app.GetTestServer().CreateHandler(),
            MaxSendMessageSize = null,
            MaxReceiveMessageSize = null,
        });
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _client = LatticeAppBridgeApiGrpcClient.Create(_channel.CreateCallInvoker(), _serializers);
    }

    [SetUp]
    public void Reset()
    {
        _bridge.ClearReceivedCalls();
        _bridge.ClearSubstitute(ClearOptions.ReturnValues);
        _authorizer.ClearReceivedCalls();
        _authorized.Clear();
        _authorizer.Configure().IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(c =>
            {
                var authorization = c.Arg<LatticeAppsApiAuthorizationContext>();
                _authorized.Add((authorization.Operation, authorization.AppSlug, authorization.Call.Method));
                return true;
            });
    }

    [OneTimeTearDown]
    public async Task Stop()
    {
        _channel.Dispose();
        await _app.DisposeAsync();
        _serializers.Dispose();
    }

    [Test]
    public async Task Get_round_trips_a_value_and_absence()
    {
        _bridge.GetAsync(Arg.Is<AppBridgeTarget>(t => t == Target), "k", Arg.Any<CancellationToken>())
            .Returns(new AppBridgeValue { Key = "k", Value = new byte[] { 1, 2, 3 } });
        _bridge.GetAsync(Arg.Is<AppBridgeTarget>(t => t == Target), "missing", Arg.Any<CancellationToken>())
            .Returns((AppBridgeValue?)null);

        var found = await _client.GetAsync(Target, "k");
        var missing = await _client.GetAsync(Target, "missing");

        Assert.That(found!.Key, Is.EqualTo("k"));
        Assert.That(found.Value.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(missing, Is.Null);
        Assert.That(_authorized, Is.All.EqualTo((LatticeAppsApiOperation.BridgeGet, "crm", LatticeAppBridgeGrpcMethods.ServicePrefix + "Get")));
    }

    [Test]
    public async Task Scan_round_trips_the_request_and_the_page()
    {
        var page = new AppBridgePage
        {
            Entries = [new AppBridgeValue { Key = "n/1", Value = new byte[] { 1 } }, new AppBridgeValue { Key = "n/2", Value = new byte[] { 2 } }],
            Continuation = "k1:n/3",
        };
        _bridge.ScanAsync(Arg.Is<AppBridgeTarget>(t => t == Target), "n/", 2, "k1:n/1", Arg.Any<CancellationToken>()).Returns(page);

        var received = await _client.ScanAsync(Target, "n/", 2, "k1:n/1");

        Assert.That(received.Entries.Select(e => e.Key), Is.EqualTo(new[] { "n/1", "n/2" }));
        Assert.That(received.Entries[1].Value.ToArray(), Is.EqualTo(new byte[] { 2 }));
        Assert.That(received.Continuation, Is.EqualTo("k1:n/3"));
        Assert.That(_authorized, Is.EqualTo(new[] { (LatticeAppsApiOperation.BridgeScan, (string?)"crm", LatticeAppBridgeGrpcMethods.ServicePrefix + "Scan") }));
    }

    [Test]
    public async Task Scan_round_trips_a_first_and_last_page()
    {
        _bridge.ScanAsync(Arg.Any<AppBridgeTarget>(), string.Empty, 10, null, Arg.Any<CancellationToken>())
            .Returns(new AppBridgePage { Entries = ImmutableArray<AppBridgeValue>.Empty });

        var received = await _client.ScanAsync(Target, string.Empty, 10);

        Assert.That(received.Entries, Is.Empty);
        Assert.That(received.Continuation, Is.Null);
    }

    [Test]
    public async Task Set_round_trips_the_value_under_the_callers_credential()
    {
        byte[]? written = null;
        string? credential = null;
        _bridge.SetAsync(Arg.Any<AppBridgeTarget>(), "k", Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<CancellationToken>())
            .Returns(c =>
            {
                written = c.ArgAt<ReadOnlyMemory<byte>>(2).ToArray();
                credential = LatticeCredentialContext.Current?.Token;
                return Task.CompletedTask;
            });
        var invoker = _channel.CreateCallInvoker().Intercept(metadata =>
        {
            metadata.Add("authorization", "Bearer alice-token");
            return metadata;
        });

        await LatticeAppBridgeApiGrpcClient.Create(invoker, _serializers).SetAsync(Target, "k", new byte[] { 9, 8 });

        Assert.That(written, Is.EqualTo(new byte[] { 9, 8 }));
        Assert.That(credential, Is.EqualTo("alice-token"), "the facade runs under the caller's own credential");
        Assert.That(_authorized, Is.EqualTo(new[] { (LatticeAppsApiOperation.BridgeSet, (string?)"crm", LatticeAppBridgeGrpcMethods.ServicePrefix + "Set") }));
    }

    [Test]
    public async Task Delete_round_trips_whether_a_value_was_removed()
    {
        _bridge.DeleteAsync(Arg.Any<AppBridgeTarget>(), "present", Arg.Any<CancellationToken>()).Returns(true);
        _bridge.DeleteAsync(Arg.Any<AppBridgeTarget>(), "absent", Arg.Any<CancellationToken>()).Returns(false);

        Assert.That(await _client.DeleteAsync(Target, "present"), Is.True);
        Assert.That(await _client.DeleteAsync(Target, "absent"), Is.False);
        Assert.That(_authorized, Is.All.EqualTo((LatticeAppsApiOperation.BridgeDelete, "crm", LatticeAppBridgeGrpcMethods.ServicePrefix + "Delete")));
    }

    [TestCase("Get")]
    [TestCase("Scan")]
    [TestCase("Set")]
    [TestCase("Delete")]
    public void A_call_the_authorizer_refuses_is_denied_and_never_reaches_the_facade(string method)
    {
        _authorizer.Configure().IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>()).Returns(false);

        var error = Assert.ThrowsAsync<AppBridgeException>(() => Invoke(method));

        Assert.That(error!.Failure, Is.EqualTo(AppBridgeFailure.Denied));
        Assert.That(error.Message, Is.EqualTo(AppBridgeException.DefaultMessage(AppBridgeFailure.Denied)));
        Assert.That(_bridge.ReceivedCalls(), Is.Empty);
    }

    [TestCase(AppBridgeFailure.Denied)]
    [TestCase(AppBridgeFailure.NotFound)]
    [TestCase(AppBridgeFailure.Invalid)]
    [TestCase(AppBridgeFailure.TooLarge)]
    [TestCase(AppBridgeFailure.Conflict)]
    [TestCase(AppBridgeFailure.Unavailable)]
    public void Every_failure_code_round_trips_with_its_fixed_message(AppBridgeFailure failure)
    {
        _bridge.GetAsync(Arg.Any<AppBridgeTarget>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new AppBridgeException(failure, "facade detail naming t/acme/a/crm/notes"));

        var error = Assert.ThrowsAsync<AppBridgeException>(() => _client.GetAsync(Target, "k"));

        Assert.That(error!.Failure, Is.EqualTo(failure));
        Assert.That(error.Message, Is.EqualTo(AppBridgeException.DefaultMessage(failure)));
    }

    [TestCase(typeof(LatticeAuthorizationDeniedException), AppBridgeFailure.Denied)]
    [TestCase(typeof(LatticeTenantAccessDeniedException), AppBridgeFailure.Denied)]
    [TestCase(typeof(InvalidOperationException), AppBridgeFailure.Unavailable)]
    [TestCase(typeof(ArgumentException), AppBridgeFailure.Unavailable)]
    public void Any_other_facade_fault_maps_to_a_fixed_sanitised_failure(Type exceptionType, AppBridgeFailure expected)
    {
        var exception = (Exception)Activator.CreateInstance(exceptionType, "detail naming t/acme/a/crm/notes for alice")!;
        _bridge.DeleteAsync(Arg.Any<AppBridgeTarget>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(exception);

        var error = Assert.ThrowsAsync<AppBridgeException>(() => _client.DeleteAsync(Target, "k"));

        Assert.That(error!.Failure, Is.EqualTo(expected));
        Assert.That(error.Message, Does.Not.Contain("a/crm").And.Not.Contain("alice"));
    }

    [Test]
    public void A_write_over_the_per_method_bound_is_refused_before_the_facade()
    {
        var error = Assert.ThrowsAsync<AppBridgeException>(() =>
            _client.SetAsync(Target, "k", new byte[LatticeAppsGrpcMarshallers.MaxBridgeRequestBytes + 1]));

        Assert.That(error!.Failure, Is.AnyOf(AppBridgeFailure.TooLarge, AppBridgeFailure.Unavailable));
        Assert.That(_bridge.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void A_response_over_the_per_method_bound_is_refused()
    {
        _bridge.GetAsync(Arg.Any<AppBridgeTarget>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new AppBridgeValue { Key = "k", Value = new byte[LatticeAppsGrpcMarshallers.MaxBridgeResponseBytes + 1] });

        var error = Assert.ThrowsAsync<AppBridgeException>(() => _client.GetAsync(Target, "k"));

        Assert.That(error!.Failure, Is.AnyOf(AppBridgeFailure.TooLarge, AppBridgeFailure.Unavailable));
    }

    [Test]
    public void A_cancelled_call_surfaces_as_cancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.That(() => _client.GetAsync(Target, "k", cancellation.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void The_client_validates_arguments_before_calling()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetAsync(null!, "k"));
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetAsync(Target, null!));
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.ScanAsync(null!, string.Empty, 1));
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.ScanAsync(Target, null!, 1));
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.SetAsync(null!, "k", ReadOnlyMemory<byte>.Empty));
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.SetAsync(Target, null!, ReadOnlyMemory<byte>.Empty));
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.DeleteAsync(null!, "k"));
            Assert.ThrowsAsync<ArgumentNullException>(() => _client.DeleteAsync(Target, null!));
            Assert.Throws<ArgumentNullException>(() => LatticeAppBridgeApiGrpcClient.Create(null!, _serializers));
            Assert.Throws<ArgumentNullException>(() => LatticeAppBridgeApiGrpcClient.Create(_channel.CreateCallInvoker(), null!));
        });
        Assert.That(_authorizer.ReceivedCalls(), Is.Empty);
    }

    private Task Invoke(string method) => method switch
    {
        "Get" => _client.GetAsync(Target, "k"),
        "Scan" => _client.ScanAsync(Target, string.Empty, 10),
        "Set" => _client.SetAsync(Target, "k", new byte[] { 1 }),
        "Delete" => _client.DeleteAsync(Target, "k"),
        _ => throw new ArgumentException(method),
    };
}
