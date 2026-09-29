using System.Text.Json;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.Extensions;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

/// <summary>
/// The additive <c>UpdateRoleBindings</c> RPC on the existing <c>orleans.lattice.api.apps</c>
/// service: it round-trips the version-pinned replacement, is classified and authorized as its
/// own operation before the facade is reached, maps facade failures like every other verb, and
/// answers Unimplemented on a host whose facade does not serve role re-binding.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Measured 0.269s for all 7 cases including TestServer lifecycle; fake facades, no sockets or silo.")]
public sealed class AppsGrpcRoleBindingsTests
{
    private static readonly AppRoleBindingsUpdate Request = new()
    {
        Slug = "demo",
        Version = "1.2.3",
        RoleBindings =
        [
            new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-readers" },
            new AppRoleBindingDescriptor { RoleName = "writer", GroupId = "g-writers" },
        ],
    };

    private static readonly AppRoleBindingsReport Report = new()
    {
        Slug = "demo",
        Version = "1.2.3",
        RoleBindings = Request.RoleBindings,
        State = AppLifecycleState.Enabled,
    };

    private WebApplication _app = null!;
    private GrpcChannel _channel = null!;
    private ServiceProvider _serializers = null!;
    private ILatticeAppRoleBindings _bindings = null!;
    private ILatticeAppsApiAuthorizer _authorizer = null!;
    private LatticeAppsApiGrpcClient _client = null!;
    private readonly List<(LatticeAppsApiOperation Operation, string? Slug, string Method)> _authorized = [];

    [OneTimeSetUp]
    public async Task Start()
    {
        _bindings = Substitute.For<ILatticeAppRoleBindings>();
        _authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        builder.Services.AddRouting();
        builder.Services.AddSerializer();
        builder.Services.AddSingleton(Substitute.For<ILatticeAppsControl>());
        builder.Services.AddSingleton(_bindings);
        builder.Services.AddSingleton(_authorizer);
        builder.Services.AddLatticeAppsApiGrpc();
        _app = builder.Build();
        _app.MapLatticeAppsApiGrpc();
        await _app.StartAsync();
        _channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions { HttpHandler = _app.GetTestServer().CreateHandler() });
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _client = LatticeAppsApiGrpcClient.Create(_channel.CreateCallInvoker(), _serializers);
    }

    [SetUp]
    public void Reset()
    {
        _bindings.ClearReceivedCalls();
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
    public async Task UpdateRoleBindings_round_trips_the_version_pinned_replacement_and_its_report()
    {
        _bindings.UpdateRoleBindingsAsync(Arg.Any<AppRoleBindingsUpdate>(), Arg.Any<CancellationToken>()).Returns(Report);

        var actual = await _client.UpdateRoleBindingsAsync(Request);

        AssertJson(actual, Report);
        AssertJson(_bindings.ReceivedCalls().Single().GetArguments()[0], Request);
        Assert.That(_authorized, Is.EqualTo(new[]
        {
            (LatticeAppsApiOperation.UpdateRoleBindings, (string?)"demo", LatticeAppsGrpcMethods.ServicePrefix + "UpdateRoleBindings"),
        }));
    }

    [Test]
    public void A_denied_call_never_reaches_the_facade()
    {
        _authorizer.Configure().IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var error = Assert.ThrowsAsync<RpcException>(() => _client.UpdateRoleBindingsAsync(Request));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(_bindings.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void Facade_failures_map_to_sanitized_statuses()
    {
        _bindings.UpdateRoleBindingsAsync(Arg.Any<AppRoleBindingsUpdate>(), Arg.Any<CancellationToken>())
            .Returns(
                Task.FromException<AppRoleBindingsReport>(new LatticeAuthorizationDeniedException("*", LatticeOperation.AppInstall, "root", "denied")),
                Task.FromException<AppRoleBindingsReport>(new ArgumentException("undeclared role")),
                Task.FromException<AppRoleBindingsReport>(new KeyNotFoundException("not installed")),
                Task.FromException<AppRoleBindingsReport>(new InvalidOperationException("t/acme/a/demo/notes version mismatch")));

        var statuses = new List<(StatusCode Code, string Detail)>();
        for (var i = 0; i < 4; i++)
        {
            var error = Assert.ThrowsAsync<RpcException>(() => _client.UpdateRoleBindingsAsync(Request));
            statuses.Add((error!.StatusCode, error.Status.Detail));
        }

        Assert.That(statuses.Select(s => s.Code), Is.EqualTo(new[]
        {
            StatusCode.PermissionDenied, StatusCode.InvalidArgument, StatusCode.NotFound, StatusCode.FailedPrecondition,
        }));
        Assert.That(statuses.Select(s => s.Detail), Has.None.Contains("t/acme"));
    }

    [Test]
    public void The_client_validates_its_argument_before_any_call()
    {
        Assert.ThrowsAsync<ArgumentNullException>(() => _client.UpdateRoleBindingsAsync(null!));
        Assert.That(_authorizer.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void A_host_whose_control_facade_does_not_rebind_answers_unimplemented()
    {
        var service = Service(Substitute.For<ILatticeAppsControl>(), roleBindings: null);

        var error = Assert.ThrowsAsync<RpcException>(() => service.UpdateRoleBindings(Request, Context()));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public async Task A_control_facade_that_also_rebinds_serves_the_rpc_when_none_is_registered()
    {
        var control = Substitute.For<ILatticeAppsControl, ILatticeAppRoleBindings>();
        ((ILatticeAppRoleBindings)control).UpdateRoleBindingsAsync(Request, Arg.Any<CancellationToken>()).Returns(Report);

        var report = await Service(control, roleBindings: null).UpdateRoleBindings(Request, Context());

        Assert.That(report, Is.SameAs(Report));
    }

    [Test]
    public void The_interceptor_classifies_only_the_matching_request_shape()
    {
        var method = LatticeAppsGrpcMethods.ServicePrefix + "UpdateRoleBindings";

        Assert.Multiple(() =>
        {
            Assert.That(LatticeAppsApiGrpcAuthInterceptor.DescribeCall(method, Request),
                Is.EqualTo((LatticeAppsApiOperation.UpdateRoleBindings, (string?)"demo")));
            Assert.That(LatticeAppsApiGrpcAuthInterceptor.DescribeCall(method, new AppsSlugRequest { Slug = "demo" }).Operation,
                Is.EqualTo(LatticeAppsApiOperation.Unknown));
            Assert.That((int)LatticeAppsApiOperation.UpdateRoleBindings, Is.EqualTo(23), "appended, never renumbered");
        });
    }

    private static LatticeAppsGrpcService Service(ILatticeAppsControl control, ILatticeAppRoleBindings? roleBindings)
    {
        var options = Options.Create(new LatticeAppsApiGrpcOptions());
        return new LatticeAppsGrpcService(
            control,
            new HeaderLatticeAppsApiCredentialBridge(options),
            new OptionsLatticeAppsApiAuthSchemeSource(Substitute.For<IOptionsMonitor<LatticeAppsApiGrpcOptions>>()),
            options,
            NullLogger<LatticeAppsGrpcService>.Instance,
            roleBindings);
    }

    private static TestCallContext Context() => new(LatticeAppsGrpcMethods.ServicePrefix + "UpdateRoleBindings");

    private static void AssertJson(object? actual, object expected)
        => Assert.That(JsonSerializer.Serialize(actual), Is.EqualTo(JsonSerializer.Serialize(expected)));
}
