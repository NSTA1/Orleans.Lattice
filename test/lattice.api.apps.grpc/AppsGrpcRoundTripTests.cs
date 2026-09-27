using System.Collections.Immutable;
using System.Text.Json;
using Grpc.Core;
using GrpcMetadata = Grpc.Core.Metadata;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.Extensions;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

[TestFixture]
[FastInProcessHostFixture("Measured 0.903s for all 36 cases including TestServer lifecycle; fake facade, no sockets or silo.")]
public sealed partial class AppsGrpcRoundTripTests
{
    private WebApplication _app = null!;
    private GrpcChannel _channel = null!;
    private ServiceProvider _serializers = null!;
    private ILatticeAppsControl _control = null!;
    private ILatticeAppsApiAuthorizer _authorizer = null!;
    private LatticeAppsApiGrpcClient _client = null!;
    private readonly GrpcMetadata _headers = new();
    private readonly List<(LatticeAppsApiOperation Operation, string? Slug, string Method)> _authorized = [];
    private readonly System.Diagnostics.Stopwatch _duration = new();

    [OneTimeSetUp]
    public async Task Start()
    {
        _duration.Start();
        _control = Substitute.For<ILatticeAppsControl>();
        _authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        var services = builder.Services;
        services.AddRouting();
        services.AddSerializer();
        services.AddSingleton(_control);
        services.AddSingleton(_authorizer);
        services.AddLatticeAppsApiGrpc(o => o.AdvertisedAuthSchemes.Add(new()
        {
            SchemeId = "entra", DisplayName = "Sign in",
            Parameters = ImmutableDictionary<string, string>.Empty.Add("authority", "https://login.example"),
        }));
        services.AddLatticeAppsApiGrpc();
        _app = builder.Build();
        _app.MapLatticeAppsApiGrpc();
        await _app.StartAsync();
        _channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
            { HttpHandler = _app.GetTestServer().CreateHandler() });
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _client = LatticeAppsApiGrpcClient.Create(
            _channel.CreateCallInvoker().Intercept(new HeaderInterceptor(_headers)), _serializers);
    }

    [SetUp]
    public void Reset()
    {
        _headers.Clear();
        _control.ClearReceivedCalls();
        _authorizer.ClearReceivedCalls();
        _authorized.Clear();
        _authorizer.Configure().IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(c =>
            {
                var authorization = c.Arg<LatticeAppsApiAuthorizationContext>();
                _authorized.Add((authorization.Operation, authorization.AppSlug, authorization.Call.Method));
                return true;
            });
        _control.Configure().ListAsync(Arg.Any<CancellationToken>()).Returns(new AppCatalog());
    }

    [OneTimeTearDown]
    public async Task Stop()
    {
        _channel.Dispose();
        await _app.DisposeAsync();
        _serializers.Dispose();
        TestContext.Progress.WriteLine($"Fixture including host lifecycle: {_duration.Elapsed.TotalSeconds:F3}s");
    }

    [Test]
    public async Task Install_round_trips_the_complete_version_pinned_request()
    {
        _control.InstallAsync(Arg.Any<AppInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsGrpcTestData.Lifecycle(AppLifecycleState.Installed));
        var actual = await _client.InstallAsync(AppsGrpcTestData.Install);
        AssertJson(actual, AppsGrpcTestData.Lifecycle(AppLifecycleState.Installed));
        var call = _control.ReceivedCalls().Single();
        AssertJson(call.GetArguments()[0], AppsGrpcTestData.Install);
        await AssertAuthorized(LatticeAppsApiOperation.Install, "demo");
    }

    [TestCase("Enable", AppLifecycleState.Enabled)]
    [TestCase("Disable", AppLifecycleState.Disabled)]
    [TestCase("Uninstall", AppLifecycleState.Uninstalled)]
    public async Task Lifecycle_round_trips_slug_state_and_change(string method, AppLifecycleState state)
    {
        var expected = AppsGrpcTestData.Lifecycle(state);
        _control.EnableAsync("demo", Arg.Any<CancellationToken>()).Returns(expected);
        _control.DisableAsync("demo", Arg.Any<CancellationToken>()).Returns(expected);
        _control.UninstallAsync("demo", Arg.Any<CancellationToken>()).Returns(expected);
        var actual = method switch
        {
            "Enable" => await _client.EnableAsync("demo"),
            "Disable" => await _client.DisableAsync("demo"),
            _ => await _client.UninstallAsync("demo"),
        };
        AssertJson(actual, expected);
        Assert.That(_control.ReceivedCalls().Single().GetMethodInfo().Name, Is.EqualTo(method + "Async"));
        await AssertAuthorized(Enum.Parse<LatticeAppsApiOperation>(method), "demo");
    }

    [Test]
    public async Task List_round_trips_the_catalog()
    {
        var expected = new AppCatalog { Apps = [new() { Slug = "demo", Version = "1.2.3",
            State = AppLifecycleState.Enabled, Provenance = AppsGrpcTestData.Provenance }] };
        _control.ListAsync(Arg.Any<CancellationToken>()).Returns(expected);
        AssertJson(await _client.ListAsync(), expected);
        await AssertAuthorized(LatticeAppsApiOperation.List, null);
    }

    [TestCase(null)]
    [TestCase("1.2.3")]
    public async Task Describe_round_trips_all_manifest_sections_and_version_selection(string? version)
    {
        _control.DescribeAsync("demo", version, Arg.Any<CancellationToken>()).Returns(AppsGrpcTestData.Descriptor);
        AssertJson(await _client.DescribeAsync("demo", version), AppsGrpcTestData.Descriptor);
        await _control.Received(1).DescribeAsync("demo", version, Arg.Any<CancellationToken>());
        await AssertAuthorized(LatticeAppsApiOperation.Describe, "demo");
    }

    [Test]
    public async Task Describe_and_consent_preserve_absence()
    {
        _control.DescribeAsync("missing", null, Arg.Any<CancellationToken>()).Returns((AppDescriptor?)null);
        _control.GetConsentAsync("missing", Arg.Any<CancellationToken>()).Returns((AppConsentReport?)null);
        Assert.That(await _client.DescribeAsync("missing"), Is.Null);
        Assert.That(await _client.GetConsentAsync("missing"), Is.Null);
    }

    [Test]
    public async Task GetConsent_round_trips_the_complete_ceiling()
    {
        _control.GetConsentAsync("demo", Arg.Any<CancellationToken>()).Returns(AppsGrpcTestData.Consent);
        AssertJson(await _client.GetConsentAsync("demo"), AppsGrpcTestData.Consent);
        await AssertAuthorized(LatticeAppsApiOperation.GetConsent, "demo");
    }

    [Test]
    public async Task UpdateConsent_round_trips_the_replacement_ceiling()
    {
        var request = new AppConsentUpdate { Slug = "demo", Version = "1.2.3", Ceiling = AppsGrpcTestData.Ceiling };
        _control.UpdateConsentAsync(Arg.Any<AppConsentUpdate>(), Arg.Any<CancellationToken>())
            .Returns(AppsGrpcTestData.Consent);
        AssertJson(await _client.UpdateConsentAsync(request), AppsGrpcTestData.Consent);
        AssertJson(_control.ReceivedCalls().Single().GetArguments()[0], request);
        await AssertAuthorized(LatticeAppsApiOperation.UpdateConsent, "demo");
    }

    [Test]
    public async Task Capabilities_round_trips_all_permissions()
    {
        var expected = new LatticeAppsCapabilities { CanInstall = true, CanEnable = true, CanDisable = true,
            CanUninstall = true, CanList = true, CanDescribe = true, CanGetConsent = true, CanUpdateConsent = true };
        _control.GetCapabilitiesAsync(Arg.Any<CancellationToken>()).Returns(expected);
        AssertJson(await _client.GetCapabilitiesAsync(), expected);
        await AssertAuthorized(LatticeAppsApiOperation.GetCapabilities, null);
    }

    [Test]
    public async Task Advertisement_is_public_and_does_not_invoke_control_or_authorizer()
    {
        _authorizer.Configure().IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(false);
        var schemes = await _client.GetAuthSchemeAsync();
        Assert.That(schemes, Has.Count.EqualTo(1));
        Assert.That(schemes[0].SchemeId, Is.EqualTo("entra"));
        Assert.That(schemes[0].DisplayName, Is.EqualTo("Sign in"));
        Assert.That(schemes[0].Parameters["authority"], Is.EqualTo("https://login.example"));
        Assert.That(_control.ReceivedCalls(), Is.Empty);
        Assert.That(_authorizer.ReceivedCalls(), Is.Empty);
    }

    [TestCase("Install")]
    [TestCase("Enable")]
    [TestCase("Disable")]
    [TestCase("Uninstall")]
    [TestCase("List")]
    [TestCase("Describe")]
    [TestCase("GetConsent")]
    [TestCase("UpdateConsent")]
    [TestCase("GetCapabilities")]
    public void Denied_calls_never_reach_the_facade(string method)
    {
        _authorizer.Configure().IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(false);
        var error = Assert.ThrowsAsync<RpcException>(async () => await Invoke(method));
        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(_control.ReceivedCalls(), Is.Empty);
    }

    private Task Invoke(string method) => method switch
    {
        "Install" => _client.InstallAsync(AppsGrpcTestData.Install),
        "Enable" => _client.EnableAsync("demo"),
        "Disable" => _client.DisableAsync("demo"),
        "Uninstall" => _client.UninstallAsync("demo"),
        "List" => _client.ListAsync(),
        "Describe" => _client.DescribeAsync("demo"),
        "GetConsent" => _client.GetConsentAsync("demo"),
        "UpdateConsent" => _client.UpdateConsentAsync(new()
            { Slug = "demo", Version = "1.2.3", Ceiling = AppsGrpcTestData.Ceiling }),
        "GetCapabilities" => _client.GetCapabilitiesAsync(),
        _ => throw new ArgumentException(method),
    };

    private Task AssertAuthorized(LatticeAppsApiOperation operation, string? slug)
    {
        Assert.That(_authorized, Is.EqualTo(new[] { (operation, slug, LatticeAppsGrpcMethods.ServicePrefix + operation) }));
        Assert.That(_authorizer.ReceivedCalls().Count(), Is.EqualTo(1));
        return Task.CompletedTask;
    }

    private static void AssertJson(object? actual, object expected)
        => Assert.That(JsonSerializer.Serialize(actual), Is.EqualTo(JsonSerializer.Serialize(expected)));

    private sealed class HeaderInterceptor(GrpcMetadata headers) : Interceptor
    {
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
            => continuation(request, new(context.Method, context.Host, context.Options.WithHeaders(headers)));
    }
}
