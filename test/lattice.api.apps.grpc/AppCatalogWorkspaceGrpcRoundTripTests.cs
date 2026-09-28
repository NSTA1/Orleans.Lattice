using System.Collections.Immutable;
using System.Text.Json;
using Grpc.Core;
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

/// <summary>
/// Round trips every catalogue and workspace RPC through the real gRPC services, marshallers, interceptor and
/// clients against substitute facades, including default deny, absence, failure mapping and the per-method
/// asset size bound.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Measured 2.5s for all 29 cases including TestServer lifecycle and a 2 MiB asset; fake facades, no sockets or silo.")]
public sealed class AppCatalogWorkspaceGrpcRoundTripTests
{
    private WebApplication _app = null!;
    private GrpcChannel _channel = null!;
    private ServiceProvider _serializers = null!;
    private ILatticeAppCatalog _catalog = null!;
    private ILatticeAppWorkspace _workspace = null!;
    private ILatticeAppsApiAuthorizer _authorizer = null!;
    private LatticeAppCatalogApiGrpcClient _catalogClient = null!;
    private LatticeAppWorkspaceApiGrpcClient _workspaceClient = null!;
    private readonly List<(LatticeAppsApiOperation Operation, string? Slug, string Method)> _authorized = [];
    private readonly System.Diagnostics.Stopwatch _duration = new();

    private static readonly AppIconAsset Icon = new() { Bytes = new byte[] { 1, 2, 3 }, MediaType = "image/svg+xml", Sha256 = new string('a', 64) };

    private static readonly AppUiDescriptor Ui = new()
    {
        Entry = "index.html",
        Styles = ["site.css"],
        Scripts = [new AppUiScriptDescriptor { Path = "app.js", Module = true }],
        Assets = [new AppUiAssetDescriptor { Path = "index.html", MediaType = "text/html", Sha256 = new string('b', 64) }],
        BundleDigest = new string('c', 64),
        Bridge = [new AppUiBridgeGrantDescriptor { Operation = "data.read", Tree = "events" }, new AppUiBridgeGrantDescriptor { Operation = "ui.notify" }],
        MinProtocol = 1,
    };

    private static readonly AppPresentationDescriptor Presentation = new()
    {
        DisplayName = "Demo",
        Summary = "A demo.",
        Description = "Line one.\nLine two.",
        Icon = new AppIconDescriptor { Path = "icon.svg", Sha256 = new string('a', 64) },
        Categories = ["tools"],
        DocumentationUrl = "https://example.com",
        PublisherDisplayName = "Contoso",
    };

    [OneTimeSetUp]
    public async Task Start()
    {
        _duration.Start();
        _catalog = Substitute.For<ILatticeAppCatalog>();
        _workspace = Substitute.For<ILatticeAppWorkspace>();
        _authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        var services = builder.Services;
        services.AddRouting();
        services.AddSerializer();
        services.AddSingleton(_catalog);
        services.AddSingleton(_workspace);
        services.AddSingleton(_authorizer);
        services.AddLatticeAppCatalogApiGrpc();
        services.AddLatticeAppWorkspaceApiGrpc();
        services.AddLatticeAppWorkspaceApiGrpc();
        _app = builder.Build();
        _app.MapLatticeAppCatalogApiGrpc();
        _app.MapLatticeAppWorkspaceApiGrpc();
        await _app.StartAsync();
        _channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions { HttpHandler = _app.GetTestServer().CreateHandler() });
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _catalogClient = LatticeAppCatalogApiGrpcClient.Create(_channel.CreateCallInvoker(), _serializers);
        _workspaceClient = LatticeAppWorkspaceApiGrpcClient.Create(_channel.CreateCallInvoker(), _serializers);
    }

    [SetUp]
    public void Reset()
    {
        _catalog.ClearReceivedCalls();
        _workspace.ClearReceivedCalls();
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
        TestContext.Progress.WriteLine($"Fixture including host lifecycle: {_duration.Elapsed.TotalSeconds:F3}s");
    }

    [Test]
    public async Task ListSources_round_trips_every_source()
    {
        ImmutableArray<AppSourceSummary> expected =
        [
            new() { Key = "in-image", DisplayName = "In image", Kind = AppSourceSummaryKind.Static, Capabilities = AppSourceSummaryCapabilities.Enumerate },
            new() { Key = "feed", DisplayName = "Feed", Kind = AppSourceSummaryKind.Dynamic, Capabilities = AppSourceSummaryCapabilities.Search | AppSourceSummaryCapabilities.RequiresAcquisition },
        ];
        _catalog.ListSourcesAsync(Arg.Any<CancellationToken>()).Returns(expected);

        AssertJson(await _catalogClient.ListSourcesAsync(), expected);
        AssertAuthorized(LatticeAppsApiOperation.ListSources, null, "ListSources", LatticeAppCatalogGrpcMethods.ServicePrefix);
    }

    [Test]
    public async Task ListAvailable_round_trips_the_query_and_the_page()
    {
        var query = new AvailableAppQuery { SourceKey = "feed", Text = "dem", Filter = AvailableAppFilter.Updates, PageSize = 7, Continuation = "abc" };
        var expected = new AvailableAppPage
        {
            Apps =
            [
                new()
                {
                    SourceKey = "feed", Slug = "demo", NewestVersion = "2.0.0", AvailableVersions = ["2.0.0", "1.0.0"],
                    Presentation = Presentation, HasUi = true, InstalledVersion = "1.0.0", InstalledState = AppLifecycleState.Enabled,
                },
            ],
            Continuation = "next",
        };
        _catalog.ListAvailableAsync(Arg.Any<AvailableAppQuery>(), Arg.Any<CancellationToken>()).Returns(expected);

        AssertJson(await _catalogClient.ListAvailableAsync(query), expected);
        AssertJson(_catalog.ReceivedCalls().Single().GetArguments()[0], query);
        AssertAuthorized(LatticeAppsApiOperation.ListAvailable, null, "ListAvailable", LatticeAppCatalogGrpcMethods.ServicePrefix);
    }

    [TestCase(null)]
    [TestCase("1.2.3")]
    public async Task DescribeFromSource_round_trips_the_descriptor_with_presentation_ui_and_source_key(string? version)
    {
        var expected = AppsGrpcTestData.Descriptor with { Presentation = Presentation, Ui = Ui, SourceKey = "feed" };
        _catalog.DescribeFromSourceAsync("feed", "demo", version, Arg.Any<CancellationToken>()).Returns(expected);

        AssertJson(await _catalogClient.DescribeFromSourceAsync("feed", "demo", version), expected);
        AssertAuthorized(LatticeAppsApiOperation.DescribeFromSource, "demo", "DescribeFromSource", LatticeAppCatalogGrpcMethods.ServicePrefix);
    }

    [Test]
    public async Task Catalog_GetIcon_round_trips_the_bytes()
    {
        _catalog.GetIconAsync("feed", "demo", "1.0.0", Arg.Any<CancellationToken>()).Returns(Icon);

        var icon = await _catalogClient.GetIconAsync("feed", "demo", "1.0.0");

        Assert.That(icon!.Bytes.ToArray(), Is.EqualTo(Icon.Bytes.ToArray()));
        Assert.That(icon.MediaType, Is.EqualTo(Icon.MediaType));
        Assert.That(icon.Sha256, Is.EqualTo(Icon.Sha256));
        AssertAuthorized(LatticeAppsApiOperation.GetSourceIcon, "demo", "GetIcon", LatticeAppCatalogGrpcMethods.ServicePrefix);
    }

    [Test]
    public async Task Catalog_capabilities_round_trip()
    {
        var expected = new LatticeAppCatalogCapabilities { CanListSources = true, CanListAvailable = true, CanDescribeFromSource = true, CanGetIcon = true };
        _catalog.GetCapabilitiesAsync(Arg.Any<CancellationToken>()).Returns(expected);

        AssertJson(await _catalogClient.GetCapabilitiesAsync(), expected);
        AssertAuthorized(LatticeAppsApiOperation.GetCatalogCapabilities, null, "GetCapabilities", LatticeAppCatalogGrpcMethods.ServicePrefix);
    }

    [Test]
    public async Task ListMyApps_round_trips_the_callers_apps()
    {
        ImmutableArray<WorkspaceAppSummary> expected =
            [new() { Slug = "demo", Version = "1.2.3", InstallRevision = 4, Presentation = Presentation, HasUi = true, Roles = ["reader"] }];
        _workspace.ListMyAppsAsync(Arg.Any<CancellationToken>()).Returns(expected);

        AssertJson(await _workspaceClient.ListMyAppsAsync(), expected);
        AssertAuthorized(LatticeAppsApiOperation.ListMyApps, null, "ListMyApps", LatticeAppWorkspaceGrpcMethods.ServicePrefix);
    }

    [Test]
    public async Task DescribeMyApp_round_trips_the_sanitised_descriptor()
    {
        var expected = new WorkspaceAppDescriptor
        {
            Slug = "demo", Version = "1.2.3", InstallRevision = 4, SourceKey = "in-image", State = AppLifecycleState.Enabled,
            Presentation = Presentation,
            Trees = [new() { Name = "events", Rebuildable = true, Adopted = true, ShardCount = 2, SoftDeleteDuration = TimeSpan.FromDays(1) }],
            Roles = AppsGrpcTestData.Descriptor.Roles,
            McpTools = AppsGrpcTestData.Descriptor.McpTools,
            Subscriptions = AppsGrpcTestData.Descriptor.Subscriptions,
            Replication = AppsGrpcTestData.Descriptor.Replication,
            Ui = Ui,
        };
        _workspace.DescribeMyAppAsync("demo", Arg.Any<CancellationToken>()).Returns(expected);

        AssertJson(await _workspaceClient.DescribeMyAppAsync("demo"), expected);
        AssertAuthorized(LatticeAppsApiOperation.DescribeMyApp, "demo", "DescribeMyApp", LatticeAppWorkspaceGrpcMethods.ServicePrefix);
    }

    [Test]
    public async Task Workspace_GetIcon_and_GetUiAsset_round_trip_the_bytes()
    {
        var asset = new AppUiAsset { Path = "app.js", Bytes = new byte[] { 9, 8, 7 }, MediaType = "text/javascript", Sha256 = new string('d', 64) };
        _workspace.GetIconAsync("demo", Arg.Any<CancellationToken>()).Returns(Icon);
        _workspace.GetUiAssetAsync("demo", "app.js", Arg.Any<CancellationToken>()).Returns(asset);

        var icon = await _workspaceClient.GetIconAsync("demo");
        AssertAuthorized(LatticeAppsApiOperation.GetMyAppIcon, "demo", "GetIcon", LatticeAppWorkspaceGrpcMethods.ServicePrefix);
        _authorized.Clear();
        _authorizer.ClearReceivedCalls();
        var received = await _workspaceClient.GetUiAssetAsync("demo", "app.js");

        Assert.That(icon!.Bytes.ToArray(), Is.EqualTo(Icon.Bytes.ToArray()));
        Assert.That(received!.Path, Is.EqualTo("app.js"));
        Assert.That(received.Bytes.ToArray(), Is.EqualTo(new byte[] { 9, 8, 7 }));
        Assert.That(received.MediaType, Is.EqualTo("text/javascript"));
        Assert.That(received.Sha256, Is.EqualTo(asset.Sha256));
        AssertAuthorized(LatticeAppsApiOperation.GetUiAsset, "demo", "GetUiAsset", LatticeAppWorkspaceGrpcMethods.ServicePrefix);
    }

    [Test]
    public async Task A_maximum_size_asset_fits_the_per_method_bound()
    {
        var bytes = new byte[2 * 1024 * 1024];
        _workspace.GetUiAssetAsync("demo", "big.js", Arg.Any<CancellationToken>())
            .Returns(new AppUiAsset { Path = "big.js", Bytes = bytes, MediaType = "text/javascript", Sha256 = new string('e', 64) });

        var received = await _workspaceClient.GetUiAssetAsync("demo", "big.js");

        Assert.That(received!.Bytes.Length, Is.EqualTo(bytes.Length));
    }

    [Test]
    public void An_asset_message_over_the_per_method_bound_is_refused()
    {
        _workspace.GetUiAssetAsync("demo", "huge.js", Arg.Any<CancellationToken>())
            .Returns(new AppUiAsset { Path = "huge.js", Bytes = new byte[LatticeAppsGrpcMarshallers.MaxAssetMessageBytes + 1], MediaType = "text/javascript", Sha256 = new string('e', 64) });

        var error = Assert.ThrowsAsync<RpcException>(() => _workspaceClient.GetUiAssetAsync("demo", "huge.js"));

        Assert.That(error!.StatusCode, Is.AnyOf(StatusCode.ResourceExhausted, StatusCode.Unknown, StatusCode.Internal));
    }

    [Test]
    public async Task Absence_is_preserved_on_every_nullable_rpc()
    {
        _catalog.DescribeFromSourceAsync("feed", "missing", null, Arg.Any<CancellationToken>()).Returns((AppDescriptor?)null);
        _catalog.GetIconAsync("feed", "missing", null, Arg.Any<CancellationToken>()).Returns((AppIconAsset?)null);
        _workspace.DescribeMyAppAsync("missing", Arg.Any<CancellationToken>()).Returns((WorkspaceAppDescriptor?)null);
        _workspace.GetIconAsync("missing", Arg.Any<CancellationToken>()).Returns((AppIconAsset?)null);
        _workspace.GetUiAssetAsync("missing", "index.html", Arg.Any<CancellationToken>()).Returns((AppUiAsset?)null);
        _workspace.ListMyAppsAsync(Arg.Any<CancellationToken>()).Returns(ImmutableArray<WorkspaceAppSummary>.Empty);

        Assert.That(await _catalogClient.DescribeFromSourceAsync("feed", "missing"), Is.Null);
        Assert.That(await _catalogClient.GetIconAsync("feed", "missing"), Is.Null);
        Assert.That(await _workspaceClient.DescribeMyAppAsync("missing"), Is.Null);
        Assert.That(await _workspaceClient.GetIconAsync("missing"), Is.Null);
        Assert.That(await _workspaceClient.GetUiAssetAsync("missing", "index.html"), Is.Null);
        Assert.That(await _workspaceClient.ListMyAppsAsync(), Is.Empty);
    }

    [TestCase("ListSources")]
    [TestCase("ListAvailable")]
    [TestCase("DescribeFromSource")]
    [TestCase("CatalogGetIcon")]
    [TestCase("CatalogGetCapabilities")]
    [TestCase("ListMyApps")]
    [TestCase("DescribeMyApp")]
    [TestCase("WorkspaceGetIcon")]
    [TestCase("GetUiAsset")]
    public void Denied_calls_never_reach_the_facades(string method)
    {
        _authorizer.Configure().IsAuthorizedAsync(Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>()).Returns(false);

        var error = Assert.ThrowsAsync<RpcException>(async () => await Invoke(method));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(_catalog.ReceivedCalls(), Is.Empty);
        Assert.That(_workspace.ReceivedCalls(), Is.Empty);
    }

    [TestCase(typeof(LatticeAuthorizationDeniedException), StatusCode.PermissionDenied)]
    [TestCase(typeof(LatticeTenantAccessDeniedException), StatusCode.PermissionDenied)]
    [TestCase(typeof(ArgumentException), StatusCode.InvalidArgument)]
    [TestCase(typeof(KeyNotFoundException), StatusCode.NotFound)]
    [TestCase(typeof(InvalidOperationException), StatusCode.FailedPrecondition)]
    [TestCase(typeof(OperationCanceledException), StatusCode.Cancelled)]
    [TestCase(typeof(NotSupportedException), StatusCode.Internal)]
    public void Facade_failures_map_to_fixed_sanitised_statuses(Type exceptionType, StatusCode expected)
    {
        var exception = (Exception)Activator.CreateInstance(exceptionType, "detail naming t/acme/a/demo/events")!;
        _catalog.ListSourcesAsync(Arg.Any<CancellationToken>()).Returns(Task.FromException<ImmutableArray<AppSourceSummary>>(exception));
        _workspace.ListMyAppsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromException<ImmutableArray<WorkspaceAppSummary>>(exception));

        var catalogError = Assert.ThrowsAsync<RpcException>(() => _catalogClient.ListSourcesAsync());
        var workspaceError = Assert.ThrowsAsync<RpcException>(() => _workspaceClient.ListMyAppsAsync());

        Assert.That(catalogError!.StatusCode, Is.EqualTo(expected));
        Assert.That(workspaceError!.StatusCode, Is.EqualTo(expected));
        Assert.That(catalogError.Status.Detail, Does.Not.Contain("a/demo"));
        Assert.That(workspaceError.Status.Detail, Does.Not.Contain("a/demo"));
    }

    [Test]
    public void Clients_validate_arguments_before_calling()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => _catalogClient.ListAvailableAsync(null!));
            Assert.ThrowsAsync<ArgumentException>(() => _catalogClient.DescribeFromSourceAsync(" ", "demo"));
            Assert.ThrowsAsync<ArgumentException>(() => _catalogClient.DescribeFromSourceAsync("feed", ""));
            Assert.ThrowsAsync<ArgumentException>(() => _catalogClient.GetIconAsync("", "demo"));
            Assert.ThrowsAsync<ArgumentNullException>(() => _catalogClient.GetIconAsync("feed", null!));
            Assert.ThrowsAsync<ArgumentException>(() => _workspaceClient.DescribeMyAppAsync(""));
            Assert.ThrowsAsync<ArgumentException>(() => _workspaceClient.GetIconAsync(" "));
            Assert.ThrowsAsync<ArgumentException>(() => _workspaceClient.GetUiAssetAsync("demo", ""));
            Assert.ThrowsAsync<ArgumentNullException>(() => _workspaceClient.GetUiAssetAsync(null!, "app.js"));
            Assert.Throws<ArgumentNullException>(() => LatticeAppCatalogApiGrpcClient.Create(null!, _serializers));
            Assert.Throws<ArgumentNullException>(() => LatticeAppCatalogApiGrpcClient.Create(_channel.CreateCallInvoker(), null!));
            Assert.Throws<ArgumentNullException>(() => LatticeAppWorkspaceApiGrpcClient.Create(null!, _serializers));
            Assert.Throws<ArgumentNullException>(() => LatticeAppWorkspaceApiGrpcClient.Create(_channel.CreateCallInvoker(), null!));
        });
        Assert.That(_authorizer.ReceivedCalls(), Is.Empty);
    }

    private Task Invoke(string method) => method switch
    {
        "ListSources" => _catalogClient.ListSourcesAsync(),
        "ListAvailable" => _catalogClient.ListAvailableAsync(new AvailableAppQuery()),
        "DescribeFromSource" => _catalogClient.DescribeFromSourceAsync("feed", "demo"),
        "CatalogGetIcon" => _catalogClient.GetIconAsync("feed", "demo"),
        "CatalogGetCapabilities" => _catalogClient.GetCapabilitiesAsync(),
        "ListMyApps" => _workspaceClient.ListMyAppsAsync(),
        "DescribeMyApp" => _workspaceClient.DescribeMyAppAsync("demo"),
        "WorkspaceGetIcon" => _workspaceClient.GetIconAsync("demo"),
        "GetUiAsset" => _workspaceClient.GetUiAssetAsync("demo", "index.html"),
        _ => throw new ArgumentException(method),
    };

    private void AssertAuthorized(LatticeAppsApiOperation operation, string? slug, string method, string prefix)
    {
        Assert.That(_authorized, Is.EqualTo(new[] { (operation, slug, prefix + method) }));
        Assert.That(_authorizer.ReceivedCalls().Count(), Is.EqualTo(1));
    }

    private static void AssertJson(object? actual, object expected)
        => Assert.That(JsonSerializer.Serialize(actual), Is.EqualTo(JsonSerializer.Serialize(expected)));
}
