using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// The bridge's supporting surface: the continuation format, the pinned size bounds, the options defaults and
/// the DI registration.
/// </summary>
[TestFixture]
public sealed class AppBridgeSupportTests
{
    private static ServiceCollection EngineServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IAppRegistry>());
        services.AddSingleton(Substitute.For<IAppSource>());
        services.AddSingleton(Substitute.For<IAppActivationPipeline>());
        services.AddSingleton(Substitute.For<ILatticeAccessGate>());
        services.AddSingleton(Substitute.For<ITenantContextResolver>());
        return services;
    }

    [Test]
    public void The_bounds_equal_the_AppKit_frame_protocol_limits()
    {
        Assert.That(AppBridgeLimits.MaxValueBytes, Is.EqualTo(65536));
        Assert.That(AppBridgeLimits.MaxKeyLength, Is.EqualTo(1024));
        Assert.That(AppBridgeLimits.MaxTreeNameLength, Is.EqualTo(128));
        Assert.That(AppBridgeLimits.MaxContinuationLength, Is.EqualTo(4096));
        Assert.That(AppBridgeLimits.MaxPageSize, Is.EqualTo(200));
        Assert.That(AppBridgeLimits.MaxResponseBytes, Is.EqualTo(1048576));
    }

    [Test]
    public void The_entry_estimate_counts_utf8_key_bytes_base64_value_bytes_and_overhead()
    {
        Assert.That(AppBridgeLimits.EstimateEntryBytes("ab", 0), Is.EqualTo(2 + AppBridgeLimits.EntryOverheadBytes));
        Assert.That(AppBridgeLimits.EstimateEntryBytes("\u00e9", 3), Is.EqualTo(2 + 4 + AppBridgeLimits.EntryOverheadBytes));
        Assert.That(AppBridgeLimits.EstimateEntryBytes("k", 4), Is.EqualTo(1 + 8 + AppBridgeLimits.EntryOverheadBytes));
    }

    [Test]
    public void A_continuation_round_trips_its_key()
    {
        var encoded = AppBridgeContinuation.Encode("n/2");

        Assert.That(encoded, Does.StartWith(AppBridgeContinuation.Marker));
        Assert.That(AppBridgeContinuation.TryDecode(encoded, "n/", out var key), Is.True);
        Assert.That(key, Is.EqualTo("n/2"));
        Assert.That(AppBridgeContinuation.TryDecode(encoded, string.Empty, out _), Is.True);
    }

    [TestCase("")]
    [TestCase("k1:")]
    [TestCase("x1:n/2")]
    [TestCase("k1:m/2")]
    public void A_malformed_or_out_of_prefix_continuation_is_refused(string continuation)
    {
        Assert.That(AppBridgeContinuation.TryDecode(continuation, "n/", out var key), Is.False);
        Assert.That(key, Is.Empty);
    }

    [Test]
    public void A_continuation_naming_an_overlong_key_is_refused() =>
        Assert.That(AppBridgeContinuation.TryDecode(AppBridgeContinuation.Encode(new string('k', AppBridgeLimits.MaxKeyLength + 1)), string.Empty, out _), Is.False);

    [Test]
    public void The_options_default_to_one_hundred_requests_per_second()
    {
        var options = new LatticeAppBridgeOptions();

        Assert.That(options.RateLimitPermitLimit, Is.EqualTo(LatticeAppBridgeOptions.DefaultRateLimitPermitLimit).And.EqualTo(100));
        Assert.That(options.RateLimitWindow, Is.EqualTo(LatticeAppBridgeOptions.DefaultRateLimitWindow).And.EqualTo(TimeSpan.FromSeconds(1)));
    }

    [Test]
    public void AddLatticeAppBridgeApi_registers_the_bridge_once_with_the_app_control_facades()
    {
        var services = EngineServices();

        services.AddLatticeAppBridgeApi();
        services.AddLatticeAppBridgeApi();

        Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeAppBridge)), Is.EqualTo(1));
        Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeAppsControl)), Is.EqualTo(1));
        using var provider = services.BuildServiceProvider();
        var bridge = provider.GetRequiredService<ILatticeAppBridge>();
        Assert.That(bridge, Is.TypeOf<LatticeAppBridge>());
        Assert.That(provider.GetRequiredService<ILatticeAppBridge>(), Is.SameAs(bridge));
    }

    [Test]
    public void AddLatticeAppBridgeApi_applies_the_configured_rate_limit()
    {
        var services = EngineServices();

        services.AddLatticeAppBridgeApi(o => o.RateLimitPermitLimit = 1);

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IOptions<LatticeAppBridgeOptions>>().Value.RateLimitPermitLimit, Is.EqualTo(1));
        var limiter = provider.GetRequiredService<AppBridgeRateLimiter>();
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, AppSlug.Parse("crm")), Is.True);
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, AppSlug.Parse("crm")), Is.False);
    }

    [Test]
    public void AddLatticeAppBridgeApi_with_invalid_options_fails_when_the_bridge_is_resolved()
    {
        var services = EngineServices();
        services.AddLatticeAppBridgeApi(o => o.RateLimitPermitLimit = 0);

        using var provider = services.BuildServiceProvider();
        Assert.Throws<ArgumentOutOfRangeException>(() => provider.GetRequiredService<ILatticeAppBridge>());
    }

    [Test]
    public void AddLatticeAppBridgeApi_without_the_apps_add_on_throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddLatticeAppBridgeApi());
        Assert.That(ex!.Message, Does.Contain("AddLatticeApps()"));
    }

    [Test]
    public void AddLatticeAppBridgeApi_null_arguments_throw()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppBridgeApi());
        Assert.Throws<ArgumentNullException>(() => ((ISiloBuilder)null!).AddLatticeAppBridgeApi());
    }

    [Test]
    public void AddLatticeAppBridgeApi_silo_builder_overload_registers_on_its_services()
    {
        var services = EngineServices();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        Assert.That(builder.AddLatticeAppBridgeApi(o => o.RateLimitPermitLimit = 5), Is.SameAs(builder));
        Assert.That(services.Any(d => d.ServiceType == typeof(ILatticeAppBridge)), Is.True);
    }

    [Test]
    public void The_bridge_constructor_rejects_a_null_evaluator_or_limiter()
    {
        var limiter = new AppBridgeRateLimiter(new LatticeAppBridgeOptions());
        var evaluator = new AppRoleGrantEvaluator(null, null, null);

        Assert.Throws<ArgumentNullException>(() => new LatticeAppBridge(null!, null, null, null, limiter));
        Assert.Throws<ArgumentNullException>(() => new LatticeAppBridge(evaluator, null, null, null, null!));
    }
}
