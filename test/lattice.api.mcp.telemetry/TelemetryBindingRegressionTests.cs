using System.Net;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Regression cover for the MCP telemetry binding <b>after</b> the T2 hoist moved
/// the PromQL machinery into the neutral <c>Orleans.Lattice.Api.Telemetry</c>
/// package: the tool group must behave exactly as it did before.
/// </summary>
/// <remarks>
/// <para>
/// The neutral package tests the guardrails, the access policy, and the
/// fail-closed PromQL scan as units. What the hoist made thin is proof that the
/// <b>binding still wires them correctly end to end</b>. Every collaborator here is
/// therefore resolved from a container built by a host calling
/// <see cref="LatticeMcpTelemetryServiceCollectionExtensions.AddTelemetryTools"/>
/// - not hand-constructed as the handler unit tests do - so a forwarding gap, a
/// policy built from the wrong options instance, or a guardrail invoked against
/// stale budgets shows up as a behaviour change rather than passing silently.
/// </para>
/// <para>
/// The rejection messages are asserted as literals. That is deliberate: they are
/// the caller-visible contract, and comparing them against the neutral gate's own
/// output would be tautological (both call the same code) and so could not detect
/// a change in what the agent actually sees.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TelemetryBindingRegressionTests
{
    private const string BackendBase = "https://prometheus.internal:9090/";
    private const string EmptyVector =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}";

    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Builds the container a host gets from the real opt-in, with the backend
    /// transport swapped for a capturing handler so a rejected request can be
    /// proven never to have reached the backend. <c>ConfigureAll</c> is appended
    /// after the registration's own primary-handler action, so it wins for the
    /// single telemetry HTTP client without naming it.
    /// </summary>
    private static ServiceProvider Host(
        Action<LatticeApiMcpTelemetryOptions> configure,
        CapturingHttpMessageHandler handler)
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(options =>
        {
            options.BackendAddress = new Uri(BackendBase);
            configure(options);
        });
        services.ConfigureAll<HttpClientFactoryOptions>(options =>
            options.HttpMessageHandlerBuilderActions.Add(builder =>
            {
                // The product registration already built a primary handler by the
                // time this runs; dispose it before displacing it so the test
                // leaves no orphaned handler (or attached client certificate).
                (builder.PrimaryHandler as IDisposable)?.Dispose();
                builder.PrimaryHandler = handler;
            }));

        return services.BuildServiceProvider();
    }

    private static void DenyAllExcept(LatticeApiMcpTelemetryOptions options, params string[] allowed)
    {
        options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
        foreach (var entry in allowed)
        {
            options.AllowedMetrics.Add(entry);
        }
    }

    private static async Task<(TelemetryQueryResult Result, CapturingHttpMessageHandler Handler)> QueryThroughHost(
        Action<LatticeApiMcpTelemetryOptions> configure,
        string query)
    {
        var handler = new CapturingHttpMessageHandler(EmptyVector);
        await using var provider = Host(configure, handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            provider.GetRequiredService<IPrometheusQueryClient>(),
            provider.GetRequiredService<TelemetryMetricAccessPolicy>(),
            CancellationToken.None,
            query);

        return (result, handler);
    }

    private static async Task<(TelemetryQueryResult Result, CapturingHttpMessageHandler Handler)> RangeThroughHost(
        Action<LatticeApiMcpTelemetryOptions> configure,
        string query,
        DateTimeOffset start,
        DateTimeOffset end,
        TimeSpan step)
    {
        var handler = new CapturingHttpMessageHandler(EmptyVector);
        await using var provider = Host(configure, handler);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            provider.GetRequiredService<IPrometheusQueryClient>(),
            provider.GetRequiredService<TelemetryMetricAccessPolicy>(),
            provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>(),
            CancellationToken.None,
            query,
            start,
            end,
            step);

        return (result, handler);
    }

    // ---- Metric-access posture, resolved through the binding ----

    [Test]
    public void The_unconfigured_posture_resolved_through_the_binding_is_read_all()
    {
        var handler = new CapturingHttpMessageHandler(EmptyVector);
        using var provider = Host(_ => { }, handler);

        Assert.That(
            provider.GetRequiredService<TelemetryMetricAccessPolicy>().IsReadAll,
            Is.True,
            "The documented default posture must survive the options forwarding.");
    }

    [Test]
    public async Task A_host_configured_deny_all_posture_denies_an_unlisted_metric_through_the_binding()
    {
        var (result, handler) = await QueryThroughHost(
            o => DenyAllExcept(o, "lattice_wal_append_total"), "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(
                result.Error,
                Is.EqualTo("Metric 'up' is not permitted by the telemetry metric-access allow-list."));
            Assert.That(handler.RequestCount, Is.Zero, "A denied query must not reach the backend.");
        });
    }

    [Test]
    public async Task A_host_configured_deny_all_posture_admits_a_listed_metric_through_the_binding()
    {
        var (result, handler) = await QueryThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"), "lattice_wal_append_total");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True, result.Error);
            Assert.That(handler.RequestCount, Is.EqualTo(1), "An admitted query must reach the backend.");
        });
    }

    [Test]
    public async Task The_deny_all_posture_filters_the_metric_listing_through_the_binding()
    {
        const string names =
            "{\"status\":\"success\",\"data\":[\"lattice_wal_append_total\",\"up\",\"process_cpu\"]}";
        var handler = new CapturingHttpMessageHandler(names);
        await using var provider = Host(o => DenyAllExcept(o, "lattice_wal_*"), handler);

        var result = await TelemetryToolHandlers.ListMetricsAsync(
            provider.GetRequiredService<IPrometheusQueryClient>(),
            provider.GetRequiredService<TelemetryMetricAccessPolicy>(),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True, result.Error);
            Assert.That(result.Metrics, Is.EqualTo(new[] { "lattice_wal_append_total" }));
        });
    }

    [Test]
    public async Task The_deny_all_posture_rejects_a_named_metadata_lookup_through_the_binding()
    {
        var handler = new CapturingHttpMessageHandler("{\"status\":\"success\",\"data\":{}}");
        await using var provider = Host(o => DenyAllExcept(o, "lattice_wal_*"), handler);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            provider.GetRequiredService<IPrometheusQueryClient>(),
            provider.GetRequiredService<TelemetryMetricAccessPolicy>(),
            CancellationToken.None,
            "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(
                result.Error,
                Is.EqualTo("Metric 'up' is not permitted by the telemetry metric-access allow-list."));
            Assert.That(handler.RequestCount, Is.Zero, "A denied lookup must not reach the backend.");
        });
    }

    // ---- Range and step guardrails: evaluation order and exact messages ----
    //
    // Where two rules can be violated at once, the case violates both and asserts
    // the earlier one wins, so the assertion pins the precedence and not merely the
    // message. Each case additionally uses a query the deny-all policy would
    // reject, so it also pins that the guardrails run before the access gate.

    [Test]
    public async Task An_end_before_start_is_reported_ahead_of_a_non_positive_step()
    {
        var (result, handler) = await RangeThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"),
            "up",
            Start,
            Start - TimeSpan.FromDays(9),
            TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.EqualTo("The range end must be at or after the range start."));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_non_positive_step_is_reported_ahead_of_an_over_budget_range()
    {
        var (result, handler) = await RangeThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"),
            "up",
            Start,
            Start + TimeSpan.FromDays(9),
            TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.EqualTo("The range step must be strictly positive."));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task An_over_budget_range_is_reported_ahead_of_an_over_budget_step()
    {
        var (result, handler) = await RangeThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"),
            "up",
            Start,
            Start + TimeSpan.FromDays(9),
            TimeSpan.FromHours(9));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(
                result.Error,
                Is.EqualTo(
                    $"The requested range of {TimeSpan.FromDays(9)} exceeds the configured maximum "
                    + $"of {TimeSpan.FromHours(24)}."));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task An_over_budget_step_is_reported_when_it_is_the_only_violation()
    {
        var (result, handler) = await RangeThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"),
            "up",
            Start,
            Start + TimeSpan.FromHours(2),
            TimeSpan.FromHours(9));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(
                result.Error,
                Is.EqualTo(
                    $"The requested step of {TimeSpan.FromHours(9)} exceeds the configured maximum "
                    + $"of {TimeSpan.FromHours(1)}."));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task The_guardrails_run_before_the_metric_access_gate()
    {
        // Both gates would reject: the window is over budget and 'up' is denied.
        // The binding evaluates the guardrails first, so the range violation wins.
        var (result, _) = await RangeThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"),
            "up",
            Start,
            Start + TimeSpan.FromDays(9),
            TimeSpan.FromMinutes(1));

        Assert.That(result.Error, Does.StartWith("The requested range of"));
    }

    [Test]
    public async Task The_guardrails_are_evaluated_against_the_host_configured_budgets()
    {
        var (rejected, _) = await RangeThroughHost(
            o => o.MaxRange = TimeSpan.FromMinutes(30),
            "up",
            Start,
            Start + TimeSpan.FromHours(1),
            TimeSpan.FromMinutes(1));

        var (admitted, handler) = await RangeThroughHost(
            o => o.MaxRange = TimeSpan.FromHours(2),
            "up",
            Start,
            Start + TimeSpan.FromHours(1),
            TimeSpan.FromMinutes(1));

        Assert.Multiple(() =>
        {
            Assert.That(
                rejected.Error,
                Is.EqualTo(
                    $"The requested range of {TimeSpan.FromHours(1)} exceeds the configured maximum "
                    + $"of {TimeSpan.FromMinutes(30)}."),
                "A tightened budget must reach the guardrails through the forwarded options.");
            Assert.That(admitted.Success, Is.True, admitted.Error);
            Assert.That(handler.RequestCount, Is.EqualTo(1));
        });
    }

    // ---- Fail-closed PromQL scanning, through the binding ----

    private const string UnresolvableMatcherMessage =
        "The query references a metric by a '__name__' pattern or negative matcher, "
        + "which the telemetry metric-access allow-list cannot admit.";

    private const string UnconstrainedSelectorMessage =
        "The query selects series by label without constraining the metric name, "
        + "which the telemetry metric-access allow-list cannot admit.";

    private const string NoNamedMetricMessage =
        "The query does not name a metric the telemetry metric-access allow-list can admit.";

    [TestCase("{__name__=~\"lattice_wal_.*\"}", UnresolvableMatcherMessage, TestName =
        "A regex name matcher is denied through the binding")]
    [TestCase("{__name__!=\"lattice_wal_append_total\"}", UnresolvableMatcherMessage, TestName =
        "A negative exact name matcher is denied through the binding")]
    [TestCase("{__name__!~\"lattice_wal_.*\"}", UnresolvableMatcherMessage, TestName =
        "A negative regex name matcher is denied through the binding")]
    [TestCase("{__name__=\"lattice_wal_append_total}", UnresolvableMatcherMessage, TestName =
        "An unterminated name matcher value is denied through the binding")]
    [TestCase("{job=\"api\"}", UnconstrainedSelectorMessage, TestName =
        "A bare label-only selector is denied through the binding")]
    [TestCase("lattice_wal_append_total or {job=\"api\"}", UnconstrainedSelectorMessage, TestName =
        "An admitted metric ORed with a bare selector is denied through the binding")]
    public async Task The_fail_closed_scan_denies_a_query_through_the_binding(
        string query,
        string expectedMessage)
    {
        var (result, handler) = await QueryThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"), query);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.EqualTo(expectedMessage));
            Assert.That(handler.RequestCount, Is.Zero, "A fail-closed denial must not reach the backend.");
        });
    }

    [Test]
    public async Task A_query_naming_no_metric_is_denied_through_the_binding()
    {
        var (result, handler) = await QueryThroughHost(o => DenyAllExcept(o, "lattice_wal_*"), "42");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.EqualTo(NoNamedMetricMessage));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task The_read_all_posture_admits_a_query_the_fail_closed_scan_would_deny()
    {
        var (result, handler) = await QueryThroughHost(_ => { }, "{__name__=~\"lattice_wal_.*\"}");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True, result.Error);
            Assert.That(handler.RequestCount, Is.EqualTo(1), "The read-all path must not scan.");
        });
    }

    [TestCase("{__name__=~\"lattice_wal_.*\"}", UnresolvableMatcherMessage)]
    [TestCase("{job=\"api\"}", UnconstrainedSelectorMessage)]
    public async Task The_range_tool_applies_the_same_fail_closed_scan_through_the_binding(
        string query,
        string expectedMessage)
    {
        var (result, handler) = await RangeThroughHost(
            o => DenyAllExcept(o, "lattice_wal_*"),
            query,
            Start,
            Start + TimeSpan.FromHours(1),
            TimeSpan.FromMinutes(1));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.EqualTo(expectedMessage));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    // ---- The binding is not a path to the backend credential ----

    [Test]
    public async Task An_unauthorized_backend_response_does_not_echo_the_backend_token()
    {
        // A failing backend is the one path where transport detail reaches the
        // caller: the handler catches the exception and returns its message on the
        // result's Error field. This pins the ordinary fault path - the framework's
        // own status-code exception - as token-free.
        //
        // KNOWN LIMITATION, deliberately not fixed here: the mapping in
        // TelemetryToolHandlers.BackendErrorMessage interpolates ex.Message
        // verbatim, so a *custom* HttpMessageHandler in the chain that throws with
        // the Authorization value in its message would surface it to the caller.
        // That behaviour predates the T2 hoist and is identical before and after
        // it, so it is out of scope for this re-point (whose bar is byte-identical
        // MCP behaviour); it is reported to the epic separately rather than
        // silently changing caller-visible error text here.
        const string sentinel = "dynamic-backend-sentinel";
        var handler = new CapturingHttpMessageHandler(
            "{\"status\":\"error\"}", HttpStatusCode.Unauthorized);
        var services = new ServiceCollection();
        services.AddTelemetryTools(options =>
        {
            options.BackendAddress = new Uri(BackendBase);
            options.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer;
        });
        services.AddSingleton<ITelemetryBackendTokenProvider>(new StubTokenProvider(sentinel));
        services.ConfigureAll<HttpClientFactoryOptions>(options =>
            options.HttpMessageHandlerBuilderActions.Add(builder => builder.PrimaryHandler = handler));

        await using var provider = services.BuildServiceProvider();

        var result = await TelemetryToolHandlers.QueryAsync(
            provider.GetRequiredService<IPrometheusQueryClient>(),
            provider.GetRequiredService<TelemetryMetricAccessPolicy>(),
            CancellationToken.None,
            "up");

        Assert.Multiple(() =>
        {
            Assert.That(
                handler.LastAuthorization,
                Is.EqualTo($"Bearer {sentinel}"),
                "The assertions below are only meaningful if the token really was presented.");
            Assert.That(result.Success, Is.False, "A 401 must surface as a clean structured error.");
            Assert.That(
                result.Error,
                Does.Not.Contain(sentinel),
                "A backend fault must not echo the bearer token back to the caller.");
        });
    }

    [Test]
    public void The_binding_surface_exposes_no_route_to_the_backend_token_seam()
    {
        var routes = typeof(LatticeApiMcpTelemetryOptions).Assembly
            .GetExportedTypes()
            .SelectMany(type => type
                .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
                .Where(member => Exposes(member, typeof(ITelemetryBackendTokenProvider)))
                .Select(member => $"{type.Name}.{member.Name}"))
            .ToArray();

        Assert.That(
            routes,
            Is.Empty,
            "Only the neutral proxy may consult the backend-token seam; the MCP binding must not reach it.");
    }

    [Test]
    public async Task A_dynamic_bearer_query_leaves_no_token_on_the_options_the_tools_read()
    {
        const string sentinel = "dynamic-backend-sentinel";
        var handler = new CapturingHttpMessageHandler(EmptyVector);
        var services = new ServiceCollection();
        services.AddTelemetryTools(options =>
        {
            options.BackendAddress = new Uri(BackendBase);
            options.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer;
        });
        services.AddSingleton<ITelemetryBackendTokenProvider>(new StubTokenProvider(sentinel));
        services.ConfigureAll<HttpClientFactoryOptions>(options =>
            options.HttpMessageHandlerBuilderActions.Add(builder => builder.PrimaryHandler = handler));

        await using var provider = services.BuildServiceProvider();

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            provider.GetRequiredService<IPrometheusQueryClient>(),
            provider.GetRequiredService<TelemetryMetricAccessPolicy>(),
            provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>(),
            CancellationToken.None,
            "up",
            Start,
            Start + TimeSpan.FromHours(1),
            TimeSpan.FromMinutes(1));

        // The range tool is handed IOptions<LatticeApiMcpTelemetryOptions>, which
        // post-hoist is the same object the neutral proxy reads. A token cached
        // there would therefore be readable by every tool handler.
        var toolVisibleOptions = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True, result.Error);
            Assert.That(handler.LastAuthorization, Is.EqualTo($"Bearer {sentinel}"));
            Assert.That(
                toolVisibleOptions.Credential,
                Is.Null,
                "The minted backend token must not be cached onto the options the tools can read.");
            Assert.That(
                result.Error ?? string.Empty,
                Does.Not.Contain(sentinel),
                "No tool result may echo the backend token.");
        });
    }

    private static bool Exposes(MemberInfo member, Type type) => member switch
    {
        PropertyInfo property => type.IsAssignableFrom(property.PropertyType),
        FieldInfo field => type.IsAssignableFrom(field.FieldType),
        MethodInfo method => type.IsAssignableFrom(method.ReturnType)
            || method.GetParameters().Any(p => type.IsAssignableFrom(p.ParameterType)),
        ConstructorInfo constructor =>
            constructor.GetParameters().Any(p => type.IsAssignableFrom(p.ParameterType)),
        _ => false,
    };

    private sealed class StubTokenProvider(string token) : ITelemetryBackendTokenProvider
    {
        public ValueTask<string> GetAccessTokenAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(token);
    }
}
