using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Assembles a <see cref="LatticeTelemetry"/> over scripted seams, so each test
/// states only the one thing it varies - the caller's tenant, its grants, the
/// allow-list, or the backend's answer - and inherits a working default for the
/// rest.
/// </summary>
internal sealed class TelemetryFacadeHarness
{
    private LatticeTelemetryOptions _options = new()
    {
        BackendAddress = new Uri("https://prometheus.test/"),
    };

    private ITenantContextResolver _tenants = new StubTenantContextResolver(TenantId.Parse("acme"));
    private ILatticeAccessGate? _gate = StubAccessGate.TelemetryOnly();
    private ILatticeMembershipContext? _membership;
    private IReadOnlyList<TelemetryQueryDefinition>? _definitions;

    /// <summary>The recording backend the assembled facade drives.</summary>
    public RecordingPrometheusQueryClient Backend { get; } = new();

    /// <summary>Scopes the caller to <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant the context resolver reports.</param>
    /// <param name="resolvesSynchronously">Whether the resolver answers on the warm path.</param>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness ForTenant(string tenantId, bool resolvesSynchronously = true)
    {
        _tenants = new StubTenantContextResolver(TenantId.Parse(tenantId), resolvesSynchronously);
        return this;
    }

    /// <summary>Uses <paramref name="resolver"/> as the tenant-context seam.</summary>
    /// <param name="resolver">The resolver to use.</param>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness WithTenantResolver(ITenantContextResolver resolver)
    {
        _tenants = resolver;
        return this;
    }

    /// <summary>Uses <paramref name="gate"/> as the access gate, or none at all.</summary>
    /// <param name="gate">The gate to consult, or <see langword="null"/> for no gate.</param>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness WithGate(ILatticeAccessGate? gate)
    {
        _gate = gate;
        return this;
    }

    /// <summary>Uses <paramref name="membership"/> to resolve the caller subject.</summary>
    /// <param name="membership">The membership context, or <see langword="null"/> for none.</param>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness WithMembership(ILatticeMembershipContext? membership)
    {
        _membership = membership;
        return this;
    }

    /// <summary>Grants the caller platform-operator authority as well as telemetry.</summary>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness AsPlatformOperator() => WithGate(StubAccessGate.PlatformOperator());

    /// <summary>Applies <paramref name="configure"/> to the telemetry options.</summary>
    /// <param name="configure">The option mutation.</param>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness WithOptions(Action<LatticeTelemetryOptions> configure)
    {
        configure(_options);
        return this;
    }

    /// <summary>Replaces the options wholesale.</summary>
    /// <param name="options">The options to use.</param>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness WithOptions(LatticeTelemetryOptions options)
    {
        _options = options;
        return this;
    }

    /// <summary>Serves <paramref name="definitions"/> instead of the built-in catalogue.</summary>
    /// <param name="definitions">The definitions to compile.</param>
    /// <returns>This harness, for chaining.</returns>
    public TelemetryFacadeHarness WithDefinitions(params TelemetryQueryDefinition[] definitions)
    {
        _definitions = definitions;
        return this;
    }

    /// <summary>The compiled catalogue the assembled facade serves.</summary>
    /// <returns>The catalogue.</returns>
    public LatticeTelemetryQueryCatalog BuildCatalog()
    {
        var policy = new TelemetryMetricAccessPolicy(_options);
        return _definitions is null
            ? new LatticeTelemetryQueryCatalog(policy)
            : new LatticeTelemetryQueryCatalog(_definitions, LatticeTelemetryQueries.Version, policy);
    }

    /// <summary>Assembles the facade.</summary>
    /// <returns>The facade under test.</returns>
    public LatticeTelemetry Build()
    {
        var authorizer = new TelemetryAccessAuthorizer(_gate, _membership);
        return new LatticeTelemetry(
            BuildCatalog(),
            new TelemetryTenantScopeResolver(_tenants, authorizer),
            authorizer,
            Backend,
            Options.Create(_options),
            FixedTimeProvider.AtInstant);
    }

    /// <summary>
    /// A range request for <paramref name="queryId"/> over the hour ending at the
    /// frozen test instant, at a one-minute step.
    /// </summary>
    /// <param name="queryId">The catalogue id to select.</param>
    /// <returns>The request.</returns>
    public static TelemetryQueryRequest RangeRequest(string queryId) => new()
    {
        QueryId = queryId,
        Range = TelemetryTimeRange.Between(
            FixedTimeProvider.Instant.AddHours(-1),
            FixedTimeProvider.Instant,
            TimeSpan.FromMinutes(1)),
    };

    /// <summary>An instant request for <paramref name="queryId"/> at the frozen test instant.</summary>
    /// <param name="queryId">The catalogue id to select.</param>
    /// <returns>The request.</returns>
    public static TelemetryQueryRequest InstantRequest(string queryId) => new()
    {
        QueryId = queryId,
        Range = TelemetryTimeRange.At(FixedTimeProvider.Instant),
    };
}
