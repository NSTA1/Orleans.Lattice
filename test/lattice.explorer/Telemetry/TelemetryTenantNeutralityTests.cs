using System.Reflection;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Proves the Explorer seam performs <b>no local tenant scoping</b>. It carries
/// the visibility and tenant the caller <em>requests</em>, returns whatever the
/// facade <em>pinned</em>, and hands a panel every series the facade sent.
/// </summary>
/// <remarks>
/// <para>
/// This is the D5 invariant seen from the client side. A desktop head cannot be
/// trusted to enforce tenant scoping, because whoever runs it can edit it - which
/// is exactly why the facade is routable and does the enforcing. A seam that
/// quietly filtered by a tenant label would look correct in a demo and be
/// worthless as a control, and would also hide a real degradation: series the
/// facade deliberately returned would vanish with no explanation.
/// </para>
/// </remarks>
[TestFixture]
public class TelemetryTenantNeutralityTests
{
    /// <summary>
    /// Member-name markers for a seam that decides a tenant instead of carrying
    /// one. The same markers the gRPC binding is held to.
    /// </summary>
    private static readonly string[] ForbiddenMarkers =
        ["ResolveTenant", "DeriveTenant", "EffectiveTenant", "DefaultTenant"];

    private FakeTelemetryQueryClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTelemetryQueryClient();

    private TelemetryQueryService Create() => new(_client);

    [Test]
    public void The_seam_declares_no_tenant_resolution_surface()
    {
        var offenders = ScanAssembly(typeof(ITelemetryDomain).Assembly);

        Assert.That(
            offenders,
            Is.Empty,
            "the seam is transport and projection only; deciding a tenant here would re-implement "
            + "enforcement outside the facade. Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void The_marker_scan_is_not_vacuous()
    {
        // Guards the guard: a typo in a marker would make the check above pass for
        // every assembly, including one that really did resolve a tenant locally.
        var offenders = ScanType(typeof(WouldBeOffender));

        Assert.Multiple(() =>
        {
            Assert.That(offenders, Does.Contain($"{nameof(WouldBeOffender)}.ResolveTenant"));
            Assert.That(offenders, Does.Contain($"{nameof(WouldBeOffender)}.EffectiveTenantId"));
        });
    }

    [Test]
    public async Task Every_series_the_facade_returned_reaches_the_panel_whatever_its_tenant_label()
    {
        // The facade pinned one tenant, yet returned series labelled with another
        // and one labelled with none. The seam is not entitled to second-guess
        // that: the facade decided what this caller may see.
        _client.Scope = TelemetryTenantScope.PinnedTo(
            SampleTelemetry.CallerTenant,
            TelemetryTenantVisibility.ActiveTenant);
        _client.Series = SampleTelemetry.MixedTenantSeries();

        var result = await Create().QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));
        var series = result.Value!.Series;

        Assert.Multiple(() =>
        {
            Assert.That(series, Has.Count.EqualTo(3), "no series is dropped for carrying another tenant's label");
            Assert.That(series[0].TryGetLabel("tenant", out var first), Is.True);
            Assert.That(first, Is.EqualTo(SampleTelemetry.CallerTenant));
            Assert.That(series[1].TryGetLabel("tenant", out var second), Is.True);
            Assert.That(
                second,
                Is.EqualTo(SampleTelemetry.OtherTenant),
                "a series labelled with a tenant other than the pinned one is still returned");
            Assert.That(series[2].TryGetLabel("tenant", out _), Is.False);
        });
    }

    [Test]
    public async Task Series_order_is_the_backends_order_not_a_re_sorted_one()
    {
        _client.Series = SampleTelemetry.MixedTenantSeries();

        var result = await Create().QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Value!.Series[0].Points[0].Value, Is.EqualTo(1d));
            Assert.That(result.Value.Series[1].Points[0].Value, Is.EqualTo(3d));
        });
    }

    [Test]
    public async Task A_requested_tenant_never_becomes_the_reported_one()
    {
        // The caller asked for another tenant and the facade refused, pinning the
        // caller's own. What the panel reads must be the facade's answer.
        _client.Scope = TelemetryTenantScope.PinnedTo(
            SampleTelemetry.CallerTenant,
            TelemetryTenantVisibility.SingleTenant);

        var result = await Create().QueryAsync(new ExplorerTelemetryRequest
        {
            QueryId = SampleTelemetry.RangeQueryId,
            RequestedVisibility = ExplorerTelemetryVisibility.SingleTenant,
            RequestedTenantId = SampleTelemetry.OtherTenant,
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                _client.LastRequest!.RequestedTenantId,
                Is.EqualTo(SampleTelemetry.OtherTenant),
                "the request the caller made is forwarded verbatim");
            Assert.That(
                result.Value!.Scope.TenantId,
                Is.EqualTo(SampleTelemetry.CallerTenant),
                "the effective tenant is whatever the facade pinned, never the one that was asked for");
            Assert.That(result.Value.Scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public async Task A_widened_visibility_is_forwarded_rather_than_refused_locally()
    {
        // The seam must not pre-empt the facade's decision by refusing a widening
        // it guesses will be denied: the facade validates, and its answer - here a
        // downgrade - is what the panel labels itself with.
        _client.Scope = TelemetryTenantScope.PinnedTo(
            SampleTelemetry.CallerTenant,
            TelemetryTenantVisibility.AllTenants);

        var result = await Create().QueryAsync(new ExplorerTelemetryRequest
        {
            QueryId = SampleTelemetry.RangeQueryId,
            RequestedVisibility = ExplorerTelemetryVisibility.AllTenants,
        });

        Assert.Multiple(() =>
        {
            Assert.That(_client.QueryCallCount, Is.EqualTo(1), "the widening request is sent, not refused here");
            Assert.That(_client.LastRequest!.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(result.Value!.Scope.IsCrossTenant, Is.False);
            Assert.That(result.Value.Scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public void The_domain_model_reports_no_effective_tenant_of_its_own()
    {
        // A panel must read the effective tenant from a response's scope, which the
        // facade decided, and never from an ambient property on the client, which
        // nothing validated.
        var members = typeof(ITelemetryDomain)
            .GetMembers(BindingFlags.Public | BindingFlags.Instance)
            .Select(member => member.Name)
            .ToArray();

        Assert.That(
            members.Where(name => name.Contains("Tenant", StringComparison.Ordinal)),
            Is.Empty,
            "the domain contract offers no tenant-valued member: "
            + string.Join(", ", members));
    }

    private static string[] ScanAssembly(Assembly assembly) =>
    [
        .. assembly.GetTypes()
            .SelectMany(ScanType)
            .OrderBy(name => name, StringComparer.Ordinal),
    ];

    private static string[] ScanType(Type type) =>
    [
        .. type
            .GetMembers(BindingFlags.Public
                | BindingFlags.NonPublic
                | BindingFlags.Instance
                | BindingFlags.Static
                | BindingFlags.DeclaredOnly)
            .Where(member => ForbiddenMarkers.Any(marker =>
                member.Name.Contains(marker, StringComparison.OrdinalIgnoreCase)))
            .Select(member => $"{type.Name}.{member.Name}"),
    ];

    /// <summary>
    /// A stand-in for the seam this guard exists to prevent: it both derives a
    /// tenant and caches the derived one. Never referenced by production code; it
    /// exists so the scan above is proven to detect what it claims to.
    /// </summary>
    private sealed class WouldBeOffender
    {
        public string? EffectiveTenantId { get; set; }

        public string ResolveTenant() => EffectiveTenantId ?? SampleTelemetry.CallerTenant;
    }
}
