using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Proves the binding is <b>transport only</b>: it carries the caller's credential
/// and asserted active tenant into the ambient context so the facade can derive
/// the effective tenant, and it never derives, substitutes, or rewrites a tenant
/// of its own. This is the D5 invariant - the facade is the single enforcement
/// point, and a routable facade exists precisely so a desktop head cannot enforce
/// (or bypass) scoping locally.
/// </summary>
[TestFixture]
public sealed class TelemetryGrpcTenantNeutralityTests
{
    private ServiceProvider _serializers = null!;
    private FakeTelemetry _facade = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = TelemetryGrpcTestSupport.Serializers();
        _facade = new FakeTelemetry();
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private static global::Grpc.Core.Metadata Headers(params (string Key, string Value)[] entries)
    {
        var metadata = new global::Grpc.Core.Metadata();
        foreach (var (key, value) in entries)
        {
            metadata.Add(key, value);
        }

        return metadata;
    }

    [Test]
    public async Task The_caller_credential_is_bridged_onto_the_ambient_context()
    {
        var credential = new LatticeCredential("token-value", "Bearer");
        var service = TelemetryGrpcTestSupport.Service(
            _serializers,
            _facade,
            credentialBridge: new FixedCredentialBridge(credential));

        await service.GetCatalog(
            new TelemetryCatalogRequest(),
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName)));

        Assert.That(_facade.ObservedCredential, Is.EqualTo(credential));
    }

    [Test]
    public async Task The_credential_scope_does_not_leak_past_the_call()
    {
        var service = TelemetryGrpcTestSupport.Service(
            _serializers,
            _facade,
            credentialBridge: new FixedCredentialBridge(new LatticeCredential("token-value", "Bearer")));

        await service.GetCatalog(
            new TelemetryCatalogRequest(),
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName)));

        Assert.That(
            LatticeCredentialContext.Current,
            Is.Null,
            "A leaked credential scope would let one call's identity serve the next one.");
    }

    [Test]
    public async Task An_anonymous_call_stamps_no_credential()
    {
        var service = TelemetryGrpcTestSupport.Service(_serializers, _facade);

        await service.GetCatalog(
            new TelemetryCatalogRequest(),
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName)));

        Assert.That(_facade.ObservedCredential, Is.Null);
    }

    [Test]
    public async Task The_asserted_active_tenant_header_is_carried_onto_the_ambient_context()
    {
        var service = TelemetryGrpcTestSupport.Service(_serializers, _facade);

        await service.Query(
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
                Headers((LatticeActiveTenantAssertion.DefaultHeaderName, "acme"))));

        Assert.That(_facade.ObservedActiveTenant?.Value, Is.EqualTo("acme"));
    }

    [Test]
    public async Task A_call_with_no_tenant_header_stamps_no_active_tenant()
    {
        var service = TelemetryGrpcTestSupport.Service(_serializers, _facade);

        await service.Query(
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName)));

        Assert.That(_facade.ObservedActiveTenant, Is.Null);
    }

    [Test]
    public async Task The_active_tenant_scope_does_not_leak_past_the_call()
    {
        var service = TelemetryGrpcTestSupport.Service(_serializers, _facade);

        await service.Query(
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
                Headers((LatticeActiveTenantAssertion.DefaultHeaderName, "acme"))));

        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
    }

    [Test]
    public async Task The_requested_tenant_on_the_wire_never_becomes_the_effective_scope()
    {
        var service = TelemetryGrpcTestSupport.Service(_serializers, _facade);

        var response = await service.Query(
            new TelemetryQueryRequest
            {
                QueryId = "lattice.ops.rate",
                RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
                RequestedTenantId = "a-tenant-the-caller-picked",
            },
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(
                response.Scope.TenantId,
                Is.EqualTo(FakeTelemetry.PinnedTenantId),
                "The effective tenant is whatever the facade pinned - never the one the wire asked for.");
            Assert.That(response.Scope.TenantId, Is.Not.EqualTo("a-tenant-the-caller-picked"));
            Assert.That(response.Scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public void The_binding_declares_no_tenant_resolution_surface()
    {
        // A member that resolves, derives, or defaults a tenant would be the
        // bypassable path a routable facade exists to prevent. The binding may only
        // *carry* an assertion (StampActiveTenant), never decide one.
        string[] forbiddenMarkers = ["ResolveTenant", "DeriveTenant", "EffectiveTenant", "DefaultTenant"];

        var offenders = typeof(LatticeTelemetryApiGrpcClient).Assembly
            .GetTypes()
            .SelectMany(type => type
                .GetMembers(System.Reflection.BindingFlags.Public
                    | System.Reflection.BindingFlags.NonPublic
                    | System.Reflection.BindingFlags.Instance
                    | System.Reflection.BindingFlags.Static
                    | System.Reflection.BindingFlags.DeclaredOnly)
                .Select(member => (Type: type, Member: member)))
            .Where(x => forbiddenMarkers.Any(marker =>
                x.Member.Name.Contains(marker, StringComparison.OrdinalIgnoreCase)))
            .Select(x => $"{x.Type.Name}.{x.Member.Name}")
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "The binding is transport only; deriving a tenant here would re-implement enforcement "
            + "outside the facade. Offenders: " + string.Join(", ", offenders));
    }
}
