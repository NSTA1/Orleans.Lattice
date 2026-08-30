namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Coverage for the binding options: the fail-closed defaults a host inherits when
/// it configures nothing, and the mutability of each knob.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryApiGrpcOptionsTests
{
    [Test]
    public void The_defaults_are_fail_closed_and_match_the_sibling_bindings()
    {
        var options = new LatticeTelemetryApiGrpcOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("authorization"));
            Assert.That(options.CredentialScheme, Is.EqualTo("Bearer"));
            Assert.That(options.ActiveTenantHeaderName, Is.EqualTo(LatticeActiveTenantAssertion.DefaultHeaderName));
            Assert.That(options.AdvertisedAuthSchemes, Is.Empty);
        });
    }

    [Test]
    public void The_active_tenant_header_default_matches_what_the_api_clients_send()
        => Assert.That(
            new LatticeTelemetryApiGrpcOptions().ActiveTenantHeaderName,
            Is.EqualTo("lattice-active-tenant"));

    [Test]
    public void Every_knob_is_settable()
    {
        var options = new LatticeTelemetryApiGrpcOptions
        {
            RequireAuthorization = false,
            CredentialHeaderName = "x-token",
            CredentialScheme = "Basic",
            ActiveTenantHeaderName = "x-tenant",
        };
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "basic" });

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.False);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("x-token"));
            Assert.That(options.CredentialScheme, Is.EqualTo("Basic"));
            Assert.That(options.ActiveTenantHeaderName, Is.EqualTo("x-tenant"));
            Assert.That(options.AdvertisedAuthSchemes, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void The_deny_authorizer_refuses_every_call()
    {
        var context = new LatticeTelemetryApiAuthorizationContext(
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName)),
            LatticeTelemetryApiOperation.Query,
            "lattice.ops.rate");

        Assert.That(
            new DenyTelemetryApiAuthorizer().IsAuthorizedAsync(context, CancellationToken.None).Result,
            Is.False);
    }

    [Test]
    public void The_allow_all_authorizer_permits_every_call()
    {
        var context = new LatticeTelemetryApiAuthorizationContext(
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName)),
            LatticeTelemetryApiOperation.GetCatalog,
            targetId: null);

        Assert.That(
            new AllowAllTelemetryApiAuthorizer().IsAuthorizedAsync(context, CancellationToken.None).Result,
            Is.True);
    }

    [Test]
    public void The_authorization_context_rejects_a_null_call()
        => Assert.That(
            () => new LatticeTelemetryApiAuthorizationContext(
                null!, LatticeTelemetryApiOperation.Query, "q"),
            Throws.ArgumentNullException);

    [Test]
    public void The_authorization_context_carries_its_operation_and_target()
    {
        var call = new FakeServerCallContext(
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName));

        var context = new LatticeTelemetryApiAuthorizationContext(
            call,
            LatticeTelemetryApiOperation.Query,
            "lattice.ops.rate");

        Assert.Multiple(() =>
        {
            Assert.That(context.Call, Is.SameAs(call));
            Assert.That(context.Operation, Is.EqualTo(LatticeTelemetryApiOperation.Query));
            Assert.That(context.TargetId, Is.EqualTo("lattice.ops.rate"));
        });
    }

    [Test]
    public void The_operation_enum_covers_every_gated_rpc_plus_unknown()
        => Assert.That(
            Enum.GetNames<LatticeTelemetryApiOperation>().OrderBy(name => name, StringComparer.Ordinal),
            Is.EqualTo(new[] { "GetCatalog", "Query", "Unknown" }));
}
