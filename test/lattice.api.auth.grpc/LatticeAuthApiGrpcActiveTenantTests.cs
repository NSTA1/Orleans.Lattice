using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Unit coverage for the gRPC binding's half of
/// <see cref="AuthPageRequest.ActiveTenantOnly"/>: a rule listing that asks to be
/// narrowed lifts the caller's <c>lattice-active-tenant</c> assertion onto the
/// ambient tenant for that call only, every other call ignores the header, the
/// header name is configurable, and a denied assertion maps to
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
[TestFixture]
public sealed class LatticeAuthApiGrpcActiveTenantTests
{
    private ServiceProvider _services = null!;
    private LatticeAuthApiGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeAuthApiGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeAuthApiGrpcService Service(ILatticeAuthAdmin admin, LatticeAuthApiGrpcOptions? options = null)
    {
        var bridge = Substitute.For<ILatticeAuthApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return new LatticeAuthApiGrpcService(
            _methods,
            admin,
            bridge,
            NullLogger<LatticeAuthApiGrpcService>.Instance,
            options is null ? null : Options.Create(options));
    }

    /// <summary>An admin whose listing records the ambient tenant it ran under.</summary>
    private static ILatticeAuthAdmin Recording(List<string?> seen)
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.ListRulesAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            seen.Add(LatticeActiveTenantContext.Current?.Value);
            return Task.FromResult(new AuthRulePage());
        });
        return admin;
    }

    private static LoopbackServerCallContext Context(string header = LatticeActiveTenantAssertion.DefaultHeaderName, string tenant = "acme")
    {
        var context = new LoopbackServerCallContext($"/{LatticeAuthApiGrpcMethods.ServiceName}/ListRules");
        context.RequestHeaders.Add(header, tenant);
        return context;
    }

    [Test]
    public async Task ListRules_narrowed_runs_under_the_asserted_tenant()
    {
        var seen = new List<string?>();
        var service = Service(Recording(seen));

        await service.ListRules(new AuthPageRequest { ActiveTenantOnly = true }, Context());

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(new[] { "acme" }));
            Assert.That(LatticeActiveTenantContext.Current, Is.Null, "the tenant is lifted for the one call only");
        });
    }

    [Test]
    public async Task ListRules_unnarrowed_ignores_the_header()
    {
        var seen = new List<string?>();
        var service = Service(Recording(seen));

        await service.ListRules(new AuthPageRequest(), Context());

        Assert.That(seen, Is.EqualTo(new string?[] { null }));
    }

    [Test]
    public async Task ListRules_narrowed_reads_the_configured_header_name()
    {
        var seen = new List<string?>();
        var service = Service(Recording(seen), new LatticeAuthApiGrpcOptions { ActiveTenantHeaderName = "x-tenant" });

        await service.ListRules(new AuthPageRequest { ActiveTenantOnly = true }, Context(header: "x-tenant", tenant: "globex"));
        await service.ListRules(new AuthPageRequest { ActiveTenantOnly = true }, Context());

        Assert.That(seen, Is.EqualTo(new[] { "globex", null }));
    }

    [Test]
    public void ListRules_maps_a_denied_tenant_assertion_to_PermissionDenied()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.ListRulesAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuthRulePage>>(_ => throw new LatticeTenantAccessDeniedException());
        var service = Service(admin);

        var thrown = Assert.ThrowsAsync<RpcException>(
            () => service.ListRules(new AuthPageRequest { ActiveTenantOnly = true }, Context()));

        Assert.That(thrown!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void ActiveTenantHeaderName_defaults_to_the_conventional_header()
    {
        Assert.That(new LatticeAuthApiGrpcOptions().ActiveTenantHeaderName, Is.EqualTo(LatticeActiveTenantAssertion.DefaultHeaderName));
    }
}
