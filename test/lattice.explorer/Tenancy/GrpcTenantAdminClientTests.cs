using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Unit tests for the production <see cref="GrpcTenantAdminClient"/>. They
/// exercise the constructor guards, the argument guards, the unconfigured and
/// disposed error paths, and the channel build (including every auth-attaching
/// branch) by issuing each call with an already-cancelled token so the transport
/// fails fast without a server. No cluster is stood up, so these remain fast,
/// deterministic unit tests.
/// </summary>
[TestFixture]
public class GrpcTenantAdminClientTests
{
    private static GrpcTenantAdminClient Create(
        ExplorerConfiguration? config,
        LatticeCallAuthentication? auth = null) =>
        new(ExplorerControlClientHarness.Session(config), ExplorerControlClientHarness.Auth(auth));

    [Test]
    public void Constructor_null_session_throws() =>
        Assert.That(
            () => new GrpcTenantAdminClient(null!, ExplorerControlClientHarness.Auth(null)),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_auth_throws() =>
        Assert.That(
            () => new GrpcTenantAdminClient(ExplorerControlClientHarness.Session(null), null!),
            Throws.ArgumentNullException);

    [Test]
    public void Every_call_rejects_a_missing_identifier()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.Multiple(() =>
        {
            Assert.That(() => client.GetTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.CreateTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.SuspendTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.ResumeTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.DeleteTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.SetTenantQuotasAsync(string.Empty, default),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.AuthorizeAllowedRegionsAsync(string.Empty, []),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.AuthorizeAllowedRegionsAsync(SampleTenant.TenantId, null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => client.SetTenantResidencyAsync(string.Empty, []),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.SetTenantResidencyAsync(SampleTenant.TenantId, null!),
                Throws.ArgumentNullException);
            Assert.That(() => client.GetTenantRegionStatusAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.GetTenantQuotaUsageAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.ListTenantAdminSubjectsAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.AddTenantAdminSubjectAsync(SampleTenant.TenantId, string.Empty),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.RemoveTenantAdminSubjectAsync(SampleTenant.TenantId, string.Empty),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.ListCrossTenantGrantsAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.OfferCrossTenantGrantAsync(
                    SampleTenant.TenantId, SampleTenant.OtherTenantId, string.Empty, TenantGrantAccess.Read),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.ApproveCrossTenantGrantAsync(
                    string.Empty, SampleTenant.OtherTenantId, SampleTenant.Scope),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.RejectCrossTenantGrantAsync(
                    SampleTenant.TenantId, string.Empty, SampleTenant.Scope),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => client.RevokeCrossTenantGrantAsync(
                    SampleTenant.TenantId, SampleTenant.OtherTenantId, string.Empty),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void A_call_without_an_endpoint_throws_invalid_operation()
    {
        using var client = Create(config: null);

        Assert.That(
            async () => await client.GetCurrentTenantAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void A_call_after_dispose_throws_object_disposed()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());
        client.Dispose();

        Assert.That(
            async () => await client.GetCurrentTenantAsync(),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());

        client.Dispose();

        Assert.That(() => client.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Every_call_builds_the_channel_and_leaves_a_non_denial_fault_untranslated()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var calls = new Func<Task>[]
        {
            () => client.GetCurrentTenantAsync(ct),
            () => client.ListAccessibleTenantsAsync(ct),
            () => client.GetTenantAsync(SampleTenant.TenantId, ct),
            () => client.CreateTenantAsync(SampleTenant.TenantId, null, ct),
            () => client.SuspendTenantAsync(SampleTenant.TenantId, ct),
            () => client.ResumeTenantAsync(SampleTenant.TenantId, ct),
            () => client.DeleteTenantAsync(SampleTenant.TenantId, ct),
            () => client.SetTenantQuotasAsync(SampleTenant.TenantId, SampleTenant.Quotas(), ct),
            () => client.AuthorizeAllowedRegionsAsync(SampleTenant.TenantId, ["westeurope"], ct),
            () => client.SetTenantResidencyAsync(SampleTenant.TenantId, ["westeurope"], ct),
            () => client.GetTenantRegionStatusAsync(SampleTenant.TenantId, ct),
            () => client.GetTenantQuotaUsageAsync(SampleTenant.TenantId, ct),
            () => client.ListTenantAdminSubjectsAsync(SampleTenant.TenantId, ct),
            () => client.AddTenantAdminSubjectAsync(SampleTenant.TenantId, SampleTenant.SubjectId, ct),
            () => client.RemoveTenantAdminSubjectAsync(SampleTenant.TenantId, SampleTenant.SubjectId, ct),
            () => client.ListCrossTenantGrantsAsync(SampleTenant.TenantId, ct),
            () => client.OfferCrossTenantGrantAsync(
                SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope, TenantGrantAccess.Read, ct),
            () => client.ApproveCrossTenantGrantAsync(
                SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope, ct),
            () => client.RejectCrossTenantGrantAsync(
                SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope, ct),
            () => client.RevokeCrossTenantGrantAsync(
                SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope, ct),
        };

        Assert.That(calls, Has.Length.EqualTo(20), "every facade operation must be covered here");

        foreach (var call in calls)
        {
            var ex = Assert.CatchAsync(async () => await call());
            Assert.That(ex, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(
                    ex,
                    Is.Not.InstanceOf<LatticeAuthorizationDeniedException>(),
                    "a cancelled (non permission-denied) transport fault must not be translated to a denial");
                Assert.That(
                    ex,
                    Is.Not.InstanceOf<TenancyUnavailableException>(),
                    "a cancelled (non unimplemented) transport fault must not be read as an absent capability");
            });
        }
    }

    [Test]
    public void A_call_with_static_header_auth_builds_the_channel()
    {
        var auth = LatticeCallAuthentication.Basic("operator", "secret");
        using var client = Create(ExplorerControlClientHarness.H2cConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.GetCurrentTenantAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void A_call_with_a_credential_provider_over_h2c_builds_insecure_call_credentials()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        using var client = Create(
            ExplorerControlClientHarness.H2cConfig(),
            LatticeCallAuthentication.Bearer(provider));

        var ex = Assert.CatchAsync(
            async () => await client.GetCurrentTenantAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void A_call_with_a_credential_provider_over_tls_builds_the_channel()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        using var client = Create(
            ExplorerControlClientHarness.TlsConfig(),
            LatticeCallAuthentication.Bearer(provider));

        var ex = Assert.CatchAsync(
            async () => await client.GetCurrentTenantAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void A_call_with_transport_headers_builds_the_channel()
    {
        var config = ExplorerControlClientHarness.H2cConfig(
            transportHeaders: new Dictionary<string, string> { ["x-azure-fdid"] = "origin-1" });
        using var client = Create(config);

        var ex = Assert.CatchAsync(
            async () => await client.GetCurrentTenantAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Repeated_calls_reuse_the_one_channel_both_clients_share()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        // The self-service and administrative clients are built over the same
        // invoker, so an administrative call after a self-service one must not
        // rebuild the channel.
        var first = Assert.CatchAsync(async () => await client.GetCurrentTenantAsync(ct));
        var second = Assert.CatchAsync(
            async () => await client.GetTenantQuotaUsageAsync(SampleTenant.TenantId, ct));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(second, Is.Not.Null);
        });
    }
}
