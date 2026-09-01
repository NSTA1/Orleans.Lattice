using Grpc.Core;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for the shipped authorization seam of the tree-administration
/// gRPC binding that is reachable without a live server: the two shipped
/// authorizers, the authorization context's carried state, the header credential
/// bridge, and the options-backed auth-scheme advertisement source.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminApiAuthSeamTests
{
    private static FakeServerCallContext ContextWithHeader(string? name, string? value)
    {
        var headers = new global::Grpc.Core.Metadata();
        if (name is not null && value is not null)
        {
            headers.Add(name, value);
        }

        return new FakeServerCallContext("/orleans.lattice.api.treeadmin/CheckTreeExists", headers);
    }

    private static HeaderLatticeTreeAdminApiCredentialBridge CreateBridge(LatticeTreeAdminApiGrpcOptions options) =>
        new(Options.Create(options));

    // ----- Authorizers -----

    [Test]
    public async Task DenyTreeAdminApiAuthorizer_refuses_every_call()
    {
        var authorizer = new DenyTreeAdminApiAuthorizer();
        var context = new LatticeTreeAdminApiAuthorizationContext(
            ContextWithHeader(null, null), LatticeTreeAdminApiOperation.ProbeCapabilities, "orders");

        Assert.That(await authorizer.IsAuthorizedAsync(context, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task DenyTreeAdminApiAuthorizer_refuses_the_unknown_operation()
    {
        // The default-deny posture must not soften for an unmapped RPC: an
        // unrecognised method reaches the authorizer as Unknown and stays denied.
        var authorizer = new DenyTreeAdminApiAuthorizer();
        var context = new LatticeTreeAdminApiAuthorizationContext(
            ContextWithHeader(null, null), LatticeTreeAdminApiOperation.Unknown, targetId: null);

        Assert.That(await authorizer.IsAuthorizedAsync(context, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task AllowAllTreeAdminApiAuthorizer_permits_every_call()
    {
        var authorizer = new AllowAllTreeAdminApiAuthorizer();
        var context = new LatticeTreeAdminApiAuthorizationContext(
            ContextWithHeader(null, null), LatticeTreeAdminApiOperation.CreateTree, "orders");

        Assert.That(await authorizer.IsAuthorizedAsync(context, CancellationToken.None), Is.True);
    }

    // ----- Authorization context -----

    [Test]
    public void AuthorizationContext_exposes_the_call_operation_and_target()
    {
        var call = ContextWithHeader(null, null);

        var context = new LatticeTreeAdminApiAuthorizationContext(
            call, LatticeTreeAdminApiOperation.SetTreeConfig, "orders");

        Assert.Multiple(() =>
        {
            Assert.That(context.Call, Is.SameAs(call));
            Assert.That(context.Operation, Is.EqualTo(LatticeTreeAdminApiOperation.SetTreeConfig));
            Assert.That(context.TargetId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void AuthorizationContext_allows_a_null_target_for_an_unscoped_operation()
    {
        var context = new LatticeTreeAdminApiAuthorizationContext(
            ContextWithHeader(null, null), LatticeTreeAdminApiOperation.GetStorageUsage, targetId: null);

        Assert.That(context.TargetId, Is.Null);
    }

    [Test]
    public void AuthorizationContext_rejects_a_null_call()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new LatticeTreeAdminApiAuthorizationContext(
            null!, LatticeTreeAdminApiOperation.ProbeCapabilities, "orders"));
    }

    // ----- Credential bridge -----

    [Test]
    public void CredentialBridge_rejects_a_null_options_accessor()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new HeaderLatticeTreeAdminApiCredentialBridge(null!));
    }

    [Test]
    public void CredentialBridge_rejects_a_null_context()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        Assert.Throws<ArgumentNullException>(() => bridge.Resolve(null!));
    }

    [Test]
    public void CredentialBridge_strips_the_configured_scheme_prefix()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearer opaque-token"));

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("opaque-token"));
            Assert.That(credential!.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void CredentialBridge_matches_the_scheme_prefix_case_insensitively()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        var credential = bridge.Resolve(ContextWithHeader("authorization", "bEaReR opaque-token"));

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Value.Token, Is.EqualTo("opaque-token"));
    }

    [Test]
    public void CredentialBridge_normalises_a_mixed_case_configured_header_name()
    {
        // gRPC stores metadata keys lower-cased, so a host that configures
        // "Authorization" must still match the inbound "authorization" entry.
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions { CredentialHeaderName = "X-Lattice-Token" });

        var credential = bridge.Resolve(ContextWithHeader("x-lattice-token", "Bearer opaque-token"));

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Value.Token, Is.EqualTo("opaque-token"));
    }

    [Test]
    public void CredentialBridge_keeps_a_token_that_does_not_carry_the_scheme_prefix()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        var credential = bridge.Resolve(ContextWithHeader("authorization", "opaque-token"));

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("opaque-token"));
            Assert.That(credential!.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void CredentialBridge_does_not_strip_a_prefix_that_only_shares_a_leading_substring()
    {
        // "BearerToken" starts with "Bearer" but is not the scheme: without the
        // delimiter check the bridge would silently truncate a legitimate token.
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        var credential = bridge.Resolve(ContextWithHeader("authorization", "BearerToken"));

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Value.Token, Is.EqualTo("BearerToken"));
    }

    [Test]
    public void CredentialBridge_reports_anonymous_for_a_bare_scheme_with_no_token()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        Assert.That(bridge.Resolve(ContextWithHeader("authorization", "Bearer")), Is.Null);
    }

    [Test]
    public void CredentialBridge_reports_anonymous_for_a_scheme_followed_only_by_whitespace()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        Assert.That(bridge.Resolve(ContextWithHeader("authorization", "Bearer   ")), Is.Null);
    }

    [Test]
    public void CredentialBridge_reports_anonymous_when_the_header_is_absent()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions());

        Assert.That(bridge.Resolve(ContextWithHeader(null, null)), Is.Null);
    }

    [Test]
    public void CredentialBridge_reports_anonymous_when_the_header_name_is_disabled()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions { CredentialHeaderName = string.Empty });

        Assert.That(bridge.Resolve(ContextWithHeader("authorization", "Bearer opaque-token")), Is.Null);
    }

    [Test]
    public void CredentialBridge_stamps_a_null_scheme_when_none_is_configured()
    {
        var bridge = CreateBridge(new LatticeTreeAdminApiGrpcOptions { CredentialScheme = string.Empty });

        var credential = bridge.Resolve(ContextWithHeader("authorization", "opaque-token"));

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("opaque-token"));
            Assert.That(credential!.Value.Scheme, Is.Null);
        });
    }

    // ----- Auth-scheme advertisement source -----

    [Test]
    public void AuthSchemeSource_rejects_a_null_options_monitor()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new OptionsLatticeTreeAdminApiAuthSchemeSource(null!));
    }

    [Test]
    public void AuthSchemeSource_advertises_nothing_when_no_scheme_is_configured()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeTreeAdminApiGrpcOptions());
        var source = new OptionsLatticeTreeAdminApiAuthSchemeSource(monitor);

        Assert.That(source.GetAdvertisement().Schemes, Is.Empty);
    }

    [Test]
    public void AuthSchemeSource_projects_the_configured_schemes_in_preference_order()
    {
        var options = new LatticeTreeAdminApiGrpcOptions();
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra ID" });
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "basic" });
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();
        monitor.CurrentValue.Returns(options);
        var source = new OptionsLatticeTreeAdminApiAuthSchemeSource(monitor);

        var advertisement = source.GetAdvertisement();

        Assert.Multiple(() =>
        {
            Assert.That(
                advertisement.Schemes.Select(static s => s.SchemeId).ToArray(),
                Is.EqualTo(new[] { "entra", "basic" }));
            Assert.That(advertisement.Schemes[0].DisplayName, Is.EqualTo("Entra ID"));
        });
    }
}
