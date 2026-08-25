using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="HeaderLatticeTenantAdminApiCredentialBridge"/>, the
/// default identity seam that lifts a single configurable request header into a
/// <see cref="LatticeCredential"/>. Proves the bearer-prefix strip, the fail-closed
/// treatment of a missing / blank / bare-scheme header (anonymous, so a mutation
/// can never be driven credential-less), and the null-context guard.
/// </summary>
[TestFixture]
public sealed class HeaderLatticeTenantAdminApiCredentialBridgeTests
{
    private static HeaderLatticeTenantAdminApiCredentialBridge Bridge(LatticeTenantAdminApiGrpcOptions? options = null) =>
        new(Options.Create(options ?? new LatticeTenantAdminApiGrpcOptions()));

    private static FakeServerCallContext CallWith(params (string Key, string Value)[] headers)
    {
        var metadata = new global::Grpc.Core.Metadata();
        foreach (var (key, value) in headers)
        {
            metadata.Add(key, value);
        }

        return new FakeServerCallContext("/orleans.lattice.api.tenantadmin/CreateTenant", metadata);
    }

    [Test]
    public void Resolve_strips_the_bearer_scheme_and_returns_the_token()
    {
        var credential = Bridge().Resolve(CallWith(("authorization", "Bearer secret-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("secret-token"));
            Assert.That(credential!.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_is_case_insensitive_on_the_scheme_prefix()
    {
        var credential = Bridge().Resolve(CallWith(("authorization", "bearer secret-token")));

        Assert.That(credential!.Value.Token, Is.EqualTo("secret-token"));
    }

    [Test]
    public void Resolve_returns_null_when_the_header_is_absent()
    {
        Assert.That(Bridge().Resolve(CallWith()), Is.Null);
    }

    [Test]
    public void Resolve_returns_null_for_a_bare_scheme_with_no_token()
    {
        Assert.That(Bridge().Resolve(CallWith(("authorization", "Bearer "))), Is.Null);
    }

    [Test]
    public void Resolve_returns_null_when_the_header_name_is_cleared()
    {
        var options = new LatticeTenantAdminApiGrpcOptions { CredentialHeaderName = string.Empty };

        Assert.That(Bridge(options).Resolve(CallWith(("authorization", "Bearer tok"))), Is.Null);
    }

    [Test]
    public void Resolve_keeps_the_whole_value_when_no_scheme_prefix_matches()
    {
        // With no scheme configured, the raw token is used verbatim.
        var options = new LatticeTenantAdminApiGrpcOptions { CredentialScheme = string.Empty };

        var credential = Bridge(options).Resolve(CallWith(("authorization", "raw-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("raw-token"));
            Assert.That(credential!.Value.Scheme, Is.Null);
        });
    }

    [Test]
    public void Resolve_rejects_a_null_context()
    {
        Assert.That(() => Bridge().Resolve(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_null_options()
    {
        Assert.That(() => new HeaderLatticeTenantAdminApiCredentialBridge(null!), Throws.ArgumentNullException);
    }
}
