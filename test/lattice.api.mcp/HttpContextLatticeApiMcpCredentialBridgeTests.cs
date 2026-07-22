using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for <see cref="HttpContextLatticeApiMcpCredentialBridge"/>, the
/// fail-closed identity bridge that lifts the authenticated MCP session principal
/// onto a <see cref="LatticeCredential"/>. Proves an unauthenticated session
/// resolves to <see langword="null"/> (anonymous, default-denied), an
/// authenticated session resolves its principal id and header token, scheme
/// stripping and casing tolerance, the custom-header / custom-scheme knobs, the
/// principal-id token fallback, and that the resolved credential stamps onto the
/// ambient <see cref="LatticeCredentialContext"/>.
/// </summary>
[TestFixture]
public sealed class HttpContextLatticeApiMcpCredentialBridgeTests
{
    private static HttpContextLatticeApiMcpCredentialBridge CreateBridge(
        LatticeApiMcpOptions? options = null) =>
        new(Options.Create(options ?? new LatticeApiMcpOptions()));

    private static DefaultHttpContext AnonymousContext(params (string Key, string Value)[] headers)
    {
        var context = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity()),
        };
        AddHeaders(context, headers);
        return context;
    }

    private static DefaultHttpContext AuthenticatedContext(
        string? nameIdentifier = "alice",
        string? name = null,
        params (string Key, string Value)[] headers)
    {
        var claims = new List<Claim>();
        if (nameIdentifier is not null)
        {
            claims.Add(new Claim(ClaimTypes.NameIdentifier, nameIdentifier));
        }

        if (name is not null)
        {
            claims.Add(new Claim(ClaimTypes.Name, name));
        }

        var context = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity(claims, "TestAuth", ClaimTypes.Name, ClaimTypes.Role)),
        };
        AddHeaders(context, headers);
        return context;
    }

    private static void AddHeaders(HttpContext context, (string Key, string Value)[] headers)
    {
        foreach (var (key, value) in headers)
        {
            context.Request.Headers[key] = value;
        }
    }

    [Test]
    public void Resolve_returns_null_when_session_is_unauthenticated()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(AnonymousContext(("authorization", "Bearer alice-token")));

        Assert.That(credential, Is.Null,
            "An unauthenticated MCP session must resolve to an anonymous (null) credential.");
    }

    [Test]
    public void Resolve_returns_null_when_no_principal_is_present()
    {
        var bridge = CreateBridge();
        var context = new DefaultHttpContext();

        var credential = bridge.Resolve(context);

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_strips_bearer_scheme_and_keeps_token()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(
            AuthenticatedContext(headers: ("authorization", "Bearer alice-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("alice-token"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
            Assert.That(credential.Value.PrincipalId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void Resolve_is_case_insensitive_on_the_scheme_prefix()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(
            AuthenticatedContext(headers: ("authorization", "bEaReR alice-token")));

        Assert.That(credential!.Value.Token, Is.EqualTo("alice-token"));
    }

    [Test]
    public void Resolve_keeps_raw_token_when_no_scheme_prefix_present()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(
            AuthenticatedContext(headers: ("authorization", "bare-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("bare-token"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_falls_back_to_principal_id_when_no_token_header()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(AuthenticatedContext("bob"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("bob"));
            Assert.That(credential.Value.PrincipalId, Is.EqualTo("bob"));
        });
    }

    [Test]
    public void Resolve_falls_back_to_principal_id_when_only_scheme_present()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(
            AuthenticatedContext("carol", headers: ("authorization", "Bearer ")));

        Assert.That(credential!.Value.Token, Is.EqualTo("carol"));
    }

    [Test]
    public void Resolve_uses_identity_name_when_no_name_identifier_claim()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(
            AuthenticatedContext(nameIdentifier: null, name: "dave"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.PrincipalId, Is.EqualTo("dave"));
            Assert.That(credential.Value.Token, Is.EqualTo("dave"));
        });
    }

    [Test]
    public void Resolve_returns_null_when_authenticated_but_no_identity_and_no_token()
    {
        var bridge = CreateBridge();

        // Authenticated identity with no name-identifier, no name, and no token
        // header: nothing resolves, so the caller reads as anonymous.
        var credential = bridge.Resolve(AuthenticatedContext(nameIdentifier: null));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_honours_a_custom_header_name()
    {
        var bridge = CreateBridge(new LatticeApiMcpOptions
        {
            CredentialHeaderName = "x-lattice-cred",
        });

        var credential = bridge.Resolve(
            AuthenticatedContext(headers: ("x-lattice-cred", "Bearer alice-token")));

        Assert.That(credential!.Value.Token, Is.EqualTo("alice-token"));
    }

    [Test]
    public void Resolve_with_empty_scheme_keeps_whole_header_and_null_scheme()
    {
        var bridge = CreateBridge(new LatticeApiMcpOptions
        {
            CredentialScheme = string.Empty,
        });

        var credential = bridge.Resolve(
            AuthenticatedContext(headers: ("authorization", "Bearer alice-token")));

        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo("Bearer alice-token"));
            Assert.That(credential.Value.Scheme, Is.Null);
        });
    }

    [Test]
    public void Resolve_throws_on_null_context()
    {
        var bridge = CreateBridge();

        Assert.Throws<ArgumentNullException>(() => bridge.Resolve(null!));
    }

    [Test]
    public void Resolve_prefers_oid_over_sub_for_the_principal_id()
    {
        // A delegated Entra token: `sub` is a pairwise (user, client-app) id that
        // differs from the durable `oid`. Discovery must key on `oid` so it
        // introspects the same subject the silo enforces on.
        var bridge = CreateBridge();
        var context = AuthenticatedContextWithClaims(
            new Claim("oid", "object-id-123"),
            new Claim(ClaimTypes.NameIdentifier, "pairwise-sub-456"));

        var credential = bridge.Resolve(context);

        Assert.That(credential!.Value.PrincipalId, Is.EqualTo("object-id-123"),
            "The durable oid must be preferred over the pairwise sub.");
    }

    [Test]
    public void Resolve_prefers_the_mapped_oid_schema_uri_over_sub()
    {
        // With the default JWT inbound claim mapping enabled, `oid` surfaces as the
        // WS-* object-identifier schema URI. It must still be preferred over `sub`.
        var bridge = CreateBridge();
        var context = AuthenticatedContextWithClaims(
            new Claim("http://schemas.microsoft.com/identity/claims/objectidentifier", "object-id-789"),
            new Claim(ClaimTypes.NameIdentifier, "pairwise-sub-000"));

        var credential = bridge.Resolve(context);

        Assert.That(credential!.Value.PrincipalId, Is.EqualTo("object-id-789"));
    }

    [Test]
    public void Resolve_falls_back_to_sub_when_no_oid_claim()
    {
        // An authenticator that carries no object id: the `sub` fallback stands.
        var bridge = CreateBridge();
        var context = AuthenticatedContextWithClaims(
            new Claim(ClaimTypes.NameIdentifier, "sub-only"));

        var credential = bridge.Resolve(context);

        Assert.That(credential!.Value.PrincipalId, Is.EqualTo("sub-only"));
    }

    [Test]
    public void Resolve_reads_the_raw_sub_claim_when_present()
    {
        // With claim mapping disabled the subject surfaces as the raw `sub` name.
        var bridge = CreateBridge();
        var context = AuthenticatedContextWithClaims(new Claim("sub", "raw-sub-1"));

        var credential = bridge.Resolve(context);

        Assert.That(credential!.Value.PrincipalId, Is.EqualTo("raw-sub-1"));
    }

    private static DefaultHttpContext AuthenticatedContextWithClaims(params Claim[] claims)
        => new()
        {
            User = new ClaimsPrincipal(new ClaimsIdentity(claims, "TestAuth", ClaimTypes.Name, ClaimTypes.Role)),
        };

    [Test]
    public void Resolved_credential_stamps_onto_the_ambient_credential_context()
    {
        var bridge = CreateBridge();
        var credential = bridge.Resolve(
            AuthenticatedContext(headers: ("authorization", "Bearer alice-token")));

        Assert.That(credential, Is.Not.Null);

        using (LatticeCredentialContext.With(credential))
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticeCredentialContext.IsActive, Is.True);
                Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("alice-token"));
                Assert.That(LatticeCredentialContext.Current!.Value.PrincipalId, Is.EqualTo("alice"));
            });
        }

        Assert.That(LatticeCredentialContext.IsActive, Is.False,
            "The credential scope must clear when disposed.");
    }
}
