using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="HeaderLatticeReplicationApiCredentialBridge"/>: it
/// lifts a single configured request header into a <see cref="LatticeCredential"/>,
/// strips a case-insensitive scheme prefix, and fails closed (yields
/// <see langword="null"/>, an anonymous caller) for an absent, empty, whitespace,
/// or bare-scheme header. Driven directly against a <see cref="FakeServerCallContext"/>
/// so no gRPC host is stood up.
/// </summary>
public sealed class HeaderLatticeReplicationApiCredentialBridgeTests
{
    private static HeaderLatticeReplicationApiCredentialBridge CreateBridge(
        LatticeReplicationApiGrpcOptions? options = null) =>
        new(Options.Create(options ?? new LatticeReplicationApiGrpcOptions()));

    private static FakeServerCallContext ContextWithHeader(string key, string value) =>
        new("/orleans.lattice.api.replication/EnableReplication", new global::Grpc.Core.Metadata { { key, value } });

    private static FakeServerCallContext ContextWithNoHeaders() =>
        new("/orleans.lattice.api.replication/EnableReplication");

    [Test]
    public void Resolve_null_context_throws()
    {
        var bridge = CreateBridge();
        Assert.Throws<ArgumentNullException>(() => bridge.Resolve(null!));
    }

    [Test]
    public void Resolve_empty_configured_header_name_returns_null()
    {
        var bridge = CreateBridge(new LatticeReplicationApiGrpcOptions { CredentialHeaderName = string.Empty });

        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearer token"));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_absent_header_returns_null()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWithNoHeaders());

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_whitespace_header_returns_null()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWithHeader("authorization", "   "));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_bearer_prefixed_token_strips_scheme_and_stamps_credential()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearer secret-token"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("secret-token"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_case_insensitive_scheme_prefix_is_stripped()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWithHeader("authorization", "bEaReR secret-token"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("secret-token"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_bare_scheme_with_trailing_space_returns_null()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearer "));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_bare_scheme_with_no_token_returns_null()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearer"));

        Assert.That(credential, Is.Null);
    }

    [Test]
    public void Resolve_token_without_scheme_prefix_stamps_configured_scheme()
    {
        var bridge = CreateBridge();

        var credential = bridge.Resolve(ContextWithHeader("authorization", "plain-token"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("plain-token"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_scheme_prefix_without_delimiter_is_not_stripped()
    {
        var bridge = CreateBridge();

        // "Bearertoken" starts with the scheme letters but has no whitespace
        // delimiter after them, so it is treated as an opaque token, not a
        // scheme-prefixed one.
        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearertoken"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("Bearertoken"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void Resolve_empty_configured_scheme_stamps_null_scheme()
    {
        var bridge = CreateBridge(new LatticeReplicationApiGrpcOptions { CredentialScheme = string.Empty });

        var credential = bridge.Resolve(ContextWithHeader("authorization", "Bearer opaque"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            // With no configured scheme nothing is stripped and no scheme is stamped.
            Assert.That(credential!.Value.Token, Is.EqualTo("Bearer opaque"));
            Assert.That(credential.Value.Scheme, Is.Null);
        });
    }

    [Test]
    public void Resolve_custom_header_name_is_matched_case_insensitively()
    {
        var bridge = CreateBridge(new LatticeReplicationApiGrpcOptions
        {
            CredentialHeaderName = "X-Lattice-Cred",
            CredentialScheme = string.Empty,
        });

        // gRPC metadata keys are stored lower-cased; the bridge lower-cases the
        // configured name before lookup so any casing matches.
        var credential = bridge.Resolve(ContextWithHeader("x-lattice-cred", "opaque-token"));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("opaque-token"));
        });
    }
}
