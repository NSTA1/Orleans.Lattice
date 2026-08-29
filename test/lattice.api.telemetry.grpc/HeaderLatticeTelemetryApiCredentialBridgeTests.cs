using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Coverage for the default header-backed credential bridge: which header shapes
/// resolve to a credential, and which resolve to none (an anonymous caller, which
/// the facade then scopes fail-closed).
/// </summary>
[TestFixture]
public sealed class HeaderLatticeTelemetryApiCredentialBridgeTests
{
    private static HeaderLatticeTelemetryApiCredentialBridge Bridge(LatticeTelemetryApiGrpcOptions? options = null)
        => new(Options.Create(options ?? new LatticeTelemetryApiGrpcOptions()));

    private static FakeServerCallContext Context(params (string Key, string Value)[] headers)
    {
        var metadata = new global::Grpc.Core.Metadata();
        foreach (var (key, value) in headers)
        {
            metadata.Add(key, value);
        }

        return new FakeServerCallContext(
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            metadata);
    }

    [Test]
    public void A_bearer_header_resolves_the_token_without_the_scheme_prefix()
    {
        var credential = Bridge().Resolve(Context(("authorization", "Bearer abc.def.ghi")));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("abc.def.ghi"));
            Assert.That(credential.Value.Scheme, Is.EqualTo("Bearer"));
        });
    }

    [Test]
    public void The_scheme_prefix_is_matched_case_insensitively()
        => Assert.That(
            Bridge().Resolve(Context(("authorization", "bEaReR abc")))?.Token,
            Is.EqualTo("abc"));

    [Test]
    public void A_raw_token_with_no_scheme_prefix_is_taken_verbatim()
        => Assert.That(Bridge().Resolve(Context(("authorization", "abc")))?.Token, Is.EqualTo("abc"));

    [Test]
    public void A_bare_scheme_with_no_token_resolves_to_no_credential()
        => Assert.That(Bridge().Resolve(Context(("authorization", "Bearer "))), Is.Null);

    [Test]
    public void A_missing_header_resolves_to_no_credential()
        => Assert.That(Bridge().Resolve(Context()), Is.Null);

    [Test]
    public void A_whitespace_header_resolves_to_no_credential()
        => Assert.That(Bridge().Resolve(Context(("authorization", "   "))), Is.Null);

    [Test]
    public void A_configured_header_name_is_matched_regardless_of_case()
    {
        var bridge = Bridge(new LatticeTelemetryApiGrpcOptions { CredentialHeaderName = "X-Lattice-Token" });

        Assert.That(bridge.Resolve(Context(("x-lattice-token", "Bearer abc")))?.Token, Is.EqualTo("abc"));
    }

    [Test]
    public void An_empty_configured_header_name_disables_the_bridge()
    {
        var bridge = Bridge(new LatticeTelemetryApiGrpcOptions { CredentialHeaderName = string.Empty });

        Assert.That(bridge.Resolve(Context(("authorization", "Bearer abc"))), Is.Null);
    }

    [Test]
    public void An_empty_scheme_stamps_a_credential_with_no_scheme()
    {
        var bridge = Bridge(new LatticeTelemetryApiGrpcOptions { CredentialScheme = string.Empty });

        var credential = bridge.Resolve(Context(("authorization", "abc")));

        Assert.Multiple(() =>
        {
            Assert.That(credential, Is.Not.Null);
            Assert.That(credential!.Value.Token, Is.EqualTo("abc"));
            Assert.That(credential.Value.Scheme, Is.Null);
        });
    }

    [Test]
    public void A_token_that_merely_starts_with_the_scheme_letters_is_not_stripped()
        => Assert.That(
            Bridge().Resolve(Context(("authorization", "Bearertoken")))?.Token,
            Is.EqualTo("Bearertoken"));

    [Test]
    public void The_bridge_rejects_a_null_context()
        => Assert.That(() => Bridge().Resolve(null!), Throws.ArgumentNullException);

    [Test]
    public void The_bridge_rejects_null_options()
        => Assert.That(
            () => new HeaderLatticeTelemetryApiCredentialBridge(null!),
            Throws.ArgumentNullException);
}
