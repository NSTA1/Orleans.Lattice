using Grpc.Core;
using Grpc.Net.Client;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// Regression coverage for the transport gate on static sign-in credentials.
/// <para>
/// A static credential (the <c>authorization: Basic base64(user:password)</c>
/// header a non-interactive sign-in produces) is attached through an ordinary
/// metadata interceptor rather than through <see cref="CallCredentials"/>.
/// gRPC's own insecure-channel safeguard - the one
/// <see cref="GrpcChannelOptions.UnsafeUseInsecureChannelCallCredentials"/>
/// lifts - only ever applies to call credentials, so it never saw that path.
/// The result was that the <em>most</em> sensitive credential the Explorer can
/// hold (a long-lived username and password, recoverable by anyone who can
/// base64-decode) crossed a plaintext <c>http</c> h2c link with no operator
/// opt-in at all, while the strictly less sensitive short-lived refreshable
/// bearer token was correctly gated behind
/// <see cref="LatticeConnectionSettings.AllowUnencryptedHttp2"/>.
/// </para>
/// <para>
/// The gate mirrors the replication transport's scheme gate: refuse outright,
/// with operator guidance, unless the endpoint is <c>https</c> or the operator
/// explicitly accepted an unencrypted transport.
/// </para>
/// </summary>
[TestFixture]
public sealed class StaticCredentialTransportGateTests
{
    private static LatticeConnectionSettings Settings(
        string address,
        bool allowUnencrypted,
        LatticeCallAuthentication? authentication) =>
        new()
        {
            Address = address,
            AllowUnencryptedHttp2 = allowUnencrypted,
            Authentication = authentication,
        };

    private static CallInvoker CreateInvoker(LatticeConnectionSettings settings)
    {
        var options = LatticeGrpcChannelFactory.BuildChannelOptions(settings);
        var channel = GrpcChannel.ForAddress(settings.Address, options);
        return LatticeGrpcChannelFactory.CreateCallInvoker(channel, settings);
    }

    [Test]
    public void Basic_credentials_are_refused_over_a_plaintext_endpoint_without_the_opt_in()
    {
        var settings = Settings(
            "http://lattice.example:5199",
            allowUnencrypted: false,
            LatticeCallAuthentication.Basic("operator", "correct-horse-battery-staple"));

        var ex = Assert.Throws<InvalidOperationException>(() => CreateInvoker(settings));

        Assert.Multiple(() =>
        {
            // The refusal must name the endpoint and point at the explicit
            // opt-in, so an operator hitting it in local development knows both
            // what was refused and how to accept the risk deliberately.
            Assert.That(ex!.Message, Does.Contain("http://lattice.example:5199"));
            Assert.That(ex.Message, Does.Contain(nameof(LatticeConnectionSettings.AllowUnencryptedHttp2)));
            // The credential itself must never appear in the diagnostic.
            Assert.That(ex.Message, Does.Not.Contain("correct-horse-battery-staple"));
        });
    }

    [Test]
    public void Basic_credentials_are_allowed_over_a_plaintext_endpoint_with_the_explicit_opt_in()
    {
        var settings = Settings(
            "http://lattice.example:5199",
            allowUnencrypted: true,
            LatticeCallAuthentication.Basic("operator", "s3cret"));

        Assert.That(CreateInvoker(settings), Is.Not.Null);
    }

    [Test]
    public void Basic_credentials_are_allowed_over_an_https_endpoint()
    {
        var settings = Settings(
            "https://lattice.example:443",
            allowUnencrypted: false,
            LatticeCallAuthentication.Basic("operator", "s3cret"));

        Assert.That(CreateInvoker(settings), Is.Not.Null);
    }

    [Test]
    public void An_anonymous_connection_over_a_plaintext_endpoint_is_unaffected()
    {
        // There is no credential to protect, so the gate must not turn an
        // ordinary local-development connection into a hard failure.
        var settings = Settings("http://lattice.example:5199", allowUnencrypted: false, authentication: null);

        Assert.That(CreateInvoker(settings), Is.Not.Null);
    }

    [Test]
    public void Non_credential_sign_in_headers_over_a_plaintext_endpoint_are_unaffected()
    {
        // Only the standard authentication headers carry a secret. Routing
        // metadata must keep working over a plaintext development endpoint.
        var settings = Settings(
            "http://lattice.example:5199",
            allowUnencrypted: false,
            new LatticeCallAuthentication
            {
                Headers = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["x-lattice-region"] = "westeurope",
                },
            });

        Assert.That(CreateInvoker(settings), Is.Not.Null);
    }
}
