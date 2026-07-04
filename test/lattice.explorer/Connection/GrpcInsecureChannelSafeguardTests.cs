using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// Regression coverage for the insecure-channel safeguard on the Explorer's
/// gRPC state client. gRPC refuses to send per-call credentials over a channel
/// it cannot confirm is secure; that safeguard
/// (<see cref="Grpc.Net.Client.GrpcChannelOptions.UnsafeUseInsecureChannelCallCredentials"/>)
/// must only be lifted for a genuinely plaintext endpoint when the operator has
/// explicitly opted into unencrypted transport. For an <c>https</c> address the
/// safeguard must stay active even though credentials are attached, so a
/// credentialed connection to a TLS endpoint never silently downgrades.
/// </summary>
[TestFixture]
public sealed class GrpcInsecureChannelSafeguardTests
{
    private static LatticeConnectionSettings SettingsFor(string address, bool allowUnencrypted) =>
        new()
        {
            Address = address,
            AllowUnencryptedHttp2 = allowUnencrypted,
            Authentication = LatticeCallAuthentication.Bearer(new FakeCredentialProvider()),
        };

    [Test]
    public void BuildChannelOptions_for_an_https_address_leaves_the_insecure_safeguard_active()
    {
        var settings = SettingsFor("https://lattice.example:443", allowUnencrypted: true);

        var options = GrpcLatticeStateClient.BuildChannelOptions(settings);

        // Even with the plaintext opt-in set, an https endpoint must keep the
        // safeguard: credentials still attach over the confirmed-secure TLS
        // channel, but the flag stays false so gRPC never treats it as insecure.
        Assert.That(options.UnsafeUseInsecureChannelCallCredentials, Is.False);
    }

    [Test]
    public void BuildChannelOptions_for_an_http_address_without_the_opt_in_leaves_the_safeguard_active()
    {
        var settings = SettingsFor("http://lattice.example:5199", allowUnencrypted: false);

        var options = GrpcLatticeStateClient.BuildChannelOptions(settings);

        // A plaintext endpoint without the explicit operator opt-in must not
        // lift the safeguard.
        Assert.That(options.UnsafeUseInsecureChannelCallCredentials, Is.False);
    }

    [Test]
    public void BuildChannelOptions_for_an_http_address_with_the_opt_in_lifts_the_safeguard()
    {
        var settings = SettingsFor("http://lattice.example:5199", allowUnencrypted: true);

        var options = GrpcLatticeStateClient.BuildChannelOptions(settings);

        // Only a genuinely plaintext endpoint with the explicit opt-in lifts the
        // safeguard so credentials can flow over h2c.
        Assert.That(options.UnsafeUseInsecureChannelCallCredentials, Is.True);
    }

    [Test]
    public void BuildChannelOptions_without_credentials_never_lifts_the_safeguard()
    {
        var settings = new LatticeConnectionSettings
        {
            Address = "http://lattice.example:5199",
            AllowUnencryptedHttp2 = true,
            Authentication = null,
        };

        var options = GrpcLatticeStateClient.BuildChannelOptions(settings);

        // With no per-call credentials there is nothing to protect, so the
        // insecure-call-credentials flag is irrelevant and stays false.
        Assert.That(options.UnsafeUseInsecureChannelCallCredentials, Is.False);
    }
}
