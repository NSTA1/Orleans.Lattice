namespace Orleans.Lattice.Membership.Oidc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeOidcAuthenticatorOptions"/>: its defaults and
/// its metadata-address derivation.
/// </summary>
public class LatticeOidcAuthenticatorOptionsTests
{
    [Test]
    public void Defaults_are_fail_closed_and_standards_shaped()
    {
        var options = new LatticeOidcAuthenticatorOptions();

        Assert.That(options.Authority, Is.Empty);
        Assert.That(options.MetadataAddress, Is.Null);
        Assert.That(options.Issuer, Is.Empty);
        Assert.That(options.Audiences, Is.Empty);
        Assert.That(options.Algorithms, Is.Empty);
        Assert.That(options.SubjectClaimTypes, Is.EqualTo(new[] { OidcClaimNames.Subject }));
        Assert.That(
            options.GroupClaimTypes,
            Is.EqualTo(new[] { OidcClaimNames.Groups, OidcClaimNames.Roles, OidcClaimNames.Role }));
        Assert.That(options.SchemeHint, Is.Null);
        Assert.That(options.ValidateLifetime, Is.True);
        Assert.That(options.ClockSkew, Is.EqualTo(TimeSpan.FromMinutes(5)));
        Assert.That(options.AutomaticRefreshInterval, Is.EqualTo(TimeSpan.FromHours(12)));
        Assert.That(options.RefreshInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void Settable_properties_round_trip()
    {
        var options = new LatticeOidcAuthenticatorOptions
        {
            Authority = "https://idp.example.com",
            MetadataAddress = "https://idp.example.com/metadata",
            Issuer = "https://idp.example.com/",
            SchemeHint = "okta",
            ValidateLifetime = false,
            ClockSkew = TimeSpan.FromSeconds(30),
            AutomaticRefreshInterval = TimeSpan.FromHours(1),
            RefreshInterval = TimeSpan.FromMinutes(1),
        };

        Assert.That(options.Authority, Is.EqualTo("https://idp.example.com"));
        Assert.That(options.MetadataAddress, Is.EqualTo("https://idp.example.com/metadata"));
        Assert.That(options.Issuer, Is.EqualTo("https://idp.example.com/"));
        Assert.That(options.SchemeHint, Is.EqualTo("okta"));
        Assert.That(options.ValidateLifetime, Is.False);
        Assert.That(options.ClockSkew, Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(options.AutomaticRefreshInterval, Is.EqualTo(TimeSpan.FromHours(1)));
        Assert.That(options.RefreshInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void ResolveMetadataAddress_derives_from_authority()
    {
        var options = new LatticeOidcAuthenticatorOptions { Authority = "https://idp.example.com/oauth2/default" };

        Assert.That(
            options.ResolveMetadataAddress(),
            Is.EqualTo("https://idp.example.com/oauth2/default/.well-known/openid-configuration"));
    }

    [Test]
    public void ResolveMetadataAddress_trims_a_trailing_authority_slash()
    {
        var options = new LatticeOidcAuthenticatorOptions { Authority = "https://idp.example.com/oauth2/default/" };

        Assert.That(
            options.ResolveMetadataAddress(),
            Is.EqualTo("https://idp.example.com/oauth2/default/.well-known/openid-configuration"));
    }

    [Test]
    public void ResolveMetadataAddress_prefers_an_explicit_metadata_address()
    {
        var options = new LatticeOidcAuthenticatorOptions
        {
            Authority = "https://idp.example.com/oauth2/default",
            MetadataAddress = "https://idp.example.com/elsewhere/config",
        };

        Assert.That(options.ResolveMetadataAddress(), Is.EqualTo("https://idp.example.com/elsewhere/config"));
    }

    [Test]
    public void ResolveMetadataAddress_without_authority_or_metadata_address_is_empty()
    {
        Assert.That(new LatticeOidcAuthenticatorOptions().ResolveMetadataAddress(), Is.Empty);
    }

    [Test]
    public void ResolveMetadataAddress_ignores_a_whitespace_metadata_address()
    {
        var options = new LatticeOidcAuthenticatorOptions
        {
            Authority = "https://idp.example.com",
            MetadataAddress = "   ",
        };

        Assert.That(
            options.ResolveMetadataAddress(),
            Is.EqualTo("https://idp.example.com/.well-known/openid-configuration"));
    }
}
