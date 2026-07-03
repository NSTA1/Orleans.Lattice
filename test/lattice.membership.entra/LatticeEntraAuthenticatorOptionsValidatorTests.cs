namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeEntraAuthenticatorOptionsValidator"/>.
/// </summary>
public class LatticeEntraAuthenticatorOptionsValidatorTests
{
    private static LatticeEntraAuthenticatorOptions ValidOptions()
    {
        var options = new LatticeEntraAuthenticatorOptions
        {
            Authority = "https://login.microsoftonline.com/common/v2.0",
        };
        options.TenantIds.Add("11111111-1111-1111-1111-111111111111");
        options.Audiences.Add("api://lattice");
        return options;
    }

    private static bool Validate(LatticeEntraAuthenticatorOptions options) =>
        new LatticeEntraAuthenticatorOptionsValidator().Validate(null, options).Succeeded;

    [Test]
    public void Validate_valid_options_succeeds()
    {
        Assert.That(Validate(ValidOptions()), Is.True);
    }

    [Test]
    public void Validate_missing_authority_fails()
    {
        var options = ValidOptions();
        options.Authority = string.Empty;

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_no_tenant_ids_fails()
    {
        var options = new LatticeEntraAuthenticatorOptions { Authority = "https://login.microsoftonline.com/common/v2.0" };
        options.Audiences.Add("api://lattice");

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_no_audiences_fails()
    {
        var options = new LatticeEntraAuthenticatorOptions { Authority = "https://login.microsoftonline.com/common/v2.0" };
        options.TenantIds.Add("11111111-1111-1111-1111-111111111111");

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_issuer_template_without_placeholder_fails()
    {
        var options = ValidOptions();
        options.IssuerTemplate = "https://login.microsoftonline.com/fixed/v2.0";

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_undefined_group_resolution_mode_fails()
    {
        var options = ValidOptions();
        options.GroupResolutionMode = (EntraGroupResolutionMode)99;

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_non_positive_refresh_interval_fails()
    {
        var options = ValidOptions();
        options.RefreshInterval = TimeSpan.Zero;

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void ValidateAndThrow_invalid_options_throws()
    {
        Assert.That(
            () => LatticeEntraAuthenticatorOptionsValidator.ValidateAndThrow(new LatticeEntraAuthenticatorOptions()),
            Throws.TypeOf<Microsoft.Extensions.Options.OptionsValidationException>());
    }

    [Test]
    public void ResolveMetadataAddress_derives_from_authority_when_unset()
    {
        var options = ValidOptions();

        Assert.That(
            options.ResolveMetadataAddress(),
            Is.EqualTo("https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration"));
    }

    [Test]
    public void ResolveMetadataAddress_prefers_explicit_metadata_address()
    {
        var options = ValidOptions();
        options.MetadataAddress = "https://example/.well-known/openid-configuration";

        Assert.That(options.ResolveMetadataAddress(), Is.EqualTo("https://example/.well-known/openid-configuration"));
    }
}
