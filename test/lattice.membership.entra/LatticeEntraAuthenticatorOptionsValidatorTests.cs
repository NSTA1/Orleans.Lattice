namespace Orleans.Lattice.Membership.Entra.Tests;

using Microsoft.Extensions.Options;

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

    /// <summary>
    /// The default pin is non-empty, so the common case reaches none of the
    /// algorithm checks below. Asserted explicitly because every negative test
    /// here clears the collection, and a default that was silently empty would
    /// make all of them pass for the wrong reason.
    /// </summary>
    [Test]
    public void Validate_default_options_pin_RS256()
    {
        var options = ValidOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Algorithms, Is.EqualTo(new[] { LatticeEntraAuthenticatorOptions.DefaultAlgorithm }));
            Assert.That(Validate(options), Is.True);
        });
    }

    /// <summary>
    /// Regression: an empty algorithm pin must fail at startup, naming the option.
    /// The authenticator installs a deny-all validator for this case, which is
    /// correct but silent - it denies every token at runtime with nothing pointing
    /// at the option responsible, so a configuration error presents as a total
    /// authentication outage. Failing here converts that into a startup error.
    /// </summary>
    [Test]
    public void Validate_empty_algorithms_fails()
    {
        var options = ValidOptions();
        options.Algorithms.Clear();

        Assert.That(Validate(options), Is.False);
    }

    /// <summary>
    /// The failure has to name <c>Algorithms</c>, since the whole point of moving
    /// the refusal to startup is that the operator learns which option is wrong.
    /// A bare "options are invalid" would leave them no better off than the silent
    /// runtime deny this replaces.
    /// </summary>
    [Test]
    public void Validate_empty_algorithms_names_the_option_in_its_failure()
    {
        var options = ValidOptions();
        options.Algorithms.Clear();

        var result = new LatticeEntraAuthenticatorOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(
                result.Failures,
                Has.Some.Contains(nameof(LatticeEntraAuthenticatorOptions.Algorithms)),
                "An empty pin must name the option so the operator can act on it.");
        });
    }

    [Test]
    public void Validate_whitespace_algorithm_fails()
    {
        var options = ValidOptions();
        options.Algorithms.Clear();
        options.Algorithms.Add("  ");

        Assert.That(Validate(options), Is.False);
    }

    /// <summary>
    /// The refusal is of an <em>empty</em> pin, not of a non-default one. A host
    /// replacing RS256 with another algorithm set is a supported configuration and
    /// must still validate, or the guard would have narrowed the option instead of
    /// closing the fail-open branch.
    /// </summary>
    [Test]
    public void Validate_repopulated_non_default_algorithms_succeeds()
    {
        var options = ValidOptions();
        options.Algorithms.Clear();
        options.Algorithms.Add("PS256");

        Assert.That(Validate(options), Is.True);
    }

    /// <summary>
    /// ValidateAndThrow is the registration-time entry point, so the empty pin has
    /// to surface through it as well as through Validate - that is the path a host
    /// actually hits at startup.
    /// </summary>
    [Test]
    public void ValidateAndThrow_empty_algorithms_throws()
    {
        var options = ValidOptions();
        options.Algorithms.Clear();

        Assert.That(
            () => LatticeEntraAuthenticatorOptionsValidator.ValidateAndThrow(options),
            Throws.TypeOf<OptionsValidationException>());
    }
}
