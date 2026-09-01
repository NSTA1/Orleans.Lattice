using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Oidc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeOidcAuthenticatorOptionsValidator"/>: every
/// rule it enforces, and the fail-fast <c>ValidateAndThrow</c> registration path.
/// </summary>
public class LatticeOidcAuthenticatorOptionsValidatorTests
{
    private static LatticeOidcAuthenticatorOptions Valid(Action<LatticeOidcAuthenticatorOptions>? configure = null)
    {
        var options = new LatticeOidcAuthenticatorOptions
        {
            Authority = "https://idp.example.com/oauth2/default",
            Issuer = "https://idp.example.com/oauth2/default",
        };
        options.Audiences.Add("api://lattice");
        configure?.Invoke(options);
        return options;
    }

    private static ValidateOptionsResult Validate(LatticeOidcAuthenticatorOptions options) =>
        new LatticeOidcAuthenticatorOptionsValidator().Validate(Options.DefaultName, options);

    [Test]
    public void Validate_valid_options_succeeds()
    {
        Assert.That(Validate(Valid()).Succeeded, Is.True);
    }

    [Test]
    public void Validate_null_options_throws()
    {
        Assert.That(
            () => new LatticeOidcAuthenticatorOptionsValidator().Validate(Options.DefaultName, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Validate_missing_authority_fails()
    {
        var result = Validate(Valid(o => o.Authority = "   "));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("Authority"));
    }

    [Test]
    public void Validate_missing_issuer_fails()
    {
        var result = Validate(Valid(o => o.Issuer = string.Empty));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("Issuer"));
    }

    [Test]
    public void Validate_no_audiences_fails()
    {
        var options = new LatticeOidcAuthenticatorOptions
        {
            Authority = "https://idp.example.com",
            Issuer = "https://idp.example.com",
        };

        var result = Validate(options);

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("at least one audience"));
    }

    [Test]
    public void Validate_blank_audience_fails()
    {
        var result = Validate(Valid(o => o.Audiences.Add("  ")));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("null or empty audience"));
    }

    [Test]
    public void Validate_no_subject_claim_types_fails()
    {
        var result = Validate(Valid(o => o.SubjectClaimTypes.Clear()));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("SubjectClaimTypes"));
    }

    [Test]
    public void Validate_blank_subject_claim_type_fails()
    {
        var result = Validate(Valid(o => o.SubjectClaimTypes.Add(" ")));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("SubjectClaimTypes"));
    }

    [Test]
    public void Validate_blank_group_claim_type_fails()
    {
        var result = Validate(Valid(o => o.GroupClaimTypes.Add(string.Empty)));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("GroupClaimTypes"));
    }

    [Test]
    public void Validate_cleared_group_claim_types_succeeds()
    {
        // An empty group claim list is a legitimate configuration: it disables
        // token-asserted groups entirely.
        Assert.That(Validate(Valid(o => o.GroupClaimTypes.Clear())).Succeeded, Is.True);
    }

    [Test]
    public void Validate_blank_algorithm_fails()
    {
        var result = Validate(Valid(o => o.Algorithms.Add(" ")));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("Algorithms"));
    }

    [Test]
    public void Validate_empty_algorithms_succeeds()
    {
        // Empty means "pin from the discovery document", not "accept anything".
        Assert.That(Validate(Valid()).Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_non_positive_automatic_refresh_interval_fails(int minutes)
    {
        var result = Validate(Valid(o => o.AutomaticRefreshInterval = TimeSpan.FromMinutes(minutes)));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("AutomaticRefreshInterval"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_non_positive_refresh_interval_fails(int minutes)
    {
        var result = Validate(Valid(o => o.RefreshInterval = TimeSpan.FromMinutes(minutes)));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("RefreshInterval"));
    }

    [Test]
    public void Validate_negative_clock_skew_fails()
    {
        var result = Validate(Valid(o => o.ClockSkew = TimeSpan.FromSeconds(-1)));

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures, Has.Some.Contains("ClockSkew"));
    }

    [Test]
    public void Validate_reports_every_failure_at_once()
    {
        var options = new LatticeOidcAuthenticatorOptions { RefreshInterval = TimeSpan.Zero };

        var result = Validate(options);

        Assert.That(result.Failed, Is.True);
        Assert.That(result.Failures!.Count(), Is.GreaterThanOrEqualTo(4));
    }

    [Test]
    public void ValidateAndThrow_valid_options_does_not_throw()
    {
        Assert.That(() => LatticeOidcAuthenticatorOptionsValidator.ValidateAndThrow(Valid()), Throws.Nothing);
    }

    [Test]
    public void ValidateAndThrow_invalid_options_throws()
    {
        Assert.That(
            () => LatticeOidcAuthenticatorOptionsValidator.ValidateAndThrow(Valid(o => o.Issuer = string.Empty)),
            Throws.TypeOf<OptionsValidationException>());
    }
}
