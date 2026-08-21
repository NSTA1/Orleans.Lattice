namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Additional unit tests for <see cref="LatticeEntraAuthenticatorOptionsValidator"/>
/// covering the per-entry allow-list validation, the automatic-refresh and
/// clock-skew bounds, and the non-throwing <c>ValidateAndThrow</c> path that the
/// existing fixture does not exercise.
/// </summary>
public class LatticeEntraAuthenticatorOptionsValidatorCoverageTests
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
    public void Validate_tenant_id_that_is_empty_fails()
    {
        var options = ValidOptions();
        options.TenantIds.Add(string.Empty);

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_tenant_id_that_is_whitespace_fails()
    {
        var options = ValidOptions();
        options.TenantIds.Add("   ");

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_audience_that_is_empty_fails()
    {
        var options = ValidOptions();
        options.Audiences.Add(string.Empty);

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_non_positive_automatic_refresh_interval_fails()
    {
        var options = ValidOptions();
        options.AutomaticRefreshInterval = TimeSpan.Zero;

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_negative_clock_skew_fails()
    {
        var options = ValidOptions();
        options.ClockSkew = TimeSpan.FromMinutes(-1);

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void ValidateAndThrow_valid_options_does_not_throw()
    {
        Assert.That(
            () => LatticeEntraAuthenticatorOptionsValidator.ValidateAndThrow(ValidOptions()),
            Throws.Nothing);
    }
}
