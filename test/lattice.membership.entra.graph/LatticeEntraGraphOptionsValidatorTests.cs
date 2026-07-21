namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeEntraGraphOptionsValidator"/>: the mutually
/// exclusive authentication modes (secret-less credential vs. the confidential-
/// client triple), at-least-one-scope, and non-negative refresh skew rules.
/// </summary>
public class LatticeEntraGraphOptionsValidatorTests
{
    private static LatticeEntraGraphOptions Valid() => new()
    {
        TenantId = "11111111-1111-1111-1111-111111111111",
        ClientId = "22222222-2222-2222-2222-222222222222",
        ClientSecret = "secret",
    };

    private static LatticeEntraGraphOptionsValidator Validator() => new();

    [Test]
    public void Validate_complete_options_succeeds()
    {
        var result = Validator().Validate(null, Valid());

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_credential_without_secret_succeeds()
    {
        var options = new LatticeEntraGraphOptions { Credential = new FakeTokenCredential() };

        var result = Validator().Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_neither_credential_nor_secret_fails()
    {
        var options = new LatticeEntraGraphOptions();

        var result = Validator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_both_credential_and_secret_fails()
    {
        var options = Valid();
        options.Credential = new FakeTokenCredential();

        var result = Validator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_missing_tenant_fails()
    {
        var options = Valid();
        options.TenantId = string.Empty;

        var result = Validator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_missing_client_id_fails()
    {
        var options = Valid();
        options.ClientId = string.Empty;

        var result = Validator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_missing_client_secret_fails()
    {
        var options = Valid();
        options.ClientSecret = string.Empty;

        var result = Validator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_no_scopes_fails()
    {
        var options = Valid();
        options.Scopes.Clear();

        var result = Validator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_negative_refresh_skew_fails()
    {
        var options = Valid();
        options.TokenRefreshSkew = TimeSpan.FromSeconds(-1);

        var result = Validator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }
}
