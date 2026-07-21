namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure.Tests;

/// <summary>
/// Tests for <see cref="AzureTelemetryBackendTokenOptionsValidator"/>: a
/// credential with the default scope validates, and a missing credential, an
/// empty scope, or a negative refresh skew is rejected.
/// </summary>
[TestFixture]
public sealed class AzureTelemetryBackendTokenOptionsValidatorTests
{
    private static readonly AzureTelemetryBackendTokenOptionsValidator Validator = new();

    private static AzureTelemetryBackendTokenOptions Valid() => new()
    {
        Credential = new FakeTokenCredential(_ => new("t", DateTimeOffset.MaxValue)),
    };

    private static bool IsValid(AzureTelemetryBackendTokenOptions options)
        => Validator.Validate(name: null, options).Succeeded;

    [Test]
    public void A_credential_with_defaults_validates()
        => Assert.That(IsValid(Valid()), Is.True);

    [Test]
    public void A_missing_credential_is_rejected()
    {
        var options = Valid();
        options.Credential = null;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void An_empty_scope_is_rejected()
    {
        var options = Valid();
        options.Scope = "   ";
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void A_negative_refresh_skew_is_rejected()
    {
        var options = Valid();
        options.RefreshSkew = TimeSpan.FromSeconds(-1);
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Validate_rejects_a_null_options_instance()
        => Assert.Throws<ArgumentNullException>(() => Validator.Validate(name: null, options: null!));
}
