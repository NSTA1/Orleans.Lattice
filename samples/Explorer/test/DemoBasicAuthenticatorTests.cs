namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class DemoBasicAuthenticatorTests
{
    private static string Token(string text) => Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(text));

    [TestCase("Basic", true)]
    [TestCase("basic", true)]
    [TestCase("Bearer", false)]
    public void It_handles_the_basic_scheme_only(string scheme, bool expected) =>
        Assert.That(new DemoBasicAuthenticator().CanHandle(new LatticeCredential("token", scheme)), Is.EqualTo(expected));

    [TestCase("alice:any-password", "alice")]
    [TestCase("bob", "bob")]
    [TestCase("carol:with:colons", "carol")]
    public async Task The_username_is_the_subject(string decoded, string subject)
    {
        var principal = await new DemoBasicAuthenticator().AuthenticateAsync(new LatticeCredential(Token(decoded), "Basic"));

        Assert.That(principal?.SubjectId, Is.EqualTo(subject));
        Assert.That(principal?.Issuer, Is.EqualTo(DemoBasicAuthenticator.Issuer));
    }

    [Test]
    public async Task A_malformed_token_resolves_no_principal() =>
        Assert.That(await new DemoBasicAuthenticator().AuthenticateAsync(new LatticeCredential("%%%", "Basic")), Is.Null);

    [Test]
    public async Task An_empty_username_resolves_no_principal() =>
        Assert.That(await new DemoBasicAuthenticator().AuthenticateAsync(new LatticeCredential(Token(":password"), "Basic")), Is.Null);
}
