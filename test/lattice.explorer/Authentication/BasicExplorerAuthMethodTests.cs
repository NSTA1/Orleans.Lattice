using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class BasicExplorerAuthMethodTests
{
    private static ExplorerAuthChallengeContext Context(string? username, string? password)
        => new()
        {
            SchemeId = ExplorerAuthSchemes.Basic,
            Inputs = new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.UsernameInput] = username,
                [ExplorerAuthSchemes.PasswordInput] = password,
            },
        };

    [Test]
    public void SchemeId_isBasic()
    {
        Assert.That(new BasicExplorerAuthMethod().SchemeId, Is.EqualTo("basic"));
    }

    [Test]
    public void CanHandle_basicOrEmpty_isTrue_otherScheme_isFalse()
    {
        var method = new BasicExplorerAuthMethod();

        Assert.Multiple(() =>
        {
            Assert.That(method.CanHandle("basic"), Is.True);
            Assert.That(method.CanHandle("BASIC"), Is.True);
            Assert.That(method.CanHandle(string.Empty), Is.True, "an undiscovered endpoint stays on Basic");
            Assert.That(method.CanHandle("entra"), Is.False);
        });
    }

    [Test]
    public async Task ChallengeAsync_producesSameBasicHeaderAsBefore()
    {
        var method = new BasicExplorerAuthMethod();

        var signIn = await method.ChallengeAsync(Context("alice", "Password1"));

        var expected = "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("alice:Password1"));
        Assert.Multiple(() =>
        {
            Assert.That(signIn.SchemeId, Is.EqualTo("basic"));
            Assert.That(signIn.DisplayName, Is.EqualTo("alice"));
            Assert.That(signIn.Authentication.Headers!["authorization"], Is.EqualTo(expected));
            Assert.That(signIn.Authentication.HasCredentialProvider, Is.False, "Basic is a static header, not a token provider");
        });
    }

    [Test]
    public void ChallengeAsync_emptyUsername_throwsArgumentException()
    {
        var method = new BasicExplorerAuthMethod();

        Assert.That(async () => await method.ChallengeAsync(Context("  ", "Password1")), Throws.ArgumentException);
    }

    [Test]
    public void ChallengeAsync_nullPassword_throwsArgumentNullException()
    {
        var method = new BasicExplorerAuthMethod();

        Assert.That(async () => await method.ChallengeAsync(Context("alice", null)), Throws.ArgumentNullException);
    }

    [Test]
    public void ChallengeAsync_nullContext_throws()
    {
        var method = new BasicExplorerAuthMethod();

        Assert.That(async () => await method.ChallengeAsync(null!), Throws.ArgumentNullException);
    }
}
