using System.Security.Claims;
using Microsoft.Identity.Client;
using Microsoft.Identity.Web;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Unit tests for <see cref="IdentityWebExplorerTokenAcquirer"/>: argument
/// guards, the not-authenticated guard, and translation of Microsoft.Identity's
/// interactive-required signal into <see cref="ExplorerWebReauthRequiredException"/>.
/// The success path is exercised end-to-end through the auth method's fake
/// acquirer; here we cover the branches that need the real ITokenAcquisition seam.
/// </summary>
[TestFixture]
public sealed class IdentityWebExplorerTokenAcquirerTests
{
    private static readonly string[] Scopes = { "api://resource/.default" };

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var tokenAcquisition = Substitute.For<ITokenAcquisition>();
        var stateProvider = new FakeAuthenticationStateProvider(FakeAuthenticationStateProvider.Anonymous());

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new IdentityWebExplorerTokenAcquirer(null!, stateProvider));
            Assert.Throws<ArgumentNullException>(() => new IdentityWebExplorerTokenAcquirer(tokenAcquisition, null!));
        });
    }

    [Test]
    public void AcquireTokenAsync_rejects_empty_scopes()
    {
        var acquirer = new IdentityWebExplorerTokenAcquirer(
            Substitute.For<ITokenAcquisition>(),
            new FakeAuthenticationStateProvider(FakeAuthenticationStateProvider.Authenticated("bob")));

        Assert.ThrowsAsync<ArgumentException>(() => acquirer.AcquireTokenAsync(Array.Empty<string>()));
    }

    [Test]
    public void AcquireTokenAsync_throws_reauth_when_the_browser_is_not_authenticated()
    {
        var tokenAcquisition = Substitute.For<ITokenAcquisition>();
        var acquirer = new IdentityWebExplorerTokenAcquirer(
            tokenAcquisition,
            new FakeAuthenticationStateProvider(FakeAuthenticationStateProvider.Anonymous()));

        Assert.ThrowsAsync<ExplorerWebReauthRequiredException>(() => acquirer.AcquireTokenAsync(Scopes));
    }

    [Test]
    public void AcquireTokenAsync_translates_MsalUiRequired_into_reauth()
    {
        var tokenAcquisition = Substitute.For<ITokenAcquisition>();
        tokenAcquisition
            .GetAuthenticationResultForUserAsync(
                Arg.Any<IEnumerable<string>>(),
                user: Arg.Any<ClaimsPrincipal?>(),
                tokenAcquisitionOptions: Arg.Any<TokenAcquisitionOptions?>())
            .ThrowsAsyncForAnyArgs(new MsalUiRequiredException("code", "interactive required"));

        var acquirer = new IdentityWebExplorerTokenAcquirer(
            tokenAcquisition,
            new FakeAuthenticationStateProvider(FakeAuthenticationStateProvider.Authenticated("bob")));

        var ex = Assert.ThrowsAsync<ExplorerWebReauthRequiredException>(() => acquirer.AcquireTokenAsync(Scopes));
        Assert.That(ex!.InnerException, Is.TypeOf<MsalUiRequiredException>());
    }
}
