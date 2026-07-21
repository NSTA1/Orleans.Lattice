using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Unit tests for <see cref="ExplorerEntraWebAutoSignInCircuitHandler"/>: the
/// best-effort auto-sign-in drives the session only for an authenticated browser
/// user when the endpoint advertises the <c>entra</c> scheme, and never throws
/// out of <see cref="ExplorerEntraWebAutoSignInCircuitHandler.OnConnectionUpAsync"/>.
/// </summary>
[TestFixture]
public sealed class ExplorerEntraWebAutoSignInCircuitHandlerTests
{
    private static ExplorerAuthSchemeAdvertisement EntraAdvertisement() => new()
    {
        Schemes = new[] { new ExplorerAuthSchemeDescriptor { SchemeId = ExplorerAuthSchemes.Entra } },
    };

    private static ExplorerEntraWebAutoSignInCircuitHandler CreateHandler(
        IExplorerAuthSession session,
        ClaimsPrincipalKind user)
    {
        var principal = user == ClaimsPrincipalKind.Authenticated
            ? FakeAuthenticationStateProvider.Authenticated("alice")
            : FakeAuthenticationStateProvider.Anonymous();

        return new ExplorerEntraWebAutoSignInCircuitHandler(
            session,
            new FakeAuthenticationStateProvider(principal),
            NullLogger<ExplorerEntraWebAutoSignInCircuitHandler>.Instance);
    }

    private static IExplorerAuthSession CreateSession(bool isAuthenticated, ExplorerAuthSchemeAdvertisement advertisement)
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(isAuthenticated);
        session.InitializeAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        session.LoginWithMethodAsync(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string?>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        session.DiscoverAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(advertisement));
        return session;
    }

    [Test]
    public async Task Signs_in_an_authenticated_user_when_entra_is_advertised()
    {
        var session = CreateSession(isAuthenticated: false, EntraAdvertisement());
        var handler = CreateHandler(session, ClaimsPrincipalKind.Authenticated);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        await session.Received(1).LoginWithMethodAsync(
            ExplorerAuthSchemes.Entra,
            Arg.Any<IReadOnlyDictionary<string, string?>?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Does_nothing_for_an_anonymous_user()
    {
        var session = CreateSession(isAuthenticated: false, EntraAdvertisement());
        var handler = CreateHandler(session, ClaimsPrincipalKind.Anonymous);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        await session.DidNotReceive().InitializeAsync(Arg.Any<CancellationToken>());
        await session.DidNotReceive().LoginWithMethodAsync(
            Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string?>?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Does_nothing_when_already_signed_in()
    {
        var session = CreateSession(isAuthenticated: true, EntraAdvertisement());
        var handler = CreateHandler(session, ClaimsPrincipalKind.Authenticated);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        await session.DidNotReceive().DiscoverAsync(Arg.Any<CancellationToken>());
        await session.DidNotReceive().LoginWithMethodAsync(
            Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string?>?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Does_not_sign_in_when_entra_is_not_advertised()
    {
        var session = CreateSession(isAuthenticated: false, ExplorerAuthSchemeAdvertisement.Empty);
        var handler = CreateHandler(session, ClaimsPrincipalKind.Authenticated);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        await session.DidNotReceive().LoginWithMethodAsync(
            Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string?>?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Never_throws_when_the_sign_in_fails()
    {
        var session = CreateSession(isAuthenticated: false, EntraAdvertisement());
        session.LoginWithMethodAsync(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string?>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("boom")));
        var handler = CreateHandler(session, ClaimsPrincipalKind.Authenticated);

        Assert.DoesNotThrowAsync(() => handler.OnConnectionUpAsync(null!, CancellationToken.None));
    }

    internal enum ClaimsPrincipalKind
    {
        Anonymous,
        Authenticated,
    }
}
