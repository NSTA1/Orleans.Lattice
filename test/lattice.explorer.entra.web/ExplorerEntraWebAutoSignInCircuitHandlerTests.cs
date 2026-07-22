using System.Collections.Concurrent;
using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;

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
        ClaimsPrincipalKind user,
        ILogger<ExplorerEntraWebAutoSignInCircuitHandler>? logger = null,
        IExplorerSession? explorerSession = null)
    {
        var principal = user == ClaimsPrincipalKind.Authenticated
            ? FakeAuthenticationStateProvider.Authenticated("alice")
            : FakeAuthenticationStateProvider.Anonymous();

        return new ExplorerEntraWebAutoSignInCircuitHandler(
            explorerSession ?? Substitute.For<IExplorerSession>(),
            session,
            new FakeAuthenticationStateProvider(principal),
            logger ?? NullLogger<ExplorerEntraWebAutoSignInCircuitHandler>.Instance);
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
    public async Task Initializes_the_explorer_session_before_discovering_the_scheme()
    {
        // Regression: the endpoint configuration (IExplorerSession.Current) is
        // seeded by IExplorerSession.InitializeAsync, which the ConfigurationGate
        // component only runs on render - after OnConnectionUpAsync. The handler
        // must initialize the explorer session itself, otherwise DiscoverAsync sees
        // a null Current and wrongly reports no advertised scheme, leaving the
        // console anonymous.
        var explorerSession = Substitute.For<IExplorerSession>();
        var session = CreateSession(isAuthenticated: false, EntraAdvertisement());
        var handler = CreateHandler(session, ClaimsPrincipalKind.Authenticated, explorerSession: explorerSession);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        Received.InOrder(() =>
        {
            explorerSession.InitializeAsync(Arg.Any<CancellationToken>());
            session.DiscoverAsync(Arg.Any<CancellationToken>());
        });
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

    [Test]
    public async Task Logs_a_warning_when_the_circuit_is_anonymous()
    {
        var session = CreateSession(isAuthenticated: false, EntraAdvertisement());
        var logger = new CapturingLogger();
        var handler = CreateHandler(session, ClaimsPrincipalKind.Anonymous, logger);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        Assert.That(
            logger.Entries.Any(e => e.Level == LogLevel.Warning && e.Message.Contains("anonymous", StringComparison.OrdinalIgnoreCase)),
            Is.True,
            "the silent anonymous-circuit early-return must be surfaced in the log");
    }

    [Test]
    public async Task Logs_information_on_a_successful_automatic_sign_in()
    {
        var session = CreateSession(isAuthenticated: false, EntraAdvertisement());
        var logger = new CapturingLogger();
        var handler = CreateHandler(session, ClaimsPrincipalKind.Authenticated, logger);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries.Any(e => e.Level == LogLevel.Information), Is.True);
            Assert.That(logger.Entries.Any(e => e.Level == LogLevel.Warning), Is.False);
        });
    }

    [Test]
    public async Task Logs_a_warning_when_the_sign_in_fails()
    {
        var session = CreateSession(isAuthenticated: false, EntraAdvertisement());
        session.LoginWithMethodAsync(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string?>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("boom")));
        var logger = new CapturingLogger();
        var handler = CreateHandler(session, ClaimsPrincipalKind.Authenticated, logger);

        await handler.OnConnectionUpAsync(null!, CancellationToken.None);

        Assert.That(
            logger.Entries.Any(e => e.Level == LogLevel.Warning && e.Exception is InvalidOperationException),
            Is.True);
    }

    internal enum ClaimsPrincipalKind
    {
        Anonymous,
        Authenticated,
    }

    private sealed class CapturingLogger : ILogger<ExplorerEntraWebAutoSignInCircuitHandler>
    {
        public ConcurrentQueue<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Enqueue((logLevel, formatter(state, exception), exception));

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
