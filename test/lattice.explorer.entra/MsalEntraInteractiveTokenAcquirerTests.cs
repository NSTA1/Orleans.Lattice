using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.Options;
using Microsoft.Identity.Client;
using NSubstitute;

namespace Orleans.Lattice.Explorer.Entra.Tests;

/// <summary>
/// Unit tests for <see cref="MsalEntraInteractiveTokenAcquirer"/>, the thin MSAL
/// adapter behind <see cref="IEntraInteractiveTokenAcquirer"/>. MSAL's fluent
/// builders are concrete and cannot be executed offline, so the flow branches are
/// driven by substituting the cached <c>IPublicClientApplication</c> (seeded into
/// the acquirer's private per-(client,authority) cache via reflection), and the
/// private projection/callback helpers are exercised directly with constructed
/// MSAL result types. Everything here is deterministic and needs no network,
/// browser, or Azure dependency. These tests only observe production behaviour;
/// they never mutate production logic.
/// </summary>
[TestFixture]
public class MsalEntraInteractiveTokenAcquirerTests
{
    private const string ClientId = "11111111-1111-1111-1111-111111111111";
    private const string Authority = "https://login.microsoftonline.com/common";

    private static readonly DateTimeOffset Expiry = new(2025, 6, 1, 12, 0, 0, TimeSpan.Zero);

    private static MsalEntraInteractiveTokenAcquirer CreateAcquirer(ExplorerEntraOptions? options = null)
        => new(Options.Create(options ?? new ExplorerEntraOptions()));

    private static EntraTokenRequest Request(bool useDeviceCode = false) => new()
    {
        Authority = Authority,
        ClientId = ClientId,
        Scopes = new[] { "api://state-api/.default" },
        UseDeviceCode = useDeviceCode,
    };

    /// <summary>
    /// Seeds the acquirer's private <c>_apps</c> cache with a substitute keyed by
    /// the exact "{clientId}|{authority}" key the production code composes, so the
    /// next acquisition resolves the substitute instead of building a real MSAL app.
    /// </summary>
    private static void SeedApp(MsalEntraInteractiveTokenAcquirer acquirer, EntraTokenRequest request, IPublicClientApplication app)
    {
        var field = typeof(MsalEntraInteractiveTokenAcquirer)
            .GetField("_apps", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var apps = (ConcurrentDictionary<string, IPublicClientApplication>)field.GetValue(acquirer)!;
        apps[$"{request.ClientId}|{request.Authority}"] = app;
    }

    private static EntraTokenResult InvokeMap(AuthenticationResult result)
    {
        var map = typeof(MsalEntraInteractiveTokenAcquirer)
            .GetMethod("Map", BindingFlags.NonPublic | BindingFlags.Static)!;
        return (EntraTokenResult)map.Invoke(null, new object[] { result })!;
    }

    private static Task InvokeOnDeviceCode(MsalEntraInteractiveTokenAcquirer acquirer, DeviceCodeResult info)
    {
        var onDeviceCode = typeof(MsalEntraInteractiveTokenAcquirer)
            .GetMethod("OnDeviceCode", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)onDeviceCode.Invoke(acquirer, new object[] { info, CancellationToken.None })!;
    }

    private static AuthenticationResult BuildAuthResult(string accessToken, DateTimeOffset expiresOn, IAccount? account)
        => new(
            accessToken: accessToken,
            isExtendedLifeTimeToken: false,
            uniqueId: "unique-id",
            expiresOn: expiresOn,
            extendedExpiresOn: expiresOn,
            tenantId: "tenant-id",
            account: account!,
            idToken: "id-token",
            scopes: Array.Empty<string>(),
            correlationId: Guid.NewGuid(),
            authenticationResultMetadata: new AuthenticationResultMetadata(TokenSource.IdentityProvider),
            tokenType: "Bearer");

    private static DeviceCodeResult BuildDeviceCodeResult(string message)
    {
        var ctor = typeof(DeviceCodeResult).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            new[]
            {
                typeof(string), typeof(string), typeof(string), typeof(DateTimeOffset),
                typeof(long), typeof(string), typeof(string), typeof(ISet<string>),
            },
            modifiers: null)!;
        return (DeviceCodeResult)ctor.Invoke(new object[]
        {
            "USER-CODE", "DEVICE-CODE", "https://microsoft.com/devicelogin",
            DateTimeOffset.UtcNow.AddMinutes(5), 5L, message, ClientId, new HashSet<string>(),
        });
    }

    [Test]
    public void Constructor_nullOptions_throws()
        => Assert.That(
            () => new MsalEntraInteractiveTokenAcquirer(null!),
            Throws.ArgumentNullException);

    [Test]
    public void AcquireInteractiveAsync_nullRequest_throws()
    {
        var acquirer = CreateAcquirer();
        Assert.That(async () => await acquirer.AcquireInteractiveAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AcquireSilentAsync_nullRequest_throws()
    {
        var acquirer = CreateAcquirer();
        Assert.That(async () => await acquirer.AcquireSilentAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task AcquireSilentAsync_noCachedAccount_returnsNull()
    {
        // Builds a real MSAL public-client app (exercising GetOrCreateApp) and reads
        // its empty in-memory account cache - no network is touched.
        var acquirer = CreateAcquirer();

        var result = await acquirer.AcquireSilentAsync(Request());

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task AcquireSilentAsync_cachedAccount_uiRequired_returnsNull()
    {
        var acquirer = CreateAcquirer();
        var request = Request();
        var app = Substitute.For<IPublicClientApplication>();
        var account = Substitute.For<IAccount>();
        app.GetAccountsAsync().Returns(Task.FromResult<IEnumerable<IAccount>>(new[] { account }));
        app.AcquireTokenSilent(Arg.Any<IEnumerable<string>>(), Arg.Any<IAccount>())
            .Returns(_ => throw new MsalUiRequiredException("ui_required", "interaction required"));
        SeedApp(acquirer, request, app);

        var result = await acquirer.AcquireSilentAsync(request);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void AcquireSilentAsync_cachedAccount_nonUiError_propagates()
    {
        var acquirer = CreateAcquirer();
        var request = Request();
        var app = Substitute.For<IPublicClientApplication>();
        var account = Substitute.For<IAccount>();
        app.GetAccountsAsync().Returns(Task.FromResult<IEnumerable<IAccount>>(new[] { account }));
        app.AcquireTokenSilent(Arg.Any<IEnumerable<string>>(), Arg.Any<IAccount>())
            .Returns(_ => throw new MsalClientException("client_error", "boom"));
        SeedApp(acquirer, request, app);

        Assert.That(
            async () => await acquirer.AcquireSilentAsync(request),
            Throws.TypeOf<MsalClientException>());
    }

    [Test]
    public void AcquireInteractiveAsync_interactiveFlow_whenAppThrows_propagates()
    {
        var acquirer = CreateAcquirer();
        var request = Request(useDeviceCode: false);
        var app = Substitute.For<IPublicClientApplication>();
        app.AcquireTokenInteractive(Arg.Any<IEnumerable<string>>())
            .Returns(_ => throw new MsalClientException("interactive_failed", "boom"));
        SeedApp(acquirer, request, app);

        Assert.That(
            async () => await acquirer.AcquireInteractiveAsync(request),
            Throws.TypeOf<MsalClientException>());
    }

    [Test]
    public void AcquireInteractiveAsync_deviceCodeFlow_whenAppThrows_propagates()
    {
        var acquirer = CreateAcquirer();
        var request = Request(useDeviceCode: true);
        var app = Substitute.For<IPublicClientApplication>();
        app.AcquireTokenWithDeviceCode(Arg.Any<IEnumerable<string>>(), Arg.Any<Func<DeviceCodeResult, Task>>())
            .Returns(_ => throw new MsalClientException("device_failed", "boom"));
        SeedApp(acquirer, request, app);

        Assert.That(
            async () => await acquirer.AcquireInteractiveAsync(request),
            Throws.TypeOf<MsalClientException>());
    }

    [Test]
    public void Map_projectsAccessToken_expiry_andUsername()
    {
        var account = Substitute.For<IAccount>();
        account.Username.Returns("mapped@contoso.com");
        var authResult = BuildAuthResult("access-token-value", Expiry, account);

        var result = InvokeMap(authResult);

        Assert.Multiple(() =>
        {
            Assert.That(result.AccessToken, Is.EqualTo("access-token-value"));
            Assert.That(result.ExpiresOn, Is.EqualTo(Expiry));
            Assert.That(result.Username, Is.EqualTo("mapped@contoso.com"));
        });
    }

    [Test]
    public void Map_nullAccount_usesEmptyUsername()
    {
        var authResult = BuildAuthResult("access-token-value", Expiry, account: null);

        var result = InvokeMap(authResult);

        Assert.Multiple(() =>
        {
            Assert.That(result.AccessToken, Is.EqualTo("access-token-value"));
            Assert.That(result.Username, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public async Task OnDeviceCode_noCallback_writesMessageToConsole()
    {
        var acquirer = CreateAcquirer(new ExplorerEntraOptions());
        var info = BuildDeviceCodeResult("device-code-prompt-text");

        var original = Console.Out;
        var captured = new StringWriter();
        Console.SetOut(captured);
        try
        {
            await InvokeOnDeviceCode(acquirer, info);
        }
        finally
        {
            Console.SetOut(original);
        }

        Assert.That(captured.ToString(), Does.Contain("device-code-prompt-text"));
    }

    [Test]
    public async Task OnDeviceCode_withCallback_invokesCallbackWithMessage()
    {
        string? observed = null;
        var options = new ExplorerEntraOptions
        {
            DeviceCodeCallback = (message, _) =>
            {
                observed = message;
                return Task.CompletedTask;
            },
        };
        var acquirer = CreateAcquirer(options);
        var info = BuildDeviceCodeResult("callback-prompt-text");

        await InvokeOnDeviceCode(acquirer, info);

        Assert.That(observed, Is.EqualTo("callback-prompt-text"));
    }
}
