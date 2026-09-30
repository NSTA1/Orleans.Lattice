using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Regression tests for the two ways a signed-out <see cref="CookieCredentialStore"/>
/// credential could be brought back to life.
/// <para>
/// The revocation ledger is process-local and bounded at 1024 entries, and
/// <c>/auth/logout</c> is guarded by antiforgery alone - which is not authentication,
/// since any visitor can fetch the page and harvest a token pair. An anonymous caller
/// that could enqueue fabricated values would evict the operator's genuine revocation
/// and resurrect the credential, so only a value this store actually protected may
/// consume a slot.
/// </para>
/// <para>
/// The ledger also cannot be the whole endpoint binding: a restart, a second web-head
/// replica, or an eviction each loses it, and the auth session's own endpoint check
/// only runs for a circuit that is already signed in. The binding is therefore stamped
/// into the payload, and a read for a different endpoint fails closed.
/// </para>
/// </summary>
[TestFixture]
public sealed class CookieCredentialStoreSecurityTests
{
    private const string CookieName = "lattice-explorer-cred";
    private const string Endpoint = "https://silo.example:30000";

    [Test]
    public async Task Unprotectable_values_presented_to_ClearAsync_cannot_flush_the_revocation_ledger()
    {
        // The attack: sign the operator out, then post 1024+ junk cookie values at the
        // antiforgery-only logout endpoint. Every junk value that takes a ledger slot
        // pushes the genuine revocation one step closer to eviction; once it is gone,
        // the browser's surviving cookie is honoured again and the sign-out is undone.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider());

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(writeContext);

        var circuit = CircuitPresenting(payload);
        accessor.HttpContext.Returns(circuit);
        await store.ClearAsync();

        // Comfortably past MaxRevocations (1024), so a ledger that admitted these
        // would certainly have evicted the revocation above.
        for (var i = 0; i < 1200; i++)
        {
            var junk = CircuitPresenting($"not-a-protected-payload-{i}");
            accessor.HttpContext.Returns(junk);
            await store.ClearAsync();
        }

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={payload}";
        accessor.HttpContext.Returns(replay);

        Assert.That(
            await store.GetAsync(),
            Is.Null,
            "A caller that cannot mint a payload must not be able to evict a genuine revocation.");
    }

    [Test]
    public async Task A_credential_minted_for_another_endpoint_is_refused()
    {
        // The surviving-cookie replay the ledger cannot cover: a fresh process, or a
        // second replica, has no record of the sign-out, and ExplorerAuthSession
        // challenges with whatever the store hands it before any endpoint is compared.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var config = ConfigStore(Endpoint);
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider(), config);

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(writeContext);

        // The console is repointed. Whoever now answers at the new address must not be
        // handed the password minted for the old one.
        config.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ExplorerConfiguration?>(
                new ExplorerConfiguration { Endpoint = "https://attacker.example:30000" }));

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={payload}";
        accessor.HttpContext.Returns(replay);

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task A_credential_minted_for_the_current_endpoint_is_still_served()
    {
        var accessor = Substitute.For<IHttpContextAccessor>();
        var store = new CookieCredentialStore(
            accessor, new EphemeralDataProtectionProvider(), ConfigStore(Endpoint));

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={PayloadFrom(writeContext)}";
        accessor.HttpContext.Returns(replay);

        var credential = await store.GetAsync();

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Password, Is.EqualTo("hunter2"));
    }

    [Test]
    public async Task An_endpoint_that_differs_only_by_a_trailing_slash_or_case_is_the_same_endpoint()
    {
        // Matching the auth session's own comparison, so a cosmetic rewrite of the
        // configured address does not sign every operator out.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var config = ConfigStore("https://Silo.Example:30000/");
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider(), config);

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(writeContext);

        config.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ExplorerConfiguration?>(
                new ExplorerConfiguration { Endpoint = Endpoint }));

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={payload}";
        accessor.HttpContext.Returns(replay);

        Assert.That(await store.GetAsync(), Is.Not.Null);
    }

    [Test]
    public async Task An_unresolvable_endpoint_refuses_the_credential()
    {
        // Fail closed. An unreadable configuration document names no endpoint, and an
        // endpoint that cannot be resolved cannot be shown to be the right one.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var config = ConfigStore(Endpoint);
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider(), config);

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(writeContext);

        config.LoadAsync(Arg.Any<CancellationToken>())
            .Returns<Task<ExplorerConfiguration?>>(_ => throw new IOException("config unreadable"));

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={payload}";
        accessor.HttpContext.Returns(replay);

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task A_legacy_unbound_payload_is_refused_by_a_store_that_binds()
    {
        // A cookie written before the binding existed carries no endpoint, so a host
        // that binds has nothing to verify it against and refuses rather than replaying
        // it. The operator signs in again and the replacement cookie is bound.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var protectionProvider = new EphemeralDataProtectionProvider();

        var legacyStore = new CookieCredentialStore(accessor, protectionProvider);
        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await legacyStore.SetAsync(new StoredCredential("alice", "hunter2"));
        var legacyPayload = PayloadFrom(writeContext);

        var boundStore = new CookieCredentialStore(accessor, protectionProvider, ConfigStore(Endpoint));
        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={legacyPayload}";
        accessor.HttpContext.Returns(replay);

        Assert.That(await boundStore.GetAsync(), Is.Null);
    }

    [Test]
    public async Task An_endpoint_named_only_by_the_first_run_seed_still_round_trips()
    {
        // A launcher-configured head names its endpoint by environment variable and
        // never writes the configuration document, so the persisted store legitimately
        // loads null. Resolving from the store alone would name no endpoint there,
        // stamp nothing on write, and then fail closed on read - refusing a credential
        // the very same head had just minted, and breaking sign-in outright. The
        // fallback mirrors ExplorerSession.InitializeAsync, which loads in this order.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var config = Substitute.For<IExplorerConfigStore>();
        config.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ExplorerConfiguration?>(null));

        var seed = Substitute.For<IExplorerConfigurationSeed>();
        seed.TrySeed().Returns(new ExplorerConfiguration { Endpoint = Endpoint });

        var store = new CookieCredentialStore(
            accessor, new EphemeralDataProtectionProvider(), config, seed);

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={PayloadFrom(writeContext)}";
        accessor.HttpContext.Returns(replay);

        var credential = await store.GetAsync();

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Password, Is.EqualTo("hunter2"));
    }

    [Test]
    public async Task The_persisted_configuration_wins_over_the_first_run_seed()
    {
        // The seed is a first-run fallback only: once an endpoint is persisted, the
        // binding must follow the document, or repointing the console through the cog
        // would leave the credential bound to whatever the launcher once seeded.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var config = ConfigStore(Endpoint);
        var seed = Substitute.For<IExplorerConfigurationSeed>();
        seed.TrySeed().Returns(new ExplorerConfiguration { Endpoint = "https://stale.example:30000" });

        var store = new CookieCredentialStore(
            accessor, new EphemeralDataProtectionProvider(), config, seed);

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(writeContext);

        // Only the seed now names the old address; the document names the new one.
        config.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ExplorerConfiguration?>(
                new ExplorerConfiguration { Endpoint = "https://elsewhere.example:30000" }));

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={payload}";
        accessor.HttpContext.Returns(replay);

        Assert.That(await store.GetAsync(), Is.Null);
    }

    private static IExplorerConfigStore ConfigStore(string endpoint)
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ExplorerConfiguration?>(
                new ExplorerConfiguration { Endpoint = endpoint }));
        return store;
    }

    private static string PayloadFrom(HttpContext writeContext)
    {
        var setCookie = writeContext.Response.Headers.SetCookie.ToString();
        return setCookie.Split(';')[0][(CookieName.Length + 1)..];
    }

    /// <summary>
    /// A Blazor circuit context: the response headers are already sent, and the
    /// browser is presenting <paramref name="payload"/> as its credential cookie.
    /// </summary>
    private static HttpContext CircuitPresenting(string payload)
    {
        var jar = Substitute.For<IRequestCookieCollection>();
        jar[CookieName].Returns(payload);
        var request = Substitute.For<HttpRequest>();
        request.Cookies.Returns(jar);
        var response = Substitute.For<HttpResponse>();
        response.HasStarted.Returns(true);
        var context = Substitute.For<HttpContext>();
        context.Request.Returns(request);
        context.Response.Returns(response);
        return context;
    }
}
