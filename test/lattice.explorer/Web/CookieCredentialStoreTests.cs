using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Unit tests for <see cref="CookieCredentialStore"/>. The store writes and clears
/// the credential through the response cookie collection, which is only writable
/// while the response headers are unsent. The auto-sign-in circuit handler drives a
/// token sign-in from <c>OnConnectionUpAsync</c>, where <see cref="IHttpContextAccessor"/>
/// returns the long-lived SignalR request whose response has already started;
/// mutating the cookie there must be a safe no-op rather than throwing
/// "Headers are read-only, response has already started" (which aborted the whole
/// automatic Entra sign-in and left the console anonymous).
/// </summary>
[TestFixture]
public class CookieCredentialStoreTests
{
    private const string CookieName = "lattice-explorer-cred";

    private static CookieCredentialStore CreateStore(HttpContext? context, out IResponseCookies cookies)
    {
        cookies = Substitute.For<IResponseCookies>();
        if (context is not null)
        {
            context.Response.Cookies.Returns(cookies);
        }

        var accessor = Substitute.For<IHttpContextAccessor>();
        accessor.HttpContext.Returns(context);
        return new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider());
    }

    private static HttpContext ContextWithStartedResponse(bool hasStarted)
    {
        var response = Substitute.For<HttpResponse>();
        response.HasStarted.Returns(hasStarted);
        var context = Substitute.For<HttpContext>();
        context.Response.Returns(response);
        return context;
    }

    [Test]
    public async Task ClearAsync_when_response_has_started_does_not_throw_and_leaves_cookie_untouched()
    {
        var context = ContextWithStartedResponse(hasStarted: true);
        var store = CreateStore(context, out var cookies);

        await store.ClearAsync();

        cookies.DidNotReceive().Delete(Arg.Any<string>());
        cookies.DidNotReceive().Delete(Arg.Any<string>(), Arg.Any<CookieOptions>());
    }

    [Test]
    public async Task ClearAsync_when_response_has_started_still_revokes_the_presented_credential()
    {
        // The delete header cannot be written on a circuit, so the browser keeps
        // presenting the cookie. ExplorerAuthSession clears the credential when the
        // console is repointed at a different endpoint precisely so the next launch
        // cannot replay it there; if the clear were a pure no-op the surviving cookie
        // would hand that endpoint's Basic password to whoever now answers at the new
        // address. The revocation, not the header, is what makes the sign-out hold.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider());

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var payload = PayloadFrom(writeContext);

        // The circuit: the accessor resolves the long-lived SignalR request, whose
        // response headers are already sent.
        var circuit = CircuitPresenting(payload);
        accessor.HttpContext.Returns(circuit);

        await store.ClearAsync();

        // A later request still presents the cookie - the delete never reached the
        // browser - and the store must refuse it anyway.
        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={payload}";
        accessor.HttpContext.Returns(replay);

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task GetAsync_still_serves_a_credential_that_was_never_cleared()
    {
        // The revocation ledger must not swallow an unrelated browser's cookie.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider());

        var writeContext = new DefaultHttpContext();
        accessor.HttpContext.Returns(writeContext);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var kept = PayloadFrom(writeContext);

        var otherWrite = new DefaultHttpContext();
        accessor.HttpContext.Returns(otherWrite);
        await store.SetAsync(new StoredCredential("bob", "correcthorse"));
        var revoked = PayloadFrom(otherWrite);

        var circuit = CircuitPresenting(revoked);
        accessor.HttpContext.Returns(circuit);
        await store.ClearAsync();

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={kept}";
        accessor.HttpContext.Returns(replay);

        var credential = await store.GetAsync();

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Username, Is.EqualTo("alice"));
    }

    [Test]
    public async Task SetAsync_after_a_revoked_clear_restores_a_usable_credential()
    {
        // Signing back in must work: the new cookie is a fresh protected payload, so
        // it is a different value and the revocation of the old one does not reach it.
        var accessor = Substitute.For<IHttpContextAccessor>();
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider());

        var firstWrite = new DefaultHttpContext();
        accessor.HttpContext.Returns(firstWrite);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var first = PayloadFrom(firstWrite);

        var circuit = CircuitPresenting(first);
        accessor.HttpContext.Returns(circuit);
        await store.ClearAsync();

        var secondWrite = new DefaultHttpContext();
        accessor.HttpContext.Returns(secondWrite);
        await store.SetAsync(new StoredCredential("alice", "hunter2"));
        var second = PayloadFrom(secondWrite);

        var replay = new DefaultHttpContext();
        replay.Request.Headers.Cookie = $"{CookieName}={second}";
        accessor.HttpContext.Returns(replay);

        var credential = await store.GetAsync();

        Assert.That(credential, Is.Not.Null);
        Assert.That(credential!.Password, Is.EqualTo("hunter2"));
    }

    private static string PayloadFrom(HttpContext writeContext)
    {
        var setCookie = writeContext.Response.Headers.SetCookie.ToString();
        return setCookie.Split(';')[0][(CookieName.Length + 1)..];
    }

    /// <summary>
    /// A Blazor circuit context: the response headers are already sent, and the
    /// browser is still presenting <paramref name="payload"/> as its credential cookie.
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

    [Test]
    public async Task ClearAsync_with_writable_response_deletes_the_cookie()
    {
        var context = ContextWithStartedResponse(hasStarted: false);
        var store = CreateStore(context, out var cookies);

        await store.ClearAsync();

        cookies.Received(1).Delete(CookieName);
    }

    [Test]
    public async Task ClearAsync_without_a_context_is_a_noop()
    {
        var store = CreateStore(context: null, out _);

        Assert.That(async () => await store.ClearAsync(), Throws.Nothing);
    }

    [Test]
    public async Task SetAsync_when_response_has_started_does_not_throw_and_leaves_cookie_untouched()
    {
        var context = ContextWithStartedResponse(hasStarted: true);
        var store = CreateStore(context, out var cookies);

        await store.SetAsync(new StoredCredential("user", "secret"));

        cookies.DidNotReceive().Append(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CookieOptions>());
    }

    [Test]
    public async Task SetAsync_with_writable_response_appends_the_cookie()
    {
        var context = ContextWithStartedResponse(hasStarted: false);
        var store = CreateStore(context, out var cookies);

        await store.SetAsync(new StoredCredential("user", "secret"));

        cookies.Received(1).Append(CookieName, Arg.Any<string>(), Arg.Any<CookieOptions>());
    }

    [Test]
    public void SetAsync_without_a_context_throws()
    {
        var store = CreateStore(context: null, out _);

        Assert.That(
            async () => await store.SetAsync(new StoredCredential("user", "secret")),
            Throws.InvalidOperationException);
    }

    [Test]
    public void SetAsync_null_credential_throws()
    {
        var context = ContextWithStartedResponse(hasStarted: false);
        var store = CreateStore(context, out _);

        Assert.That(
            async () => await store.SetAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetAsync_round_trips_a_credential_written_by_SetAsync()
    {
        // Same store instance => same Data Protection purpose, so a cookie written on
        // the response can be read back off the request.
        var writeContext = new DefaultHttpContext();
        var accessor = Substitute.For<IHttpContextAccessor>();
        accessor.HttpContext.Returns(writeContext);
        var store = new CookieCredentialStore(accessor, new EphemeralDataProtectionProvider());

        await store.SetAsync(new StoredCredential("alice", "hunter2"));

        var setCookie = writeContext.Response.Headers.SetCookie.ToString();
        var payload = setCookie.Split(';')[0][(CookieName.Length + 1)..];

        var readContext = new DefaultHttpContext();
        readContext.Request.Headers.Cookie = $"{CookieName}={payload}";
        accessor.HttpContext.Returns(readContext);

        var credential = await store.GetAsync();

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Username, Is.EqualTo("alice"));
            Assert.That(credential.Password, Is.EqualTo("hunter2"));
        });
    }
}
