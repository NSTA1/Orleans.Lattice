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
