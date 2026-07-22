using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// A Blazor Server <see cref="ICredentialStore"/> that rests the explorer's
/// sign-in credential in an <c>HttpOnly</c>, <c>Secure</c>, <c>SameSite=Strict</c>
/// cookie whose payload is encrypted with ASP.NET Core Data Protection. The
/// credential stays out of JS / XSS reach and out of browser
/// <c>localStorage</c> / <c>sessionStorage</c>; the cookie is written and cleared
/// from the server-side <c>/auth/login</c> and <c>/auth/logout</c> endpoints,
/// where an <see cref="HttpContext"/> is available.
/// </summary>
/// <remarks>
/// Cookie writes require a response whose headers are still unsent, which holds on
/// the auth endpoints but not on a Blazor Server circuit: there the
/// <see cref="IHttpContextAccessor"/> resolves the long-lived SignalR request whose
/// response has already started, so <see cref="SetAsync"/> and <see cref="ClearAsync"/>
/// skip the write (guarding on <see cref="HttpResponse.HasStarted"/>) instead of
/// throwing. The credential is written and cleared on the server-side
/// <c>/auth/login</c> and <c>/auth/logout</c> endpoints; the encrypted cookie is the
/// per-browser at-rest store each circuit's scoped auth session reads its own
/// credential from, so no circuit inherits another operator's sign-in.
/// </remarks>
public sealed class CookieCredentialStore : ICredentialStore
{
    private const string CookieName = "lattice-explorer-cred";
    private const string Purpose = "Orleans.Lattice.Explorer.Credential.v1";

    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly IDataProtector _protector;

    /// <summary>Creates the cookie store.</summary>
    /// <param name="httpContextAccessor">Accessor for the current request context.</param>
    /// <param name="dataProtectionProvider">The Data Protection provider used to encrypt the cookie payload.</param>
    public CookieCredentialStore(
        IHttpContextAccessor httpContextAccessor,
        IDataProtectionProvider dataProtectionProvider)
    {
        ArgumentNullException.ThrowIfNull(httpContextAccessor);
        ArgumentNullException.ThrowIfNull(dataProtectionProvider);
        _httpContextAccessor = httpContextAccessor;
        _protector = dataProtectionProvider.CreateProtector(Purpose);
    }

    /// <inheritdoc />
    public Task<StoredCredential?> GetAsync(CancellationToken cancellationToken = default)
    {
        var context = _httpContextAccessor.HttpContext;
        var cookie = context?.Request.Cookies[CookieName];
        if (string.IsNullOrEmpty(cookie))
        {
            return Task.FromResult<StoredCredential?>(null);
        }

        try
        {
            var json = _protector.Unprotect(cookie);
            return Task.FromResult(JsonSerializer.Deserialize<StoredCredential>(json));
        }
        catch (Exception ex) when (ex is System.Security.Cryptography.CryptographicException or JsonException or FormatException)
        {
            return Task.FromResult<StoredCredential?>(null);
        }
    }

    /// <inheritdoc />
    public Task SetAsync(StoredCredential credential, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(credential);

        var context = _httpContextAccessor.HttpContext
            ?? throw new InvalidOperationException(
                "Setting the credential cookie requires an active HttpContext; sign in through the /auth/login endpoint.");

        // The accessor also returns a context whose response has already started -
        // for example the long-lived SignalR request behind a Blazor circuit, where
        // the auto-sign-in handler runs. Cookie headers cannot be written once the
        // response has started; persisting the credential is best-effort at-rest
        // state, so skip the write and let the next /auth/login request reconcile it.
        if (context.Response.HasStarted)
        {
            return Task.CompletedTask;
        }

        var payload = _protector.Protect(JsonSerializer.Serialize(credential));
        context.Response.Cookies.Append(CookieName, payload, new CookieOptions
        {
            HttpOnly = true,
            Secure = true,
            SameSite = SameSiteMode.Strict,
            IsEssential = true,
        });

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearAsync(CancellationToken cancellationToken = default)
    {
        var context = _httpContextAccessor.HttpContext;

        // Guard on HasStarted: on a Blazor circuit the accessor returns the
        // long-lived SignalR request whose response headers are already sent (this
        // is the path the Entra auto-sign-in handler clears a stale credential
        // from), and deleting the cookie there throws "Headers are read-only,
        // response has already started". The credential is best-effort at-rest
        // state, so skip the write when the response cannot carry it.
        if (context is not null && !context.Response.HasStarted)
        {
            context.Response.Cookies.Delete(CookieName);
        }

        return Task.CompletedTask;
    }
}
