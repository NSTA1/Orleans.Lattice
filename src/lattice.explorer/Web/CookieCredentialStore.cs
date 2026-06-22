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
/// Cookie reads and writes require an active <see cref="HttpContext"/>, which is
/// present on the auth endpoints but not on a SignalR circuit. The auth session
/// is a process singleton, so the in-memory credential established at sign-in
/// remains effective for the life of the process; the encrypted cookie provides
/// at-rest persistence for the credential rather than circuit-time retrieval.
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
        context?.Response.Cookies.Delete(CookieName);
        return Task.CompletedTask;
    }
}
