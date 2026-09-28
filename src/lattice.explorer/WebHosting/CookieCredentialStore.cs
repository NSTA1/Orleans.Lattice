using System.Security.Cryptography;
using System.Text;
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
/// <para>
/// A clear that cannot write headers is <b>not</b> a clear that did nothing. Skipping
/// only the header write would leave a security decision resting on a best-effort
/// side effect: the auth session clears the credential when the console is repointed
/// at a different endpoint precisely so the next launch cannot replay it there, and a
/// silent no-op would let the surviving cookie hand that endpoint's password to
/// whoever now answers at the new address. So <see cref="ClearAsync"/> always revokes
/// the presented cookie value in-process, and <see cref="GetAsync"/> refuses a revoked
/// value, whether or not the delete header could be sent.
/// </para>
/// </remarks>
public sealed class CookieCredentialStore : ICredentialStore
{
    private const string CookieName = "lattice-explorer-cred";
    private const string Purpose = "Orleans.Lattice.Explorer.Credential.v1";

    /// <summary>
    /// How many revoked cookie values are remembered. The set is bounded so a caller
    /// cannot grow it without limit by driving sign-outs; eviction is oldest-first and
    /// only reaches values revoked more than this many revocations ago, by which point
    /// the browser holding one has long since been re-challenged.
    /// </summary>
    private const int MaxRevocations = 1024;

    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly IDataProtector _protector;

    // Cookie values whose credential has been revoked by ClearAsync. Keyed by a
    // SHA-256 digest of the value rather than the value itself, so the revocation
    // ledger never retains the protected payload. Guarded by _revocationGate.
    private readonly HashSet<string> _revoked = new(StringComparer.Ordinal);
    private readonly Queue<string> _revocationOrder = new();
    private readonly Lock _revocationGate = new();

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

        // A revoked value is treated as absent. ClearAsync cannot always delete the
        // cookie header, so the browser can keep presenting a value whose credential
        // was signed out; honouring it here is what would replay it.
        if (IsRevoked(cookie))
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
        if (context is null)
        {
            return Task.CompletedTask;
        }

        // Revoke first, and unconditionally. The delete below is the best-effort half
        // of a sign-out; this is the half that has to hold, because on a Blazor circuit
        // the accessor returns the long-lived SignalR request whose response headers
        // are already sent and no delete can be written at all.
        var presented = context.Request.Cookies[CookieName];
        if (!string.IsNullOrEmpty(presented))
        {
            Revoke(presented);
        }

        // Guard on HasStarted: deleting the cookie once the response has started throws
        // "Headers are read-only, response has already started". The header write is
        // skipped there; the revocation above is what makes the sign-out effective.
        if (!context.Response.HasStarted)
        {
            context.Response.Cookies.Delete(CookieName);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Records <paramref name="cookieValue"/> as revoked, evicting the oldest entry
    /// once the bounded ledger is full.
    /// </summary>
    private void Revoke(string cookieValue)
    {
        var digest = Digest(cookieValue);
        lock (_revocationGate)
        {
            if (!_revoked.Add(digest))
            {
                return;
            }

            _revocationOrder.Enqueue(digest);
            if (_revocationOrder.Count > MaxRevocations)
            {
                _revoked.Remove(_revocationOrder.Dequeue());
            }
        }
    }

    /// <summary>Reports whether <paramref name="cookieValue"/> has been revoked.</summary>
    private bool IsRevoked(string cookieValue)
    {
        var digest = Digest(cookieValue);
        lock (_revocationGate)
        {
            return _revoked.Contains(digest);
        }
    }

    private static string Digest(string cookieValue) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(cookieValue)));
}
