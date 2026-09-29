using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;

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
/// <para>
/// That revocation ledger is process-local, bounded, and lost on restart, so it is
/// not by itself sufficient to hold the endpoint binding: a restart, a second web-head
/// replica, or an eviction would each resurrect a signed-out cookie. The binding is
/// therefore recorded in the payload itself - <see cref="SetAsync"/> stamps the
/// endpoint the credential was minted for and <see cref="GetAsync"/> refuses any
/// credential whose stamp is not recognisably the endpoint now configured, failing
/// closed when no endpoint can be resolved. The ledger remains as the prompt,
/// same-process half of a sign-out.
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
    private readonly IExplorerConfigStore? _configStore;

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
        : this(httpContextAccessor, dataProtectionProvider, configStore: null)
    {
    }

    /// <summary>
    /// Creates the cookie store, binding each persisted credential to the endpoint
    /// it was minted for.
    /// </summary>
    /// <param name="httpContextAccessor">Accessor for the current request context.</param>
    /// <param name="dataProtectionProvider">The Data Protection provider used to encrypt the cookie payload.</param>
    /// <param name="configStore">
    /// The process-wide configuration document naming the endpoint every circuit
    /// dials. Supplied, the store stamps that endpoint into the cookie payload and
    /// refuses on read a credential minted for a different one. Omitted, no binding
    /// is recorded and the endpoint check cannot run, so a host that wants it must
    /// register a store. It is a singleton, as this store is, so reading it here
    /// captures no per-circuit state.
    /// </param>
    public CookieCredentialStore(
        IHttpContextAccessor httpContextAccessor,
        IDataProtectionProvider dataProtectionProvider,
        IExplorerConfigStore? configStore)
    {
        ArgumentNullException.ThrowIfNull(httpContextAccessor);
        ArgumentNullException.ThrowIfNull(dataProtectionProvider);
        _httpContextAccessor = httpContextAccessor;
        _protector = dataProtectionProvider.CreateProtector(Purpose);
        _configStore = configStore;
    }

    /// <inheritdoc />
    public async Task<StoredCredential?> GetAsync(CancellationToken cancellationToken = default)
    {
        var context = _httpContextAccessor.HttpContext;
        var cookie = context?.Request.Cookies[CookieName];
        if (string.IsNullOrEmpty(cookie))
        {
            return null;
        }

        // A revoked value is treated as absent. ClearAsync cannot always delete the
        // cookie header, so the browser can keep presenting a value whose credential
        // was signed out; honouring it here is what would replay it.
        if (IsRevoked(cookie))
        {
            return null;
        }

        string json;
        try
        {
            json = _protector.Unprotect(cookie);
        }
        catch (Exception ex) when (ex is CryptographicException or FormatException)
        {
            return null;
        }

        CredentialEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<CredentialEnvelope>(json);
        }
        catch (JsonException)
        {
            return null;
        }

        // The bound shape. Honour it only for the endpoint it was minted for: the
        // revocation ledger above is process-local and bounded, so it cannot be the
        // only thing standing between a surviving cookie and a replay of this
        // password to whoever now answers at a new address.
        if (envelope?.Credential is { } bound)
        {
            var current = await ResolveEndpointAsync(cancellationToken).ConfigureAwait(false);

            // Fail closed: an endpoint that cannot be resolved, or one that is not
            // recognisably the endpoint this credential was minted for, refuses.
            return IsSameEndpoint(envelope.Endpoint, current) ? bound : null;
        }

        // The legacy unbound shape, written before this store stamped an endpoint or
        // by a host that registered no configuration store. There is nothing to check
        // it against, so it is served only when this store could not have bound it in
        // the first place; a host that binds refuses what it cannot verify.
        if (_configStore is not null)
        {
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize<StoredCredential>(json);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async Task SetAsync(StoredCredential credential, CancellationToken cancellationToken = default)
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
            return;
        }

        // Stamp the endpoint this credential is being minted for, so a later read
        // against a different endpoint can refuse it. A host with no configuration
        // store records the legacy unbound shape, which that read serves only when
        // no binding was possible.
        string payloadJson;
        if (_configStore is null)
        {
            payloadJson = JsonSerializer.Serialize(credential);
        }
        else
        {
            var endpoint = await ResolveEndpointAsync(cancellationToken).ConfigureAwait(false);
            payloadJson = JsonSerializer.Serialize(new CredentialEnvelope(endpoint, credential));
        }

        var payload = _protector.Protect(payloadJson);
        context.Response.Cookies.Append(CookieName, payload, new CookieOptions
        {
            HttpOnly = true,
            Secure = true,
            SameSite = SameSiteMode.Strict,
            IsEssential = true,
        });
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
        //
        // Only a value this store actually minted is admitted to the bounded ledger.
        // The presented cookie is caller-supplied and /auth/logout needs no sign-in,
        // so admitting junk would let an anonymous caller enqueue MaxRevocations
        // fabricated values and evict a genuine revocation - flushing the ledger and
        // bringing a signed-out credential back to life. A value that does not
        // unprotect can never be honoured by GetAsync anyway, so it needs no slot.
        var presented = context.Request.Cookies[CookieName];
        if (!string.IsNullOrEmpty(presented) && IsMintedHere(presented))
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

    /// <summary>
    /// Reports whether <paramref name="cookieValue"/> is a payload this store
    /// protected, and so is capable of being honoured on a later read.
    /// </summary>
    private bool IsMintedHere(string cookieValue)
    {
        try
        {
            _protector.Unprotect(cookieValue);
            return true;
        }
        catch (Exception ex) when (ex is CryptographicException or FormatException)
        {
            return false;
        }
    }

    /// <summary>
    /// Reads the endpoint the console is currently pointed at, or
    /// <see langword="null"/> when no configuration store is registered or the
    /// document names none.
    /// </summary>
    private async Task<string?> ResolveEndpointAsync(CancellationToken cancellationToken)
    {
        if (_configStore is null)
        {
            return null;
        }

        try
        {
            var configuration = await _configStore.LoadAsync(cancellationToken).ConfigureAwait(false);
            return configuration?.Endpoint;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
        {
            // Fail closed: an unreadable configuration resolves no endpoint, and an
            // unresolvable endpoint refuses the credential rather than replaying it.
            return null;
        }
    }

    /// <summary>
    /// Compares the endpoint a credential was minted for against the current one.
    /// Deliberately conservative, matching the auth session's own comparison:
    /// anything that is not recognisably the same endpoint is a different one.
    /// </summary>
    private static bool IsSameEndpoint(string? mintedFor, string? current)
    {
        if (string.IsNullOrWhiteSpace(mintedFor) || string.IsNullOrWhiteSpace(current))
        {
            return false;
        }

        return string.Equals(
            mintedFor.TrimEnd('/'),
            current.TrimEnd('/'),
            StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The persisted cookie payload: a credential together with the endpoint it was
    /// minted for. The legacy shape serialized a bare <see cref="StoredCredential"/>,
    /// which deserializes here with a null <see cref="Credential"/> and is how the
    /// two are told apart.
    /// </summary>
    /// <param name="Endpoint">The endpoint the credential was minted for.</param>
    /// <param name="Credential">The credential itself.</param>
    private sealed record CredentialEnvelope(string? Endpoint, StoredCredential? Credential);
}
