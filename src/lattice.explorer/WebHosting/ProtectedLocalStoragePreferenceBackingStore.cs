using Microsoft.AspNetCore.Components.Server.ProtectedBrowserStorage;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// The web head's durable <see cref="IUiPreferenceBackingStore"/>: persists the
/// preference document to the browser's per-origin <c>localStorage</c> through
/// <see cref="ProtectedLocalStorage"/> (Data Protection-encrypted, so a user
/// cannot tamper with it). Reads and writes throw during server prerender - when
/// no JS interop is available - which the preference store treats as "not yet
/// loadable" and retries once the circuit is interactive.
/// </summary>
internal sealed class ProtectedLocalStoragePreferenceBackingStore(ProtectedLocalStorage storage)
    : IUiPreferenceBackingStore
{
    private readonly ProtectedLocalStorage _storage = storage ?? throw new ArgumentNullException(nameof(storage));

    public async Task<string?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        var result = await _storage.GetAsync<string>(key).ConfigureAwait(false);
        return result.Success ? result.Value : null;
    }

    public async Task SetAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        await _storage.SetAsync(key, value).ConfigureAwait(false);
    }

    public async Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        await _storage.DeleteAsync(key).ConfigureAwait(false);
    }
}
