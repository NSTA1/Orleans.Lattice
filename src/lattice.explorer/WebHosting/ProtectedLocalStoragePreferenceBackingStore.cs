using System.Security.Cryptography;
using Microsoft.AspNetCore.Components.Server.ProtectedBrowserStorage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// The web head's durable <see cref="IUiPreferenceBackingStore"/>: persists the
/// preference document to the browser's per-origin <c>localStorage</c> through
/// <see cref="ProtectedLocalStorage"/> (Data Protection-encrypted, so a user
/// cannot tamper with it). Reads and writes throw during server prerender - when
/// no JS interop is available - which the preference store treats as "not yet
/// loadable" and retries once the circuit is interactive. A stored document that
/// can never be decrypted (a tampered value, or one protected under a key ring
/// this host no longer holds) is permanently unreadable rather than unreachable,
/// so it is logged, deleted on a best-effort basis, and reported as absent: the
/// session starts from default preferences instead of failing every load.
/// </summary>
internal sealed partial class ProtectedLocalStoragePreferenceBackingStore(
    ProtectedLocalStorage storage,
    ILogger<ProtectedLocalStoragePreferenceBackingStore>? logger = null)
    : IUiPreferenceBackingStore
{
    private readonly ProtectedLocalStorage _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    private readonly ILogger _logger = (ILogger?)logger ?? NullLogger<ProtectedLocalStoragePreferenceBackingStore>.Instance;

    public async Task<string?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        try
        {
            var result = await _storage.GetAsync<string>(key).ConfigureAwait(false);
            return result.Success ? result.Value : null;
        }
        catch (CryptographicException ex)
        {
            // Retrying cannot help: the value will never decrypt. Discard it loudly so
            // the user's preference reset is observable, then report it as absent.
            LogUndecryptableDiscarded(_logger, key, ex);
            try
            {
                await _storage.DeleteAsync(key).ConfigureAwait(false);
            }
            catch
            {
                // Best effort only: the next save overwrites the unreadable value anyway.
            }

            return null;
        }
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

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "The UI preference document under '{Key}' could not be decrypted and was discarded; preferences were reset to their defaults.")]
    private static partial void LogUndecryptableDiscarded(ILogger logger, string key, Exception exception);
}
