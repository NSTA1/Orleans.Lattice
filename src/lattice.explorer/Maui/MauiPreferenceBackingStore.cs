using Microsoft.Maui.Storage;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer;

/// <summary>
/// The desktop head's durable <see cref="IUiPreferenceBackingStore"/>: persists
/// the preference document to the platform preference store via
/// <see cref="Preferences"/>, so UI preferences survive an app restart. The
/// MAUI preference API is synchronous; the calls are wrapped in completed tasks.
/// </summary>
internal sealed class MauiPreferenceBackingStore : IUiPreferenceBackingStore
{
    public Task<string?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Task.FromResult(Preferences.Default.ContainsKey(key)
            ? Preferences.Default.Get<string?>(key, null)
            : null);
    }

    public Task SetAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        Preferences.Default.Set(key, value);
        return Task.CompletedTask;
    }

    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        Preferences.Default.Remove(key);
        return Task.CompletedTask;
    }
}
