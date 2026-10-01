namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// The host-specific persistence seam behind <see cref="IUiPreferenceStore"/>.
/// The store keeps all preferences in a single serialized document under one
/// fixed key, so a backing store only needs string get / set / remove by key.
/// The web head targets per-origin browser storage (<c>localStorage</c> via
/// protected browser storage); another host can supply an equivalent durable
/// backing store.
/// </summary>
public interface IUiPreferenceBackingStore
{
    /// <summary>
    /// Reads the string stored under <paramref name="key"/>, or
    /// <see langword="null"/> when absent. May throw when the backing store is
    /// unreachable (for example, browser storage during server prerender); the
    /// preference store treats that as "not yet loadable".
    /// </summary>
    Task<string?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Writes <paramref name="value"/> under <paramref name="key"/>.</summary>
    Task SetAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>Removes any value stored under <paramref name="key"/>.</summary>
    Task RemoveAsync(string key, CancellationToken cancellationToken = default);
}
