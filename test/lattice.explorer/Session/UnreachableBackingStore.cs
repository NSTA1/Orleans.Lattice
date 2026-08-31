using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// A preference backing store that is never reachable, so
/// <see cref="IExplorerShellPreferences.IsLoaded"/> stays <see langword="false"/>
/// however many times a component tries to hydrate it.
/// </summary>
/// <remarks>
/// <para>
/// This is a real condition, not a test artefact: browser storage is unreachable
/// during a server prerender, and stays unreachable for the whole session in a
/// head running with script disabled. A surface that renders nothing at all in
/// that state is a surface a real user can be left staring at, so it needs to be
/// reachable from a test.
/// </para>
/// <para>
/// It fails deterministically - by throwing on the read, which is exactly what
/// the preference store classifies as "not yet loadable" - so nothing here waits
/// on a clock or a race.
/// </para>
/// </remarks>
internal sealed class UnreachableBackingStore : IUiPreferenceBackingStore
{
    private const string Unreachable = "The preference backing store is unreachable.";

    /// <inheritdoc />
    public Task<string?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromException<string?>(new InvalidOperationException(Unreachable));

    /// <inheritdoc />
    public Task SetAsync(string key, string value, CancellationToken cancellationToken = default) =>
        Task.FromException(new InvalidOperationException(Unreachable));

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromException(new InvalidOperationException(Unreachable));
}
