namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// The registry of every preference key the running Explorer may write: the
/// shell's own (<see cref="ExplorerPreferenceKeys"/>) plus any a feature
/// registers.
/// </summary>
/// <remarks>
/// <para>
/// This is what makes the contract <em>enumerable</em>, and therefore what makes
/// "reset my view" possible at all: the shell can only clear what it can list.
/// It is also the extension point - a feature that needs to remember something
/// registers its key at startup and gains scoping, reset and fallback handling
/// without touching the shell.
/// </para>
/// <para>
/// Registered as a singleton: keys are declarations about the application, not
/// per-session state.
/// </para>
/// </remarks>
public interface IExplorerPreferenceCatalog
{
    /// <summary>
    /// Every registered key, in registration order with the shell's own first.
    /// </summary>
    IReadOnlyList<ExplorerPreferenceKey> Keys { get; }

    /// <summary>
    /// Registers <paramref name="key"/>, or returns the already-registered
    /// instance when the same instance was registered before (so a startup path
    /// that runs twice is harmless).
    /// </summary>
    /// <param name="key">The key to register. Must not be <see langword="null"/>.</param>
    /// <returns>The registered key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// A <em>different</em> key is already registered under the same name. Two
    /// declarations of one name is a contract bug: whichever loaded second would
    /// otherwise silently take over the first's stored value.
    /// </exception>
    ExplorerPreferenceKey Register(ExplorerPreferenceKey key);

    /// <summary>Looks a key up by its name.</summary>
    /// <param name="name">The canonical key name.</param>
    /// <param name="key">The registered key when found.</param>
    /// <returns><see langword="true"/> when a key is registered under that name.</returns>
    bool TryGet(string? name, out ExplorerPreferenceKey key);

    /// <summary>
    /// Whether <paramref name="key"/> is the registered declaration for its name.
    /// </summary>
    /// <param name="key">The key to test.</param>
    bool Contains(ExplorerPreferenceKey? key);
}
