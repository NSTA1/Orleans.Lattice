namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Default <see cref="IExplorerPreferenceCatalog"/>: the shell's declared keys
/// plus whatever features register, held in registration order.
/// </summary>
/// <remarks>
/// <b>Register this through an explicit factory, never by implementation type.</b>
/// The two constructors below are ambiguous to a DI container: registering
/// <c>TryAddSingleton&lt;IExplorerPreferenceCatalog, ExplorerPreferenceCatalog&gt;()</c>
/// makes Microsoft.Extensions.DependencyInjection choose the constructor with the
/// most satisfiable parameters, and an <see cref="IEnumerable{T}"/> is always
/// satisfiable because the container synthesises an empty sequence for it. The
/// seed constructor therefore wins with zero keys and the parameterless one never
/// runs, yielding an empty catalog that makes every
/// <see cref="IExplorerShellPreferences"/> member throw. Use
/// <c>TryAddSingleton&lt;IExplorerPreferenceCatalog&gt;(_ =&gt; new ExplorerPreferenceCatalog())</c>,
/// as <see cref="ExplorerSessionServiceCollectionExtensions.AddExplorerSession"/>
/// does.
/// </remarks>
public sealed class ExplorerPreferenceCatalog : IExplorerPreferenceCatalog
{
    private readonly List<ExplorerPreferenceKey> _keys = [];
    private readonly Dictionary<string, ExplorerPreferenceKey> _byName = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates a catalog seeded with <see cref="ExplorerPreferenceKeys.All"/>, so
    /// the shell's own contract is always present.
    /// </summary>
    public ExplorerPreferenceCatalog()
        : this(ExplorerPreferenceKeys.All)
    {
    }

    /// <summary>
    /// Creates a catalog seeded with <paramref name="seed"/>. Used by tests that
    /// want a contract containing only their own keys.
    /// </summary>
    /// <param name="seed">The keys to seed. Must not be <see langword="null"/> or contain nulls.</param>
    /// <exception cref="ArgumentNullException"><paramref name="seed"/> is <see langword="null"/>.</exception>
    public ExplorerPreferenceCatalog(IEnumerable<ExplorerPreferenceKey> seed)
    {
        ArgumentNullException.ThrowIfNull(seed);

        foreach (var key in seed)
        {
            Register(key);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerPreferenceKey> Keys => _keys;

    /// <inheritdoc />
    public ExplorerPreferenceKey Register(ExplorerPreferenceKey key)
    {
        ArgumentNullException.ThrowIfNull(key);

        if (_byName.TryGetValue(key.Name, out var existing))
        {
            if (ReferenceEquals(existing, key))
            {
                return existing;
            }

            throw new InvalidOperationException(
                $"A different preference key is already registered as '{key.Name}'. Declare each key exactly once as a static readonly field and share that instance.");
        }

        _byName.Add(key.Name, key);
        _keys.Add(key);
        return key;
    }

    /// <inheritdoc />
    public bool TryGet(string? name, out ExplorerPreferenceKey key)
    {
        if (name is not null)
        {
            return _byName.TryGetValue(name, out key!);
        }

        key = null!;
        return false;
    }

    /// <inheritdoc />
    public bool Contains(ExplorerPreferenceKey? key) =>
        key is not null && _byName.TryGetValue(key.Name, out var existing) && ReferenceEquals(existing, key);
}
