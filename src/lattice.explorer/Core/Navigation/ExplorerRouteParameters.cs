using System.Collections;

namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The immutable, key-sorted set of extra query parameters an
/// <see cref="ExplorerRoute"/> carries beyond the shell's own tenant-scope keys.
/// </summary>
/// <remarks>
/// <para>
/// This is the epic's extension point for URL state: a downstream surface that
/// needs a filter, a page cursor or a sub-tab in the address bar adds its key
/// here, and the shell's route grammar, parser and formatter carry it without
/// change. Keys are canonical lower case, enforced by
/// <see cref="ExplorerRouteParameter"/>.
/// </para>
/// <para>
/// Entries are held sorted by key, which buys two properties the shell depends
/// on. Formatting is deterministic, so the same logical view always produces the
/// same URL and a bookmark round-trips byte for byte. And equality is
/// order-insensitive, so a route rebuilt from a parsed URL compares equal to the
/// one that produced it - which is what lets
/// <see cref="ExplorerShellRouter"/> suppress the echo of its own navigation
/// without any timing.
/// </para>
/// <para>
/// The type carries value equality over its entries (records do not do this for
/// an array field), and exposes a struct enumerator so a <c>foreach</c> on the
/// formatting path allocates nothing.
/// </para>
/// </remarks>
public sealed class ExplorerRouteParameters : IReadOnlyList<ExplorerRouteParameter>, IEquatable<ExplorerRouteParameters>
{
    private static readonly ExplorerRouteParameter[] NoEntries = [];

    /// <summary>The shared empty set. Every route with no extra parameters uses this instance.</summary>
    public static ExplorerRouteParameters Empty { get; } = new(NoEntries);

    private readonly ExplorerRouteParameter[] _entries;

    private ExplorerRouteParameters(ExplorerRouteParameter[] entries) => _entries = entries;

    /// <summary>
    /// Creates a parameter set from <paramref name="parameters"/>, sorting by key
    /// and keeping the last value supplied for a repeated key.
    /// </summary>
    /// <param name="parameters">The parameters to carry. A <see langword="null"/> or empty sequence yields <see cref="Empty"/>.</param>
    /// <exception cref="ArgumentException">A key is not canonical lower case.</exception>
    public static ExplorerRouteParameters Create(IEnumerable<ExplorerRouteParameter>? parameters)
    {
        if (parameters is null)
        {
            return Empty;
        }

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var parameter in parameters)
        {
            // Constructing through the record validates the key, so an invalid
            // key thrown from a caller-supplied sequence still fails here.
            map[parameter.Key] = parameter.Value;
        }

        return FromMap(map);
    }

    /// <summary>The number of parameters carried.</summary>
    public int Count => _entries.Length;

    /// <summary>The parameter at <paramref name="index"/> in key order.</summary>
    /// <param name="index">The zero-based index.</param>
    public ExplorerRouteParameter this[int index] => _entries[index];

    /// <summary>
    /// Reads the value stored under <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The query key to look up. Case sensitive: keys are canonical lower case.</param>
    /// <param name="value">The value when present, otherwise <see cref="string.Empty"/>.</param>
    /// <returns><see langword="true"/> when the key is present.</returns>
    public bool TryGetValue(string? key, out string value)
    {
        if (!string.IsNullOrEmpty(key))
        {
            // Linear over a set that is empty or holds a couple of entries in
            // practice, so no dictionary is built and nothing is allocated.
            for (var i = 0; i < _entries.Length; i++)
            {
                if (string.Equals(_entries[i].Key, key, StringComparison.Ordinal))
                {
                    value = _entries[i].Value;
                    return true;
                }
            }
        }

        value = string.Empty;
        return false;
    }

    /// <summary>
    /// Reads the value stored under <paramref name="key"/>, or
    /// <see cref="string.Empty"/> when absent.
    /// </summary>
    /// <param name="key">The query key to look up.</param>
    public string GetValueOrEmpty(string? key) => TryGetValue(key, out var value) ? value : string.Empty;

    /// <summary>
    /// Returns a set with <paramref name="key"/> set to <paramref name="value"/>,
    /// replacing any existing entry. An empty or <see langword="null"/>
    /// <paramref name="value"/> removes the key instead, so a caller clearing a
    /// filter does not leave <c>?filter=</c> behind in the URL.
    /// </summary>
    /// <param name="key">The canonical lower-case query key.</param>
    /// <param name="value">The value to carry, or empty to remove the key.</param>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not canonical lower case.</exception>
    public ExplorerRouteParameters With(string key, string? value)
    {
        ExplorerRouteSlug.EnsureCanonical(key);

        if (string.IsNullOrEmpty(value))
        {
            return Without(key);
        }

        if (TryGetValue(key, out var existing) && string.Equals(existing, value, StringComparison.Ordinal))
        {
            return this;
        }

        var map = ToMap(_entries.Length + 1);
        map[key] = value;
        return FromMap(map);
    }

    /// <summary>
    /// Returns a set without <paramref name="key"/>, or this instance when the
    /// key is absent.
    /// </summary>
    /// <param name="key">The query key to drop.</param>
    public ExplorerRouteParameters Without(string? key)
    {
        if (string.IsNullOrEmpty(key) || !TryGetValue(key, out _))
        {
            return this;
        }

        if (_entries.Length == 1)
        {
            return Empty;
        }

        var remaining = new ExplorerRouteParameter[_entries.Length - 1];
        var next = 0;
        for (var i = 0; i < _entries.Length; i++)
        {
            if (!string.Equals(_entries[i].Key, key, StringComparison.Ordinal))
            {
                remaining[next++] = _entries[i];
            }
        }

        return new ExplorerRouteParameters(remaining);
    }

    /// <inheritdoc />
    public bool Equals(ExplorerRouteParameters? other)
    {
        if (ReferenceEquals(this, other))
        {
            return true;
        }

        if (other is null || other._entries.Length != _entries.Length)
        {
            return false;
        }

        // Both sides are key-sorted, so a positional walk is a set comparison.
        for (var i = 0; i < _entries.Length; i++)
        {
            if (_entries[i] != other._entries[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as ExplorerRouteParameters);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        for (var i = 0; i < _entries.Length; i++)
        {
            hash.Add(_entries[i].Key, StringComparer.Ordinal);
            hash.Add(_entries[i].Value, StringComparer.Ordinal);
        }

        return hash.ToHashCode();
    }

    /// <summary>A struct enumerator, so the formatting path iterates without allocating.</summary>
    public Enumerator GetEnumerator() => new(_entries);

    IEnumerator<ExplorerRouteParameter> IEnumerable<ExplorerRouteParameter>.GetEnumerator() =>
        ((IEnumerable<ExplorerRouteParameter>)_entries).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _entries.GetEnumerator();

    private Dictionary<string, string> ToMap(int capacity)
    {
        var map = new Dictionary<string, string>(capacity, StringComparer.Ordinal);
        for (var i = 0; i < _entries.Length; i++)
        {
            map[_entries[i].Key] = _entries[i].Value;
        }

        return map;
    }

    private static ExplorerRouteParameters FromMap(Dictionary<string, string> map)
    {
        if (map.Count == 0)
        {
            return Empty;
        }

        var entries = new ExplorerRouteParameter[map.Count];
        var next = 0;
        foreach (var (key, value) in map)
        {
            entries[next++] = new ExplorerRouteParameter(key, value);
        }

        Array.Sort(entries, static (left, right) => string.CompareOrdinal(left.Key, right.Key));
        return new ExplorerRouteParameters(entries);
    }

    /// <summary>Allocation-free enumerator over an <see cref="ExplorerRouteParameters"/>.</summary>
    public struct Enumerator
    {
        private readonly ExplorerRouteParameter[] _entries;
        private int _index;

        internal Enumerator(ExplorerRouteParameter[] entries)
        {
            _entries = entries;
            _index = -1;
        }

        /// <summary>The parameter at the current position.</summary>
        public readonly ExplorerRouteParameter Current => _entries[_index];

        /// <summary>Advances to the next parameter.</summary>
        /// <returns><see langword="true"/> while a parameter remains.</returns>
        public bool MoveNext() => ++_index < _entries.Length;
    }
}
