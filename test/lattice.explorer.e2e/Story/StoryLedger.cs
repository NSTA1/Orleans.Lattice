using System.Collections.Concurrent;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// Carries run-time facts between chapters under fixed <see cref="StoryKeys"/>: in story mode
/// the chapter that produced a fact recorded it; in isolated mode the F6 contract that
/// established the chapter did. One ledger per storyline run.
/// </summary>
public sealed class StoryLedger
{
    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);

    /// <summary>Records <paramref name="value"/> under <paramref name="key"/>, replacing an earlier fact.</summary>
    /// <typeparam name="T">The fact's type.</typeparam>
    /// <param name="key">The key.</param>
    /// <param name="value">The fact.</param>
    /// <param name="writer">The chapter recording it, for the ledger's report.</param>
    public void Set<T>(StoryKey<T> key, T value, string? writer = null)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        _entries[key.Name] = new Entry(value, typeof(T), writer);
    }

    /// <summary>The fact under <paramref name="key"/>.</summary>
    /// <typeparam name="T">The fact's type.</typeparam>
    /// <param name="key">The key.</param>
    /// <exception cref="KeyNotFoundException">No chapter recorded the fact: a consumed chapter did not produce it.</exception>
    public T Get<T>(StoryKey<T> key)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(key);
        if (TryGet(key, out var value))
        {
            return value;
        }

        throw new KeyNotFoundException(
            $"The story ledger holds no '{key.Name}'. The chapter whose contract produces it has not run (story mode) or its F6 contract did not record it (isolated mode).");
    }

    /// <summary>Whether a fact is recorded under <paramref name="key"/>, and the fact.</summary>
    /// <typeparam name="T">The fact's type.</typeparam>
    /// <param name="key">The key.</param>
    /// <param name="value">The fact, when recorded.</param>
    /// <exception cref="InvalidCastException">The fact was recorded with another type.</exception>
    public bool TryGet<T>(StoryKey<T> key, out T value)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!_entries.TryGetValue(key.Name, out var entry))
        {
            value = default!;
            return false;
        }

        if (entry.Value is not T typed)
        {
            throw new InvalidCastException($"The ledger's '{key.Name}' was recorded as {entry.Type.Name}, not {typeof(T).Name}.");
        }

        value = typed;
        return true;
    }

    /// <summary>The keys recorded so far, in ordinal order.</summary>
    public IReadOnlyList<string> Keys => [.. _entries.Keys.Order(StringComparer.Ordinal)];

    /// <summary>The chapter that recorded <paramref name="key"/>, or <see langword="null"/>.</summary>
    /// <param name="key">The key's name.</param>
    public string? WriterOf(string key) => _entries.TryGetValue(key, out var entry) ? entry.Writer : null;

    /// <summary>The ledger as indented JSON: each key with its value and writer, for the run's artifacts.</summary>
    public string ToJson() =>
        JsonSerializer.Serialize(
            _entries.OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .ToDictionary(pair => pair.Key, pair => new { value = pair.Value.Value, writer = pair.Value.Writer }),
            new JsonSerializerOptions { WriteIndented = true });

    private sealed record Entry(object Value, Type Type, string? Writer);
}
