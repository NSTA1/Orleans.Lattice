namespace Orleans.Lattice.Apps.Sources;

/// <summary>One page of <see cref="IAppCatalogSource.ListAsync"/>: the entries and the continuation for the next page.</summary>
public sealed class AppSourcePage
{
    private AppSourcePage(IReadOnlyList<AppSourceEntry> entries, string? continuation)
    {
        Entries = entries;
        Continuation = continuation;
    }

    /// <summary>An empty, final page.</summary>
    public static AppSourcePage Empty { get; } = new([], null);

    /// <summary>The entries on this page, in the source's listing order.</summary>
    public IReadOnlyList<AppSourceEntry> Entries { get; }

    /// <summary>
    /// The opaque token to pass as <see cref="AppSourceQuery.Continuation"/> for the next page, or null when
    /// this is the last page.
    /// </summary>
    public string? Continuation { get; }

    /// <summary>Whether another page follows this one.</summary>
    public bool HasMore => Continuation is not null;

    /// <summary>Creates a page.</summary>
    /// <param name="entries">The entries on the page; copied.</param>
    /// <param name="continuation">The continuation for the next page, or null when this is the last page.</param>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="entries"/> contains null, or <paramref name="continuation"/> is empty.
    /// </exception>
    public static AppSourcePage Create(IReadOnlyList<AppSourceEntry> entries, string? continuation)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (continuation is { Length: 0 })
            throw new ArgumentException("A continuation must be null or non-empty.", nameof(continuation));
        if (entries.Count == 0 && continuation is null)
            return Empty;
        var copy = new AppSourceEntry[entries.Count];
        for (var i = 0; i < copy.Length; i++)
            copy[i] = entries[i] ?? throw new ArgumentException("Entries cannot contain null.", nameof(entries));
        return new(copy, continuation);
    }

    /// <summary>Creates a page over an array the caller has already copied and no longer mutates.</summary>
    internal static AppSourcePage Wrap(AppSourceEntry[] entries, string? continuation) =>
        entries.Length == 0 && continuation is null ? Empty : new(entries, continuation);
}
