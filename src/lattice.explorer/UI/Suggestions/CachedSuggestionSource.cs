using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// A suggestion source over a small list read once and matched locally on every
/// keystroke: regions, tenants, provider keys. The list is remembered for the
/// circuit, keyed on the caller (<see cref="ShellCaller"/>: the sign-in, the
/// endpoint and the asserted tenant) and for at most <see cref="Freshness"/>, so
/// typing never calls the cluster per key and an answer read for one caller is
/// never offered to another.
/// </summary>
/// <remarks>
/// A list that could not be read is remembered as unavailable for the same window,
/// so a failing facade is not asked again on every key; the field meanwhile
/// accepts what is typed and says why.
/// </remarks>
/// <param name="caller">The circuit's caller, or <see langword="null"/> for a host with no sessions.</param>
/// <param name="time">The clock the freshness window is measured on.</param>
internal abstract class CachedSuggestionSource(ShellCaller? caller, TimeProvider? time) : ILtSuggestionSource
{
    /// <summary>How long a read list is reused.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromSeconds(30);

    private readonly ShellCaller _caller = caller ?? new ShellCaller();
    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private Remembered? _remembered;

    /// <summary>The sentence shown when the list cannot be read.</summary>
    protected abstract string UnavailableReason { get; }

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var key = _caller.Current;
        var now = _time.GetUtcNow();
        if (_remembered is not { } remembered
            || remembered.Caller != key
            || now - remembered.ReadAt >= Freshness)
        {
            IReadOnlyList<LtSuggestion>? values;
            try
            {
                values = await LoadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                values = null;
            }

            remembered = new Remembered(key, now, values);
            if (_caller.Current == key)
            {
                _remembered = remembered;
            }
        }

        return remembered.Values is { } list
            ? SuggestionMatcher.Match(list, text, limit)
            : LtSuggestionSet.Unavailable(UnavailableReason);
    }

    /// <summary>Forgets the remembered list, so the next query reads it again.</summary>
    public void Invalidate() => _remembered = null;

    /// <summary>Reads every existing value, in display order.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The values, or <see langword="null"/> when they cannot be listed here.</returns>
    protected abstract Task<IReadOnlyList<LtSuggestion>?> LoadAsync(CancellationToken cancellationToken);

    /// <summary>A read list, the caller it was read for, and when; <c>Values</c> is null when unreadable.</summary>
    private sealed record Remembered(ShellCallerKey Caller, DateTimeOffset ReadAt, IReadOnlyList<LtSuggestion>? Values);
}
