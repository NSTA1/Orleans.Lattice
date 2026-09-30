namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// Runs a combobox's suggestion queries: debounced by input, not by the clock.
/// </summary>
/// <remarks>
/// <para>
/// At most one query is outstanding. A request that arrives while one runs
/// cancels it and records the latest text; when the cancelled query settles, one
/// query runs for whatever was typed last. So a burst of keys costs one query,
/// however fast the typing, and nothing waits on a timer. An answer is delivered
/// only when no newer text is waiting, so a slow, stale answer can never replace
/// a newer one.
/// </para>
/// <para>
/// It is driven from one component on the renderer's synchronization context,
/// so its fields need no lock. A cancellation source is reused while it was not
/// cancelled, so a query that completes normally allocates no new one. A source is
/// never disposed: a query can resume after its field is gone, and a plain source
/// holds nothing that needs releasing (see <see cref="ComponentLifetime"/>).
/// </para>
/// </remarks>
/// <param name="deliver">Called with the text and the answer for it, on the renderer's context.</param>
internal sealed class LtSuggestionPump(Func<string, LtSuggestionSet, Task> deliver) : IDisposable
{
    /// <summary>The note a faulted query is reported with: the field then accepts what is typed.</summary>
    public const string FaultReason = "Suggestions could not be loaded, so the value is used as typed.";

    private CancellationTokenSource? _query;
    private Task? _running;
    private string? _pending;
    private ILtSuggestionSource? _pendingSource;
    private int _limit;
    private bool _disposed;

    /// <summary>Whether a query is outstanding.</summary>
    public bool IsRunning => _running is { IsCompleted: false };

    /// <summary>
    /// Asks <paramref name="source"/> for suggestions matching <paramref name="text"/>,
    /// cancelling any query still running for earlier text.
    /// </summary>
    /// <param name="source">The source to ask.</param>
    /// <param name="text">The latest text.</param>
    /// <param name="limit">The most suggestions to ask for.</param>
    /// <returns>A task that completes when the latest answer has been delivered or abandoned.</returns>
    public Task RequestAsync(ILtSuggestionSource source, string text, int limit)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(text);
        if (_disposed)
        {
            return Task.CompletedTask;
        }

        _pending = text;
        _pendingSource = source;
        _limit = limit;
        if (_running is { IsCompleted: false } running)
        {
            _query?.Cancel();
            return running;
        }

        _running = RunAsync();
        return _running;
    }

    /// <summary>Abandons the running query, if any, and anything waiting behind it.</summary>
    public void Cancel()
    {
        _pending = null;
        _pendingSource = null;
        _query?.Cancel();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _disposed = true;
        Cancel();
    }

    private async Task RunAsync()
    {
        while (!_disposed && _pending is { } text && _pendingSource is { } source)
        {
            _pending = null;
            _pendingSource = null;

            var query = _query is { } previous && previous.TryReset() ? previous : Replace();
            LtSuggestionSet answer;
            try
            {
                answer = await source.SuggestAsync(text, _limit, query.Token).ConfigureAwait(true);
            }
            catch (OperationCanceledException) when (query.IsCancellationRequested)
            {
                continue;
            }
            catch (Exception)
            {
                // Fail closed to free text: a fault in a source never breaks the field.
                answer = LtSuggestionSet.Unavailable(FaultReason);
            }

            if (_disposed || _pending is not null || query.IsCancellationRequested)
            {
                // Newer text is waiting, or the query was abandoned: this answer is stale.
                continue;
            }

            await deliver(text, answer).ConfigureAwait(true);
        }
    }

    private CancellationTokenSource Replace()
    {
        _query = new CancellationTokenSource();
        return _query;
    }
}
