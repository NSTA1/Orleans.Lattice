using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Explorer.UI.Navigation.Completion;

/// <summary>
/// Asks every completion source at once and yields each one's answer as it
/// arrives, so the address line fills in progressively and a slow or failing
/// source never holds back the others.
/// </summary>
/// <remarks>
/// Each source runs under its own <see cref="ExplorerChromeOptions.CompletionTimeout"/>
/// and is cancelled when that runs out or the caller stops wanting the answer.
/// A source that throws or times out yields an empty batch with that outcome;
/// one that returns more than <see cref="AddressQuery.Limit"/> results is cut
/// to the limit.
/// </remarks>
internal sealed class AddressCompletionFanOut
{
    private readonly ExplorerChromeOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    /// <summary>Creates the fan-out.</summary>
    /// <param name="options">The chrome's time bounds.</param>
    /// <param name="time">The clock the bounds are measured on.</param>
    /// <param name="logger">Where a failing source is reported.</param>
    public AddressCompletionFanOut(ExplorerChromeOptions options, TimeProvider time, ILogger<AddressCompletionFanOut>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);

        _options = options;
        _time = time;
        _logger = logger ?? NullLogger<AddressCompletionFanOut>.Instance;
    }

    /// <summary>Asks every source in <paramref name="sources"/>, yielding each batch as it arrives.</summary>
    /// <param name="query">The request.</param>
    /// <param name="sources">The sources to ask.</param>
    /// <param name="cancellationToken">Cancelled when the answer is no longer wanted, such as when the user types again.</param>
    public async IAsyncEnumerable<AddressCompletionBatch> CompleteAsync(
        AddressQuery query,
        IReadOnlyList<AddressCompletionSourceEntry> sources,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(sources);

        if (sources.Count == 0)
        {
            yield break;
        }

        var pending = new Task<AddressCompletionBatch>[sources.Count];
        for (var i = 0; i < sources.Count; i++)
        {
            pending[i] = AskAsync(sources[i], query, cancellationToken);
        }

        await foreach (var finished in Task.WhenEach(pending).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return await finished.ConfigureAwait(false);
        }
    }

    private async Task<AddressCompletionBatch> AskAsync(
        AddressCompletionSourceEntry entry,
        AddressQuery query,
        CancellationToken cancellationToken)
    {
        var (outcome, value, error) = await TimeBoxed
            .RunAsync(token => entry.Source.CompleteAsync(query, token), _options.CompletionTimeout, _time, cancellationToken)
            .ConfigureAwait(false);

        switch (outcome)
        {
            case TimeBoxed.Outcome.Completed:
                IReadOnlyList<AddressCompletion> completions = value is null
                    ? []
                    : [.. value.Where(static completion => completion is not null).Take(query.Limit)];
                return new AddressCompletionBatch(entry, completions, AddressCompletionOutcome.Completed);

            case TimeBoxed.Outcome.TimedOut:
                _logger.LogInformation("The {Source} completion source did not answer in time.", entry.Key);
                return new AddressCompletionBatch(entry, [], AddressCompletionOutcome.TimedOut);

            default:
                _logger.LogWarning(error, "The {Source} completion source failed.", entry.Key);
                return new AddressCompletionBatch(entry, [], AddressCompletionOutcome.Failed);
        }
    }
}
