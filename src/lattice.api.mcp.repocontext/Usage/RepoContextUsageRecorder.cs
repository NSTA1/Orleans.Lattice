using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="IRepoContextUsageRecorder"/>: a bounded in-memory ring of time buckets that
/// answers <c>repocontext_stats</c> without a backing server, and a <see cref="Meter"/> that emits the
/// same figures as telemetry counters for a Prometheus/VictoriaMetrics scraper. The in-memory window is
/// deterministic under an injected <see cref="TimeProvider"/> (so it is unit-testable with no wall clock,
/// timer, or live backend), and the counters carry only a low-cardinality <c>command</c> tag - never a
/// repoId, path, query, or any body text.
/// </summary>
internal sealed class RepoContextUsageRecorder : IRepoContextUsageRecorder, IDisposable
{
    /// <summary>The meter name the usage counters are published under.</summary>
    internal const string MeterName = "Orleans.Lattice.Api.Mcp.RepoContext";

    /// <summary>The counter name for the number of answered calls.</summary>
    internal const string CallsInstrumentName = "repocontext.calls";

    /// <summary>The counter name for the exact response tokens spent.</summary>
    internal const string ResponseTokensInstrumentName = "repocontext.response_tokens";

    /// <summary>The counter name for the whole-file read tokens replaced.</summary>
    internal const string ReadsReplacedInstrumentName = "repocontext.reads_replaced_tokens";

    /// <summary>The low-cardinality tag key the command name is emitted under.</summary>
    internal const string CommandTagKey = "command";

    private const int BucketCount = 60;
    private static readonly TimeSpan DefaultWindow = TimeSpan.FromHours(1);

    private readonly TimeProvider _timeProvider;
    private readonly long _bucketTicks;
    private readonly object _gate = new();
    private readonly long[] _bucketEpoch = new long[BucketCount];
    private readonly long[] _calls = new long[BucketCount];
    private readonly long[] _response = new long[BucketCount];
    private readonly long[] _replaced = new long[BucketCount];

    private readonly Meter _meter;
    private readonly Counter<long> _callsCounter;
    private readonly Counter<long> _responseCounter;
    private readonly Counter<long> _replacedCounter;

    /// <summary>
    /// Creates a recorder whose in-memory window spans one hour of one-minute buckets, driven by the
    /// supplied clock.
    /// </summary>
    /// <param name="timeProvider">The clock used to bucket and expire recorded figures.</param>
    public RepoContextUsageRecorder(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        Window = DefaultWindow;
        _bucketTicks = DefaultWindow.Ticks / BucketCount;

        // Seed every bucket to a sentinel epoch so an unwritten bucket is never counted in the window.
        for (var i = 0; i < BucketCount; i++)
        {
            _bucketEpoch[i] = long.MinValue;
        }

        _meter = new Meter(MeterName);
        _callsCounter = _meter.CreateCounter<long>(
            CallsInstrumentName, unit: "{call}", description: "Answered repocontext calls.");
        _responseCounter = _meter.CreateCounter<long>(
            ResponseTokensInstrumentName, unit: "{token}", description: "Exact response tokens spent by repocontext calls.");
        _replacedCounter = _meter.CreateCounter<long>(
            ReadsReplacedInstrumentName, unit: "{token}", description: "Whole-file read tokens replaced by repocontext calls.");
    }

    /// <inheritdoc />
    public TimeSpan Window { get; }

    /// <inheritdoc />
    public void Record(in RepoContextCallUsage usage)
    {
        var response = usage.ResponseTokens < 0 ? 0 : usage.ResponseTokens;
        var replaced = usage.ReplacedReadTokens < 0 ? 0 : usage.ReplacedReadTokens;
        var epoch = CurrentEpoch();
        var index = (int)(((epoch % BucketCount) + BucketCount) % BucketCount);

        lock (_gate)
        {
            if (_bucketEpoch[index] != epoch)
            {
                _bucketEpoch[index] = epoch;
                _calls[index] = 0;
                _response[index] = 0;
                _replaced[index] = 0;
            }

            _calls[index]++;
            _response[index] += response;
            _replaced[index] += replaced;
        }

        // A single struct tag (string value, no boxing) - no array is allocated on the record path.
        var tag = new KeyValuePair<string, object?>(CommandTagKey, usage.Command);
        _callsCounter.Add(1, tag);
        _responseCounter.Add(response, tag);
        _replacedCounter.Add(replaced, tag);
    }

    /// <inheritdoc />
    public RepoContextUsageAggregate Summarize()
    {
        var epoch = CurrentEpoch();
        var minEpoch = epoch - BucketCount + 1;
        long calls = 0, response = 0, replaced = 0;

        lock (_gate)
        {
            for (var i = 0; i < BucketCount; i++)
            {
                var bucketEpoch = _bucketEpoch[i];
                if (bucketEpoch >= minEpoch && bucketEpoch <= epoch)
                {
                    calls += _calls[i];
                    response += _response[i];
                    replaced += _replaced[i];
                }
            }
        }

        return new RepoContextUsageAggregate(calls, response, replaced);
    }

    private long CurrentEpoch() => _timeProvider.GetUtcNow().UtcTicks / _bucketTicks;

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
