using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Scrapes one observable gauge for one tagged series and resolves it to a
/// single value, failing loudly when the scrape reports more than one.
/// <para>
/// The distinction a 0/1 depth gauge exists to make - a <b>measured</b> zero
/// against a <b>never measured</b> absence - is destroyed by a reader that
/// collapses several measurements into one. The obvious reader shape,
/// <c>found = value</c> inside the measurement callback, is last-write-wins:
/// when a gauge's callback emits both a <c>1</c> and a <c>0</c> for the same
/// series in one scrape it silently keeps whichever arrived last, so a
/// contradiction in the instrument is reported as a confident reading.
/// </para>
/// <para>
/// That is not hypothetical. <c>storage.usage_deep_published</c> unioned its
/// per-tree entries across every live sink instance, and a <c>TestCluster</c>
/// hosts several silos - hence several sinks - in one process, so one tree
/// could emit a <c>1</c> from the sink that took the deep measurement and a
/// <c>0</c> from the sink the background poller had seeded. A last-write-wins
/// reader resolved that by callback iteration order and surfaced it as an
/// intermittent <c>Expected: 1, But was: 0</c> on metrics pull requests, where
/// the diff under review is the natural suspect (issue #3004).
/// </para>
/// <para>
/// Failing loudly instead is what makes the producer defect <i>observable</i>.
/// Without it, a green run after a determinism fix cannot be told apart from a
/// green run that won the race - which is the same
/// absence-that-cannot-be-interpreted this repository keeps paying for.
/// </para>
/// <para>
/// This library deliberately references no product assembly, so the meter, the
/// instrument name, and the tag are all parameters. Passing the
/// <see cref="Meter"/> at the call site also forces the owning metrics type's
/// initialiser to complete before the listener starts, per
/// <see cref="MeterListening"/>.
/// </para>
/// </summary>
public static class GaugeScrape
{
    /// <summary>
    /// Records the observable instruments on <paramref name="meter"/> and
    /// returns the single value reported for the series tagged
    /// <paramref name="tagKey"/>=<paramref name="tagValue"/> on the instrument
    /// named <paramref name="instrumentName"/>.
    /// </summary>
    /// <param name="meter">
    /// The meter owning the instrument. Read at the call site, which is what
    /// forces the owning type initialiser to complete before the listener
    /// starts.
    /// </param>
    /// <param name="instrumentName">The observable instrument's name.</param>
    /// <param name="tagKey">The tag key identifying the series.</param>
    /// <param name="tagValue">The tag value identifying the series.</param>
    /// <returns>
    /// The reported value, or <see langword="null"/> when the gauge reported no
    /// measurement at all for that series - the distinction that separates
    /// "never measured" from "measured and zero".
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The scrape reported more than one measurement for the series. See
    /// <see cref="ResolveSingle"/>.
    /// </exception>
    public static long? ReadSingle(Meter meter, string instrumentName, string tagKey, string tagValue)
    {
        ArgumentNullException.ThrowIfNull(meter);
        ArgumentNullException.ThrowIfNull(instrumentName);
        ArgumentNullException.ThrowIfNull(tagKey);
        ArgumentNullException.ThrowIfNull(tagValue);

        var observed = new List<long>();
        using (var listener = MeterListening.StartForMeter(
                   meter,
                   new[] { instrumentName },
                   l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                   {
                       foreach (var tag in tags)
                       {
                           if (string.Equals(tag.Key, tagKey, StringComparison.Ordinal)
                               && tag.Value is string actual
                               && string.Equals(actual, tagValue, StringComparison.Ordinal))
                           {
                               observed.Add(value);
                           }
                       }
                   })))
        {
            listener.RecordObservableInstruments();
        }

        return ResolveSingle(instrumentName, tagKey, tagValue, observed);
    }

    /// <summary>
    /// Resolves the measurements a single scrape reported for one series:
    /// none to <see langword="null"/>, one to that value, and more than one to
    /// an <see cref="InvalidOperationException"/> naming every value observed.
    /// <para>
    /// Separated from <see cref="ReadSingle"/> so the loud-failure behaviour
    /// can be exercised directly, rather than only when a producer happens to
    /// misbehave. A control whose own liveness rests on an absence is the
    /// defect it is meant to catch.
    /// </para>
    /// </summary>
    /// <param name="instrumentName">The instrument name, for the message.</param>
    /// <param name="tagKey">The tag key identifying the series.</param>
    /// <param name="tagValue">The tag value identifying the series.</param>
    /// <param name="observed">The values the scrape reported for the series.</param>
    /// <returns>The single reported value, or <see langword="null"/> for none.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="observed"/> holds more than one value, so the series
    /// carries a contradiction that no single reading can represent.
    /// </exception>
    public static long? ResolveSingle(
        string instrumentName,
        string tagKey,
        string tagValue,
        IReadOnlyList<long> observed)
    {
        ArgumentNullException.ThrowIfNull(instrumentName);
        ArgumentNullException.ThrowIfNull(tagKey);
        ArgumentNullException.ThrowIfNull(tagValue);
        ArgumentNullException.ThrowIfNull(observed);

        if (observed.Count == 0)
        {
            return null;
        }

        if (observed.Count == 1)
        {
            return observed[0];
        }

        throw new InvalidOperationException(
            $"Gauge '{instrumentName}' reported {observed.Count} measurements for "
            + $"{tagKey}='{tagValue}' in a single scrape: [{string.Join(", ", observed)}]. "
            + "One series must resolve to one value; collapsing these would let callback "
            + "iteration order decide the exported reading, which is how a contradiction "
            + "between producers is reported as a confident measurement (issue #3004).");
    }
}
