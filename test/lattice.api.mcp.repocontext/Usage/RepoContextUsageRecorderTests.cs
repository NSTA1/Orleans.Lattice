using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Usage;

/// <summary>
/// Unit tests for <see cref="RepoContextUsageRecorder"/>: the in-memory bounded-window aggregate and
/// the telemetry counter emission. The window is driven by an injected <see cref="TimeProvider"/>, so
/// every assertion is deterministic with no wall clock, timer, or <c>Task.Delay</c>, and the meter is
/// observed with a <see cref="MeterListener"/> that requires no live backend.
/// </summary>
[TestFixture]
public sealed class RepoContextUsageRecorderTests
{
    private static RepoContextCallUsage Usage(int response, int replaced)
        => new("repocontext_context", response, replaced);

    [Test]
    public void Ctor_null_time_provider_throws()
        => Assert.That(() => new RepoContextUsageRecorder(null!), Throws.ArgumentNullException);

    [Test]
    public void Window_is_one_hour()
    {
        using var recorder = new RepoContextUsageRecorder(new SettableTimeProvider());
        Assert.That(recorder.Window, Is.EqualTo(TimeSpan.FromHours(1)));
    }

    [Test]
    public void Summarize_on_a_fresh_recorder_is_empty()
    {
        using var recorder = new RepoContextUsageRecorder(new SettableTimeProvider());
        Assert.That(recorder.Summarize(), Is.EqualTo(new RepoContextUsageAggregate(0, 0, 0)));
    }

    [Test]
    public void Record_then_summarize_sums_calls_and_tokens()
    {
        using var recorder = new RepoContextUsageRecorder(new SettableTimeProvider());
        recorder.Record(Usage(10, 400));
        recorder.Record(Usage(20, 600));

        var aggregate = recorder.Summarize();
        Assert.Multiple(() =>
        {
            Assert.That(aggregate.Calls, Is.EqualTo(2));
            Assert.That(aggregate.ResponseTokens, Is.EqualTo(30));
            Assert.That(aggregate.ReadsReplacedTokens, Is.EqualTo(1000));
            Assert.That(aggregate.NetSavedTokens, Is.EqualTo(970));
        });
    }

    [Test]
    public void Record_clamps_negative_figures_to_zero()
    {
        using var recorder = new RepoContextUsageRecorder(new SettableTimeProvider());
        recorder.Record(Usage(-5, -7));

        var aggregate = recorder.Summarize();
        Assert.Multiple(() =>
        {
            Assert.That(aggregate.Calls, Is.EqualTo(1));
            Assert.That(aggregate.ResponseTokens, Is.Zero);
            Assert.That(aggregate.ReadsReplacedTokens, Is.Zero);
        });
    }

    [Test]
    public void Summarize_counts_figures_recorded_across_several_buckets_within_the_window()
    {
        var clock = new SettableTimeProvider { UtcNow = DateTimeOffset.UnixEpoch };
        using var recorder = new RepoContextUsageRecorder(clock);

        recorder.Record(Usage(10, 100));
        clock.Advance(TimeSpan.FromMinutes(5));
        recorder.Record(Usage(20, 200));
        clock.Advance(TimeSpan.FromMinutes(50));
        recorder.Record(Usage(30, 300));

        var aggregate = recorder.Summarize();
        Assert.Multiple(() =>
        {
            Assert.That(aggregate.Calls, Is.EqualTo(3), "All three fall inside the one-hour window.");
            Assert.That(aggregate.ResponseTokens, Is.EqualTo(60));
            Assert.That(aggregate.ReadsReplacedTokens, Is.EqualTo(600));
        });
    }

    [Test]
    public void Summarize_drops_figures_older_than_the_window()
    {
        var clock = new SettableTimeProvider { UtcNow = DateTimeOffset.UnixEpoch };
        using var recorder = new RepoContextUsageRecorder(clock);

        recorder.Record(Usage(10, 100));
        // Advance beyond the full one-hour window so the first record has aged out.
        clock.Advance(TimeSpan.FromMinutes(61));
        recorder.Record(Usage(20, 200));

        var aggregate = recorder.Summarize();
        Assert.Multiple(() =>
        {
            Assert.That(aggregate.Calls, Is.EqualTo(1), "The aged-out record is no longer counted.");
            Assert.That(aggregate.ResponseTokens, Is.EqualTo(20));
            Assert.That(aggregate.ReadsReplacedTokens, Is.EqualTo(200));
        });
    }

    [Test]
    public void Summarize_is_empty_once_every_record_has_aged_out()
    {
        var clock = new SettableTimeProvider { UtcNow = DateTimeOffset.UnixEpoch };
        using var recorder = new RepoContextUsageRecorder(clock);

        recorder.Record(Usage(10, 100));
        clock.Advance(TimeSpan.FromHours(2));

        Assert.That(recorder.Summarize(), Is.EqualTo(new RepoContextUsageAggregate(0, 0, 0)));
    }

    [Test]
    public void Record_emits_the_figures_as_telemetry_counters_tagged_with_the_command()
    {
        using var recorder = new RepoContextUsageRecorder(new SettableTimeProvider());

        long calls = 0, response = 0, replaced = 0;
        string? commandTag = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextUsageRecorder.CommandTagKey)
                {
                    commandTag = tag.Value as string;
                }
            }

            switch (instrument.Name)
            {
                case RepoContextUsageRecorder.CallsInstrumentName:
                    calls += measurement;
                    break;
                case RepoContextUsageRecorder.ResponseTokensInstrumentName:
                    response += measurement;
                    break;
                case RepoContextUsageRecorder.ReadsReplacedInstrumentName:
                    replaced += measurement;
                    break;
            }
        });
        listener.Start();

        recorder.Record(Usage(15, 500));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(1));
            Assert.That(response, Is.EqualTo(15));
            Assert.That(replaced, Is.EqualTo(500));
            Assert.That(commandTag, Is.EqualTo("repocontext_context"));
        });
    }
}
