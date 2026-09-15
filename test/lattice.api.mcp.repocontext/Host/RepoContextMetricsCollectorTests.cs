using System.Diagnostics.Metrics;
using System.Globalization;
using System.Reflection;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextMetricsCollector"/>: the container's
/// Prometheus scrape surface. The container image is distroless, so an instrument
/// that is not exposed here is unreadable in the deployment - which is what issue
/// #2363 records, and why these tests assert on delivered VALUES rather than only
/// on a 200 or a non-empty body.
/// </summary>
[TestFixture]
public sealed class RepoContextMetricsCollectorTests
{
    private const string ProbeCommand = "repocontext_probe_call";

    /// <summary>
    /// The positive control. A scrape endpoint that answers 200 with an empty or
    /// partial exposition looks exactly like a working one, so the first thing
    /// proved here is that a KNOWN instrument's KNOWN value reaches the payload.
    /// Every other test in this fixture is only meaningful once this one passes.
    /// </summary>
    [Test]
    public void Render_carries_a_recorded_production_counter_value()
    {
        using var collector = new RepoContextMetricsCollector();

        // A real production recorder, not a synthetic stand-in: it owns the
        // Orleans.Lattice.Api.Mcp.RepoContext meter and the three usage counters.
        using var recorder = new RepoContextUsageRecorder(TimeProvider.System);
        recorder.Record(new RepoContextCallUsage(ProbeCommand, ResponseTokens: 41, ReplacedReadTokens: 907));

        var payload = collector.Render();
        var callsLine = SampleLine(payload, "repocontext_calls_total");
        var tokensLine = SampleLine(payload, "repocontext_response_tokens_total");

        Assert.Multiple(() =>
        {
            Assert.That(callsLine, Is.Not.Null,
                "the exposition carried no repocontext_calls_total sample: " + payload);
            Assert.That(callsLine, Does.Contain($"command=\"{ProbeCommand}\""));
            Assert.That(callsLine, Does.EndWith(" 1"),
                "one recorded call must expose as the value 1, not merely as a present series");
            Assert.That(tokensLine, Does.EndWith(" 41"),
                "the recorded response-token figure must reach the payload unchanged");
        });
    }

    /// <summary>
    /// Definition-of-done criterion 3 for this item is that every instrument on the
    /// repo-context meter is exposed, so the check enumerates them rather than
    /// confirming one and generalising. The names are read by reflection from the
    /// production assembly's own constants, so an instrument added later is covered
    /// without editing this test and a rename cannot leave it asserting a name that
    /// no longer exists.
    /// </summary>
    [Test]
    public void Every_repocontext_instrument_name_is_exposed()
    {
        var names = ProductionInstrumentNames();
        Assert.That(names, Has.Count.GreaterThanOrEqualTo(8),
            "the reflection scan found too few instrument names to be trusted; it has gone vacuous");

        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter(RepoContextUsageRecorder.MeterName);
        foreach (var name in names)
        {
            meter.CreateCounter<long>(name).Add(1);
        }

        var payload = collector.Render();
        Assert.Multiple(() =>
        {
            foreach (var name in names)
            {
                var exposed = RepoContextPrometheusExposition.SanitizeMetricName(name);
                Assert.That(payload, Does.Contain(exposed),
                    $"instrument '{name}' is published on the repo-context meter but absent from the scrape");
            }
        });
    }

    /// <summary>
    /// The core <c>orleans.lattice</c> meter must be in scope too. Three of the
    /// bucket's diagnostics (the leaf checkpoint delta and activation failures in
    /// particular) are published there rather than on the repo-context meter, so a
    /// collector scoped to the repo-context meter alone would leave them exactly as
    /// unreadable as a 404 did.
    /// </summary>
    [Test]
    public void Core_lattice_meter_instruments_are_exposed()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice");
        meter.CreateCounter<long>("orleans.lattice.leaf.deactivation.checkpoint_delta").Add(7);

        var line = SampleLine(collector.Render(), "orleans_lattice_leaf_deactivation_checkpoint_delta_total");
        Assert.That(line, Does.EndWith(" 7"));
    }

    [Test]
    public void Meters_outside_every_subscribed_prefix_are_excluded()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("Contoso.Unrelated");
        meter.CreateCounter<long>("contoso.widgets").Add(5);

        Assert.Multiple(() =>
        {
            Assert.That(collector.Render(), Does.Not.Contain("contoso_widgets"));
            Assert.That(RepoContextMetricsCollector.IsSubscribedMeter("Contoso.Unrelated"), Is.False);
            Assert.That(RepoContextMetricsCollector.IsSubscribedMeter("Orleans.Lattice.Api.Mcp.RepoContext"), Is.True,
                "the repo-context meter is cased differently from the core meter and must still match");
            Assert.That(RepoContextMetricsCollector.IsSubscribedMeter(null), Is.False);
        });
    }

    /// <summary>
    /// Issue #2543: the runtime family is the only in-process account of what this
    /// container's memory is doing, and it is not under the Lattice prefix, so it was
    /// recorded by the runtime and discarded here.
    /// </summary>
    /// <remarks>
    /// This asserts against the REAL <c>System.Runtime</c> meter rather than a
    /// synthetic meter that borrows its name. A synthetic one would prove only that
    /// the predicate matches a string, which is the half of the claim that was never
    /// in doubt; the half that matters is that the base class library actually
    /// publishes these instruments in this process with no package reference, which
    /// is the premise the whole issue rests on. Publication happens synchronously
    /// while the collector's own listener starts, so there is nothing to wait for.
    /// </remarks>
    [Test]
    public void The_dotnet_runtime_family_reaches_the_exposition()
    {
        using var collector = new RepoContextMetricsCollector();

        var payload = collector.Render();

        Assert.Multiple(() =>
        {
            Assert.That(
                SampleLine(payload, "dotnet_process_memory_working_set"), Is.Not.Null,
                "the working set is the instrument that attributes container memory from inside the "
                + "process. Without it, 'how much of the limit are we using' is answerable only by "
                + "docker stats from outside, which carries no attribution and no history.");
            Assert.That(
                SampleLines(payload, "dotnet_gc_last_collection_heap_size"), Is.Not.Empty,
                "managed heap size against working set is the discriminator between managed heap growth "
                + "and unmanaged or mapped memory - the single most valuable reading when a container "
                + "approaches its ceiling.");
            Assert.That(
                SampleLines(payload, "dotnet_gc_collections_total"), Is.Not.Empty,
                "collections partitioned by generation. The host already exported an undifferentiated "
                + "total; a gen2 rate is what separates a healthy allocation-heavy workload from one "
                + "collecting the whole heap repeatedly.");
            Assert.That(
                SampleLines(payload, "dotnet_gc_collections_total")
                    .Any(line => line.Contains("generation=", StringComparison.Ordinal)),
                Is.True,
                "the generation dimension has to survive the label rendering, or the family is the same "
                + "undifferentiated total under a longer name.");
        });
    }

    /// <summary>
    /// Issue #2724. PR #2750 registered this family on the reference-architecture
    /// silos, which collect through OpenTelemetry; this container collects through
    /// its own listener and so did not inherit that fix.
    /// </summary>
    [Test]
    public void The_orleans_runtime_meter_is_subscribed_and_reaches_the_exposition()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter(RepoContextMetricsCollector.OrleansRuntimeMeterNamePrefix);
        meter.CreateCounter<long>("orleans-catalog-activation-latency").Add(3);

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextMetricsCollector.IsSubscribedMeter("Microsoft.Orleans"), Is.True);
            Assert.That(
                SampleLine(collector.Render(), "orleans_catalog_activation_latency_total"),
                Does.EndWith(" 3"),
                "an Orleans runtime series has to reach the payload with its value, not merely match a "
                + "predicate. Activation latency is the signal a survey of this rig concluded did not "
                + "exist, when in fact it was being recorded and discarded one layer above the "
                + "instrument.");
        });
    }

    /// <summary>
    /// The subscription itself has to be readable from the scrape, because a series
    /// discarded at the listener is byte-identical on the endpoint to one that was
    /// never declared.
    /// </summary>
    /// <remarks>
    /// The zero is the load-bearing part. A collector constructed with nothing
    /// published still renders one line per configured prefix, so an ABSENT prefix
    /// means this build does not subscribe to that family - which is exactly what a
    /// pre-fix image looks like - and a prefix at zero means the subscription landed
    /// and nothing published. Without the mint, both states are the same absence.
    /// </remarks>
    [Test]
    public void Every_subscribed_prefix_is_minted_on_the_exposition()
    {
        using var collector = new RepoContextMetricsCollector();

        var lines = SampleLines(collector.Render(), RepoContextMetricsCollector.SubscribedMetersGaugeName);

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextMetricsCollector.SubscribedMeterNamePrefixes,
                Has.Count.EqualTo(3),
                "control: the guard below enumerates the production list, so a list that silently "
                + "emptied would let it pass over nothing.");

            foreach (var prefix in RepoContextMetricsCollector.SubscribedMeterNamePrefixes)
            {
                Assert.That(
                    lines.Any(line => line.Contains($"prefix=\"{prefix}\"", StringComparison.Ordinal)),
                    Is.True,
                    $"prefix '{prefix}' is subscribed in code but names no series, so an operator cannot "
                    + "tell a build that collects it from one that does not.");
            }

            Assert.That(
                lines, Has.Count.EqualTo(RepoContextMetricsCollector.SubscribedMeterNamePrefixes.Count),
                "one line per prefix and no more: a duplicate would double-count and a surplus would "
                + "claim a subscription that is not in the predicate.");
        });
    }

    /// <summary>
    /// The mint is what makes the gauge readable: a prefix that has matched no meter
    /// renders the number zero rather than being omitted.
    /// </summary>
    /// <remarks>
    /// NonParallelizable because a <see cref="MeterListener"/> is process-wide, so a
    /// concurrently running fixture holding a live <c>Microsoft.Orleans</c> meter
    /// would be observed by this collector and turn the zero this test exists to
    /// prove into a one.
    /// </remarks>
    [Test]
    [NonParallelizable]
    public void A_prefix_that_has_matched_nothing_renders_zero_rather_than_being_omitted()
    {
        using var collector = new RepoContextMetricsCollector();

        var lines = SampleLines(collector.Render(), RepoContextMetricsCollector.SubscribedMetersGaugeName);
        var orleansLine = lines.Single(l =>
            l.Contains($"prefix=\"{RepoContextMetricsCollector.OrleansRuntimeMeterNamePrefix}\"", StringComparison.Ordinal));
        var runtimeLine = lines.Single(l =>
            l.Contains($"prefix=\"{RepoContextMetricsCollector.RuntimeMeterNamePrefix}\"", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                orleansLine, Does.EndWith(" 0"),
                "no Orleans runtime meter exists in this test process, and the prefix still renders. "
                + "That zero is the reachability proof: it separates 'this build subscribes to the "
                + "family and nothing published' from 'this build does not subscribe to it', which are "
                + "the same absence without the mint.");
            Assert.That(
                runtimeLine, Does.EndWith(" 1"),
                "control: the base class library publishes exactly one meter named System.Runtime, so "
                + "this arm reads a real count rather than a constant, and the zero above is an "
                + "observation rather than a gauge that reports zero for everything.");
        });
    }

    /// <summary>
    /// An observable instrument reports an absolute value on every poll, so summing
    /// its measurements would multiply it by the number of scrapes. Two renders of a
    /// constant gauge must therefore read the same, not double.
    /// </summary>
    [Test]
    public void Observable_instruments_report_the_polled_value_not_a_running_sum()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice.probe.observable");
        meter.CreateObservableGauge("orleans.lattice.probe.depth", () => 12L);

        _ = collector.Render();
        var line = SampleLine(collector.Render(), "orleans_lattice_probe_depth");

        Assert.That(line, Does.EndWith(" 12"),
            "a gauge polled twice must still read 12; a running sum would read 24");
    }

    [Test]
    public void Histograms_render_as_a_summary_with_sum_and_count()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice.probe.histogram");
        var histogram = meter.CreateHistogram<double>("orleans.lattice.probe.latency");
        histogram.Record(1.5);
        histogram.Record(2.5);

        var payload = collector.Render();
        Assert.Multiple(() =>
        {
            Assert.That(payload, Does.Contain("# TYPE orleans_lattice_probe_latency summary"));
            Assert.That(SampleLine(payload, "orleans_lattice_probe_latency_sum"), Does.EndWith(" 4"));
            Assert.That(SampleLine(payload, "orleans_lattice_probe_latency_count"), Does.EndWith(" 2"));
        });
    }

    /// <summary>
    /// An instrument with no recorded measurement must still announce itself, so a
    /// scrape can be checked for completeness before any traffic has exercised the
    /// instrument. Otherwise "the instrument is missing" and "the instrument has not
    /// fired yet" are indistinguishable from the payload - the exact confusion this
    /// item exists to remove.
    /// </summary>
    [Test]
    public void Published_instruments_announce_themselves_before_any_measurement()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice.probe.silent");
        meter.CreateCounter<long>("orleans.lattice.probe.never_recorded", unit: "{thing}", description: "A silent probe.");

        var payload = collector.Render();
        Assert.Multiple(() =>
        {
            Assert.That(payload, Does.Contain("# TYPE orleans_lattice_probe_never_recorded_total counter"));
            Assert.That(payload, Does.Contain("# HELP orleans_lattice_probe_never_recorded_total A silent probe."));
        });
    }

    [Test]
    public void Series_and_dropped_meta_metrics_are_always_present()
    {
        using var collector = new RepoContextMetricsCollector();
        var payload = collector.Render();

        Assert.Multiple(() =>
        {
            Assert.That(payload, Does.Contain(RepoContextMetricsCollector.SeriesGaugeName));
            Assert.That(payload, Does.Contain(RepoContextMetricsCollector.DroppedCounterName));
        });
    }

    /// <summary>
    /// A ceiling that drops silently leaves an absent series ambiguous between
    /// "never recorded" and "recorded and refused", which is the ambiguity that left
    /// issue #2480 undiagnosed for as long as it was. The attribution counter has to
    /// name both the family that was refused and the ceiling that refused it, so a
    /// single exploding instrument is distinguishable from an estate that has
    /// reached the memory backstop.
    /// </summary>
    [Test]
    public void A_dropped_measurement_is_attributed_to_its_family_and_ceiling()
    {
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 2);
        using var meter = new Meter("orleans.lattice.probe.attribution");
        var counter = meter.CreateCounter<long>("orleans.lattice.probe.attributed");
        for (var i = 0; i < 20; i++)
        {
            counter.Add(1, new KeyValuePair<string, object?>("id", i));
        }

        var payload = collector.Render();
        var attributed = SampleLines(payload, RepoContextMetricsCollector.DroppedByFamilyCounterName)
            .FirstOrDefault(l => l.Contains("orleans_lattice_probe_attributed_total", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(attributed, Is.Not.Null,
                "a refused measurement must be attributed to the family it was refused for: " + payload);
            Assert.That(attributed, Does.Contain(
                $"{RepoContextMetricsCollector.FamilyLabelName}=\"orleans_lattice_probe_attributed_total\""));
            Assert.That(attributed, Does.Contain(
                $"{RepoContextMetricsCollector.CeilingLabelName}=\"{RepoContextMetricsCollector.FamilyCeilingLabel}\""),
                "the per-family ceiling must be named as the one that refused");
            Assert.That(attributed, Does.EndWith(" 18"),
                "the attributed tally must carry the refused count, not merely the label");
        });
    }

    /// <summary>
    /// The negative control for the attribution surface: a collector that has
    /// refused nothing must not emit the family breakdown at all, so a present
    /// series always means a real refusal.
    /// </summary>
    [Test]
    public void No_drop_attribution_is_emitted_when_nothing_was_refused()
    {
        using var collector = new RepoContextMetricsCollector();

        Assert.That(
            SampleLines(collector.Render(), RepoContextMetricsCollector.DroppedByFamilyCounterName),
            Is.Empty);
    }

    /// <summary>
    /// The regression test for issue #2480, which is the reason the ceiling is per
    /// family rather than global.
    /// </summary>
    /// <remarks>
    /// A single global ceiling let one high-cardinality family consume the whole
    /// budget and then permanently block every other family from creating a series.
    /// The failure was silent: a series that already exists keeps updating, because
    /// the lookup precedes the ceiling check, so the exposition still looked busy
    /// and complete. Only a series whose FIRST occurrence fell after saturation was
    /// missing. That is exactly how the ANN search counter lost its
    /// <c>approximate</c> arm - the <c>bootstrapping</c> and <c>exhaustive</c> arms
    /// are created within seconds of start-up and published forever, while
    /// <c>approximate</c> cannot occur until a plane has trained, hours later and
    /// long past saturation. A trained plane was therefore indistinguishable from
    /// one that never armed.
    /// </remarks>
    [Test]
    public void A_saturated_family_does_not_block_a_late_arm_of_another_family()
    {
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 4);
        using var meter = new Meter("orleans.lattice.probe.starvation");

        // A family with an unanticipated high-cardinality tag, saturating its budget.
        var runaway = meter.CreateCounter<long>("orleans.lattice.probe.runaway");
        for (var i = 0; i < 200; i++)
        {
            runaway.Add(1, new KeyValuePair<string, object?>("id", i));
        }

        // A bounded family whose third arm occurs for the FIRST time only after the
        // neighbouring family has saturated, exactly as state="approximate" does.
        var bounded = meter.CreateCounter<long>("orleans.lattice.probe.bounded");
        bounded.Add(1, new KeyValuePair<string, object?>("state", "bootstrapping"));
        bounded.Add(1, new KeyValuePair<string, object?>("state", "exhaustive"));
        bounded.Add(1, new KeyValuePair<string, object?>("state", "approximate"));

        var payload = collector.Render();
        var boundedLines = SampleLines(payload, "orleans_lattice_probe_bounded_total");

        Assert.Multiple(() =>
        {
            // Without a saturated neighbour the test would prove nothing, so prove
            // the neighbour really did saturate before the late arm was recorded.
            Assert.That(SampleLines(payload, "orleans_lattice_probe_runaway_total"), Has.Count.EqualTo(4),
                "the runaway family must be held at its per-family ceiling");
            Assert.That(MetaValue(payload, RepoContextMetricsCollector.DroppedCounterName), Is.GreaterThanOrEqualTo(196),
                "every measurement the runaway family's ceiling refused must be counted");

            Assert.That(boundedLines, Has.Count.EqualTo(3),
                "a bounded family must keep its own budget while a neighbour is saturated: " + payload);
            Assert.That(boundedLines.Any(l => l.Contains("state=\"approximate\"", StringComparison.Ordinal)), Is.True,
                "the late-arriving arm must publish; this is the #2480 regression");
        });
    }

    [Test]
    public void Measurements_beyond_the_per_family_ceiling_are_dropped_and_counted()
    {
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 2);
        using var meter = new Meter("orleans.lattice.probe.cardinality");
        var counter = meter.CreateCounter<long>("orleans.lattice.probe.unbounded");
        for (var i = 0; i < 20; i++)
        {
            counter.Add(1, new KeyValuePair<string, object?>("id", i));
        }

        var payload = collector.Render();

        // Assert on this family's own series rather than the process-wide gauge: the
        // collector replays every instrument already published in the process and
        // polls every observable one on render, so the global figure carries other
        // fixtures' meters and is not this test's to pin.
        Assert.Multiple(() =>
        {
            Assert.That(SampleLines(payload, "orleans_lattice_probe_unbounded_total"), Has.Count.EqualTo(2),
                "the per-family ceiling must be enforced exactly");
            Assert.That(MetaValue(payload, RepoContextMetricsCollector.DroppedCounterName), Is.GreaterThanOrEqualTo(18),
                "every measurement refused by the ceiling must be counted");
        });
    }

    /// <summary>
    /// The global ceiling survives as a memory backstop. It is set far above any
    /// healthy estate precisely so it does not bind in normal operation, but it must
    /// still hold when it is reached.
    /// </summary>
    [Test]
    public void Measurements_beyond_the_global_ceiling_are_dropped_and_counted()
    {
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 1000, maxSeries: 1);
        using var meter = new Meter("orleans.lattice.probe.backstop");
        var counter = meter.CreateCounter<long>("orleans.lattice.probe.global");
        for (var i = 0; i < 20; i++)
        {
            counter.Add(1, new KeyValuePair<string, object?>("id", i));
        }

        var payload = collector.Render();

        Assert.Multiple(() =>
        {
            Assert.That(MetaValue(payload, RepoContextMetricsCollector.SeriesGaugeName), Is.EqualTo(1),
                "the global backstop must be enforced exactly, even below the per-family ceiling");
            Assert.That(MetaValue(payload, RepoContextMetricsCollector.DroppedCounterName), Is.GreaterThanOrEqualTo(19),
                "every measurement refused by the backstop must be counted");
            Assert.That(
                SampleLines(payload, RepoContextMetricsCollector.DroppedByFamilyCounterName)
                    .Any(l => l.Contains(
                        $"{RepoContextMetricsCollector.CeilingLabelName}=\"{RepoContextMetricsCollector.GlobalCeilingLabel}\"",
                        StringComparison.Ordinal)),
                Is.True,
                "a backstop refusal must be attributed to the global ceiling, not the per-family one");
        });
    }

    [Test]
    public void Constructing_with_a_non_positive_per_family_ceiling_is_rejected()
        => Assert.Throws<ArgumentOutOfRangeException>(
            () => new RepoContextMetricsCollector(maxSeriesPerFamily: 0));

    [Test]
    public void Constructing_with_a_non_positive_global_ceiling_is_rejected()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new RepoContextMetricsCollector(maxSeries: 0));

    /// <summary>
    /// The negative control for this fixture's own instrument. Every value
    /// assertion above rests on <see cref="SampleLine"/> finding a line; if the
    /// reader could never return nothing, a passing <c>Is.Not.Null</c> would prove
    /// nothing about the payload. This proves it can.
    /// </summary>
    [Test]
    public void The_sample_reader_returns_nothing_for_a_metric_that_was_never_published()
    {
        using var collector = new RepoContextMetricsCollector();
        var absent = "orleans_lattice_probe_absent_" + Guid.NewGuid().ToString("N");

        Assert.That(SampleLine(collector.Render(), absent), Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var collector = new RepoContextMetricsCollector();
        collector.Dispose();
        Assert.DoesNotThrow(collector.Dispose);
    }

    /// <summary>
    /// Reads every <c>*InstrumentName</c> constant declared by the production
    /// repo-context assembly.
    /// </summary>
    private static IReadOnlyList<string> ProductionInstrumentNames()
    {
        var assembly = typeof(RepoContextUsageRecorder).Assembly;
        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var type in assembly.GetTypes())
        {
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            {
                if (!field.IsLiteral
                    || field.FieldType != typeof(string)
                    || !field.Name.EndsWith("InstrumentName", StringComparison.Ordinal))
                {
                    continue;
                }

                if (field.GetRawConstantValue() is string value && value.Length > 0)
                {
                    names.Add(value);
                }
            }
        }

        return names.ToList();
    }

    /// <summary>
    /// Reads the numeric value of an unlabelled meta-metric sample.
    /// </summary>
    private static long MetaValue(string payload, string metricName)
    {
        var line = SampleLine(payload, metricName);
        Assert.That(line, Is.Not.Null, $"the exposition carried no '{metricName}' sample");
        return long.Parse(line!.AsSpan(metricName.Length).Trim(), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Returns every sample line for a metric name, skipping the <c># HELP</c> and
    /// <c># TYPE</c> comments and any longer name that merely starts with it. Used
    /// to count a single family's series without consulting the process-wide gauge,
    /// which other fixtures' live meters also contribute to.
    /// </summary>
    private static IReadOnlyList<string> SampleLines(string payload, string metricName)
    {
        var matches = new List<string>();
        foreach (var line in payload.Split('\n'))
        {
            if (line.StartsWith('#') || !line.StartsWith(metricName, StringComparison.Ordinal))
            {
                continue;
            }

            var rest = line.AsSpan(metricName.Length);
            if (rest.Length > 0 && (rest[0] == ' ' || rest[0] == '{'))
            {
                matches.Add(line);
            }
        }

        return matches;
    }

    /// <summary>
    /// Returns the first sample line for a metric name, skipping the <c># HELP</c>
    /// and <c># TYPE</c> comments and any longer name that merely starts with it.
    /// </summary>
    private static string? SampleLine(string payload, string metricName)
    {
        foreach (var line in payload.Split('\n'))
        {
            if (line.StartsWith('#') || !line.StartsWith(metricName, StringComparison.Ordinal))
            {
                continue;
            }

            var rest = line.AsSpan(metricName.Length);
            if (rest.Length > 0 && (rest[0] == ' ' || rest[0] == '{'))
            {
                return line;
            }
        }

        return null;
    }
}
