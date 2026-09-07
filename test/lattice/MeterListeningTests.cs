using System.Diagnostics.Metrics;

using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Controls for the <see cref="MeterListening"/> helper and for the
/// <see cref="MeterListener"/> static-initialisation hazard it exists to
/// remove (issue #2169).
/// <para>
/// Each arm below owns a <b>private probe metrics class that no other test
/// touches</b>. That is deliberate and load-bearing: the hazard is precisely
/// that it disappears once something else in the process has initialised the
/// metrics class, so a control sharing a metrics class with a sibling test
/// would be silently order-dependent - the very defect under test. One
/// untouched probe class per arm makes every assertion here deterministic
/// regardless of the order NUnit picks.
/// </para>
/// </summary>
[TestFixture]
public class MeterListeningTests
{
    // ---- controls: the hazard is live on this runtime --------------------

    /// <summary>
    /// The control. A hand-rolled listener whose <c>InstrumentPublished</c>
    /// callback holds the first process reference to its metrics class
    /// captures nothing and throws nothing; the same listener built through
    /// the helper captures the measurement.
    /// </summary>
    [Test]
    public void StartForInstrument_captures_a_measurement_the_hand_rolled_listener_silently_drops()
    {
        var handRolledCaptured = 0;
        using (var handRolled = new MeterListener
        {
            InstrumentPublished = (published, l) =>
            {
                // The first reference to UnsafeProbeMetrics in this process is
                // here, inside the callback, so it triggers the type
                // initialiser during instrument publication. The static field
                // is still unassigned when ReferenceEquals reads it.
                if (ReferenceEquals(published, UnsafeProbeMetrics.Counter))
                {
                    l.EnableMeasurementEvents(published);
                }
            },
        })
        {
            handRolled.SetMeasurementEventCallback<long>((_, _, _, _) => Interlocked.Increment(ref handRolledCaptured));
            handRolled.Start();

            UnsafeProbeMetrics.Counter.Add(1);
        }

        var helperCaptured = 0;
        using (MeterListening.StartForInstrument(
            SafeProbeMetrics.Counter,
            l => l.SetMeasurementEventCallback<long>((_, _, _, _) => Interlocked.Increment(ref helperCaptured))))
        {
            SafeProbeMetrics.Counter.Add(1);
        }

        Assert.Multiple(() =>
        {
            Assert.That(handRolledCaptured, Is.Zero,
                "the hand-rolled listener must capture nothing: its InstrumentPublished callback holds the "
                + "first reference to the metrics class, so the static instrument field is still unassigned "
                + "when the callback compares against it and the instrument is never enabled. If this arm "
                + "starts capturing, the runtime has changed the publication ordering and the hazard "
                + "#2169 guards against no longer exists");

            Assert.That(helperCaptured, Is.EqualTo(1),
                "MeterListening.StartForInstrument must capture the measurement: reading the instrument as a "
                + "call-site argument forces the type initialiser to complete before the listener is built, "
                + "so publication cannot race it. This is the arm that fails if the helper is ever rewritten "
                + "to touch the metrics class from inside its own callback");
        });
    }

    /// <summary>
    /// Matching on a static <c>Meter</c> field is only safe because the meter
    /// is declared before every instrument on it. This arm holds the
    /// counter-example: when an instrument is declared above the meter field,
    /// the listener silently drops it.
    /// </summary>
    [Test]
    public void Listener_matching_on_a_Meter_field_declared_after_an_instrument_silently_drops_that_instrument()
    {
        var captured = new List<string>();

        using (var listener = new MeterListener
        {
            InstrumentPublished = (published, l) =>
            {
                if (ReferenceEquals(published.Meter, MeterDeclaredLateProbeMetrics.Meter))
                {
                    l.EnableMeasurementEvents(published);
                }
            },
        })
        {
            listener.SetMeasurementEventCallback<long>((instrument, _, _, _) =>
            {
                lock (captured) captured.Add(instrument.Name);
            });
            listener.Start();

            MeterDeclaredLateProbeMetrics.DeclaredBeforeTheMeter.Add(1);
            MeterDeclaredLateProbeMetrics.DeclaredAfterTheMeter.Add(1);
        }

        Assert.That(captured, Is.EqualTo(new[] { MeterDeclaredLateProbeMetrics.DeclaredAfterTheMeterName }),
            "the instrument declared above the Meter field is published while that field is still unassigned, "
            + "so ReferenceEquals compares against null and the instrument is never enabled - silently, with "
            + "no exception. This is why MeterFieldDeclarationOrderTests pins the Meter field first in every "
            + "production metrics class");
    }

    /// <summary>
    /// The loud sibling of the same hazard: dereferencing a static instrument
    /// field for its <c>Name</c> inside the callback nulls out and poisons the
    /// metrics type initialiser for the rest of the process.
    /// </summary>
    [Test]
    public void Listener_dereferencing_a_static_instrument_field_inside_the_callback_poisons_the_type_initialiser()
    {
        Assert.That(RunNameDereferencingListener, Throws.TypeOf<TypeInitializationException>(),
            "reading NameDereferenceProbeMetrics.Second.Name from inside InstrumentPublished dereferences a "
            + "static field that is still null while an earlier instrument on the same meter is being "
            + "published, so the NullReferenceException escapes the type initialiser and is cached against "
            + "the type for the lifetime of the process");
    }

    private static void RunNameDereferencingListener()
    {
        using var listener = new MeterListener
        {
            InstrumentPublished = (published, l) =>
            {
                // Gate on the meter NAME first. It is a const, so it is inlined
                // and does not trigger the initialiser, which keeps the blast
                // radius of this control inside its own probe meter instead of
                // poisoning a metrics class a sibling fixture is using.
                if (!string.Equals(published.Meter.Name, NameDereferenceProbeMetrics.MeterName, StringComparison.Ordinal))
                {
                    return;
                }

                if (published.Name == NameDereferenceProbeMetrics.Second.Name)
                {
                    l.EnableMeasurementEvents(published);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, _, _, _) => { });
        listener.Start();

        NameDereferenceProbeMetrics.Second.Add(1);
    }

    // ---- helper surface --------------------------------------------------

    [Test]
    public void StartForMeter_captures_every_instrument_on_the_meter()
    {
        var captured = new List<string>();

        using (MeterListening.StartForMeter(
            MeterScopedProbeMetrics.Meter,
            l => l.SetMeasurementEventCallback<long>((instrument, _, _, _) =>
            {
                lock (captured) captured.Add(instrument.Name);
            })))
        {
            MeterScopedProbeMetrics.First.Add(1);
            MeterScopedProbeMetrics.Second.Add(1);
        }

        Assert.That(captured, Is.EquivalentTo(new[]
        {
            MeterScopedProbeMetrics.FirstName,
            MeterScopedProbeMetrics.SecondName,
        }));
    }

    [Test]
    public void StartForMeter_with_instrument_names_captures_only_the_named_instruments()
    {
        var captured = new List<string>();

        using (MeterListening.StartForMeter(
            NameFilteredProbeMetrics.Meter,
            new[] { NameFilteredProbeMetrics.SecondName },
            l => l.SetMeasurementEventCallback<long>((instrument, _, _, _) =>
            {
                lock (captured) captured.Add(instrument.Name);
            })))
        {
            NameFilteredProbeMetrics.First.Add(1);
            NameFilteredProbeMetrics.Second.Add(1);
        }

        Assert.That(captured, Is.EqualTo(new[] { NameFilteredProbeMetrics.SecondName }));
    }

    [Test]
    public void StartForInstrument_ignores_other_instruments_on_the_same_meter()
    {
        var captured = new List<string>();

        using (MeterListening.StartForInstrument(
            InstrumentScopedProbeMetrics.Second,
            l => l.SetMeasurementEventCallback<long>((instrument, _, _, _) =>
            {
                lock (captured) captured.Add(instrument.Name);
            })))
        {
            InstrumentScopedProbeMetrics.First.Add(1);
            InstrumentScopedProbeMetrics.Second.Add(1);
        }

        Assert.That(captured, Is.EqualTo(new[] { InstrumentScopedProbeMetrics.SecondName }));
    }

    [Test]
    public void StartForMeter_rejects_a_null_meter()
        => Assert.That(() => MeterListening.StartForMeter(null!, _ => { }), Throws.ArgumentNullException);

    [Test]
    public void StartForMeter_rejects_null_callbacks()
        => Assert.That(
            () => MeterListening.StartForMeter(NullArgumentProbeMetrics.Meter, null!),
            Throws.ArgumentNullException);

    [Test]
    public void StartForMeter_with_instrument_names_rejects_a_null_name_set()
        => Assert.That(
            () => MeterListening.StartForMeter(NullArgumentProbeMetrics.Meter, null!, _ => { }),
            Throws.ArgumentNullException);

    [Test]
    public void StartForInstrument_rejects_a_null_instrument()
        => Assert.That(
            () => MeterListening.StartForInstrument(null!, _ => { }),
            Throws.ArgumentNullException);

    [Test]
    public void StartForInstrument_rejects_null_callbacks()
        => Assert.That(
            () => MeterListening.StartForInstrument(NullArgumentProbeMetrics.First, null!),
            Throws.ArgumentNullException);
}

// Probe metrics classes. Each is referenced by exactly ONE test above so that
// no arm can initialise another arm's class and mask the hazard. Do not reuse
// one across tests, and do not reference one from production or fixture setup.

file static class UnsafeProbeMetrics
{
    public static readonly Meter Meter = new("orleans.lattice.test.probe.unsafe");
    public static readonly Counter<long> Counter = Meter.CreateCounter<long>("orleans.lattice.test.probe.unsafe.counter");
}

file static class SafeProbeMetrics
{
    public static readonly Meter Meter = new("orleans.lattice.test.probe.safe");
    public static readonly Counter<long> Counter = Meter.CreateCounter<long>("orleans.lattice.test.probe.safe.counter");
}

file static class MeterDeclaredLateProbeMetrics
{
    public const string DeclaredBeforeTheMeterName = "orleans.lattice.test.probe.late.before";
    public const string DeclaredAfterTheMeterName = "orleans.lattice.test.probe.late.after";

    private static readonly Meter Owner = new("orleans.lattice.test.probe.late");

    // Declared ABOVE the Meter field on purpose: this is the shape the
    // declaration-order guard forbids in production metrics classes.
    public static readonly Counter<long> DeclaredBeforeTheMeter = Owner.CreateCounter<long>(DeclaredBeforeTheMeterName);

    public static readonly Meter Meter = Owner;

    public static readonly Counter<long> DeclaredAfterTheMeter = Owner.CreateCounter<long>(DeclaredAfterTheMeterName);
}

file static class NameDereferenceProbeMetrics
{
    public const string MeterName = "orleans.lattice.test.probe.namederef";

    public static readonly Meter Meter = new(MeterName);
    public static readonly Counter<long> First = Meter.CreateCounter<long>("orleans.lattice.test.probe.namederef.first");
    public static readonly Counter<long> Second = Meter.CreateCounter<long>("orleans.lattice.test.probe.namederef.second");
}

file static class MeterScopedProbeMetrics
{
    public const string FirstName = "orleans.lattice.test.probe.meterscoped.first";
    public const string SecondName = "orleans.lattice.test.probe.meterscoped.second";

    public static readonly Meter Meter = new("orleans.lattice.test.probe.meterscoped");
    public static readonly Counter<long> First = Meter.CreateCounter<long>(FirstName);
    public static readonly Counter<long> Second = Meter.CreateCounter<long>(SecondName);
}

file static class NameFilteredProbeMetrics
{
    public const string FirstName = "orleans.lattice.test.probe.namefiltered.first";
    public const string SecondName = "orleans.lattice.test.probe.namefiltered.second";

    public static readonly Meter Meter = new("orleans.lattice.test.probe.namefiltered");
    public static readonly Counter<long> First = Meter.CreateCounter<long>(FirstName);
    public static readonly Counter<long> Second = Meter.CreateCounter<long>(SecondName);
}

file static class InstrumentScopedProbeMetrics
{
    public const string FirstName = "orleans.lattice.test.probe.instrumentscoped.first";
    public const string SecondName = "orleans.lattice.test.probe.instrumentscoped.second";

    public static readonly Meter Meter = new("orleans.lattice.test.probe.instrumentscoped");
    public static readonly Counter<long> First = Meter.CreateCounter<long>(FirstName);
    public static readonly Counter<long> Second = Meter.CreateCounter<long>(SecondName);
}

file static class NullArgumentProbeMetrics
{
    public static readonly Meter Meter = new("orleans.lattice.test.probe.nullargs");
    public static readonly Counter<long> First = Meter.CreateCounter<long>("orleans.lattice.test.probe.nullargs.first");
}
