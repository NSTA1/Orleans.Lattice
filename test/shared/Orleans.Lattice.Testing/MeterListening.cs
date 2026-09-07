using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Safe construction of <see cref="MeterListener"/> instances for tests.
/// <para>
/// <see cref="MeterListener.Start"/> replays only the instruments that
/// <em>already exist</em>. Instruments owned by a static metrics class are
/// created by that class's type initialiser, so a listener whose
/// <see cref="MeterListener.InstrumentPublished"/> callback contains the
/// first reference in the process to that class triggers the initialiser
/// <em>during instrument publication itself</em>. The callback then runs
/// re-entrantly against a partially initialised type: a static instrument
/// field it compares against is still <c>null</c>, the instrument is never
/// enabled, and the listener captures <b>zero measurements while throwing
/// nothing</b>. The resulting assertion failure reads as a product defect
/// ("expected 1, got 0") and is order-dependent, so it presents as a flake
/// that points at the wrong file.
/// </para>
/// <para>
/// Every method here takes the <see cref="Meter"/> or <see cref="Instrument"/>
/// it should listen to as a <b>parameter</b>. C# evaluates that argument at
/// the call site, before the listener exists, so the owning type initialiser
/// has necessarily run to completion by the time
/// <see cref="MeterListener.Start"/> is reached. The unsafe ordering is not
/// expressible through this surface, which is the point: use these helpers
/// rather than hand-rolling a listener, so a new fixture cannot reintroduce
/// the hazard by copying an unsafe neighbour.
/// </para>
/// <para>
/// The measurement callbacks are registered by the caller through
/// <c>configureCallbacks</c>, which runs after the listener is constructed
/// and before it is started, because the callback type
/// (<see cref="MeterListener.SetMeasurementEventCallback{T}"/>) varies per
/// instrument.
/// </para>
/// </summary>
public static class MeterListening
{
    /// <summary>
    /// Starts a listener enabled for every instrument published on
    /// <paramref name="meter"/>.
    /// </summary>
    /// <param name="meter">
    /// The meter to listen to. Reading it at the call site is what forces the
    /// owning type initialiser to complete before the listener is started.
    /// </param>
    /// <param name="configureCallbacks">
    /// Registers the measurement callbacks on the listener. Invoked before
    /// <see cref="MeterListener.Start"/>.
    /// </param>
    /// <returns>The started listener. The caller owns its disposal.</returns>
    public static MeterListener StartForMeter(Meter meter, Action<MeterListener> configureCallbacks)
    {
        ArgumentNullException.ThrowIfNull(meter);
        ArgumentNullException.ThrowIfNull(configureCallbacks);

        return Start(
            (published, listener) =>
            {
                if (ReferenceEquals(published.Meter, meter))
                {
                    listener.EnableMeasurementEvents(published);
                }
            },
            configureCallbacks);
    }

    /// <summary>
    /// Starts a listener enabled for the instruments on
    /// <paramref name="meter"/> whose names appear in
    /// <paramref name="instrumentNames"/>.
    /// </summary>
    /// <param name="meter">
    /// The meter to listen to. Reading it at the call site is what forces the
    /// owning type initialiser to complete before the listener is started.
    /// </param>
    /// <param name="instrumentNames">
    /// The instrument names to enable, compared ordinally. Supply the literal
    /// instrument ids or <c>const</c> name fields; never
    /// <c>SomeMetrics.SomeInstrument.Name</c>, which dereferences a static
    /// instrument field and is the loud sibling of the same hazard.
    /// </param>
    /// <param name="configureCallbacks">
    /// Registers the measurement callbacks on the listener. Invoked before
    /// <see cref="MeterListener.Start"/>.
    /// </param>
    /// <returns>The started listener. The caller owns its disposal.</returns>
    public static MeterListener StartForMeter(
        Meter meter,
        IEnumerable<string> instrumentNames,
        Action<MeterListener> configureCallbacks)
    {
        ArgumentNullException.ThrowIfNull(meter);
        ArgumentNullException.ThrowIfNull(instrumentNames);
        ArgumentNullException.ThrowIfNull(configureCallbacks);

        var names = new HashSet<string>(instrumentNames, StringComparer.Ordinal);

        return Start(
            (published, listener) =>
            {
                if (ReferenceEquals(published.Meter, meter) && names.Contains(published.Name))
                {
                    listener.EnableMeasurementEvents(published);
                }
            },
            configureCallbacks);
    }

    /// <summary>
    /// Starts a listener enabled for exactly <paramref name="instrument"/>,
    /// matched by reference.
    /// </summary>
    /// <param name="instrument">
    /// The instrument to listen to. Reading it at the call site is what forces
    /// the owning type initialiser to complete before the listener is started.
    /// </param>
    /// <param name="configureCallbacks">
    /// Registers the measurement callbacks on the listener. Invoked before
    /// <see cref="MeterListener.Start"/>.
    /// </param>
    /// <returns>The started listener. The caller owns its disposal.</returns>
    public static MeterListener StartForInstrument(
        Instrument instrument,
        Action<MeterListener> configureCallbacks)
    {
        ArgumentNullException.ThrowIfNull(instrument);
        ArgumentNullException.ThrowIfNull(configureCallbacks);

        return Start(
            (published, listener) =>
            {
                if (ReferenceEquals(published, instrument))
                {
                    listener.EnableMeasurementEvents(published);
                }
            },
            configureCallbacks);
    }

    private static MeterListener Start(
        Action<Instrument, MeterListener> instrumentPublished,
        Action<MeterListener> configureCallbacks)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (published, l) => instrumentPublished(published, l),
        };

        configureCallbacks(listener);
        listener.Start();
        return listener;
    }
}
