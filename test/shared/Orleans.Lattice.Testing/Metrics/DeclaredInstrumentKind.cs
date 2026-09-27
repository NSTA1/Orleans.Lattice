namespace Orleans.Lattice.Testing.Metrics;

/// <summary>
/// The <c>Meter.Create*</c> factory that declares an instrument, as read from
/// source by <see cref="DeclaredInstruments"/>.
/// </summary>
public enum DeclaredInstrumentKind
{
    /// <summary>A <c>Counter&lt;T&gt;</c>.</summary>
    Counter,

    /// <summary>An <c>UpDownCounter&lt;T&gt;</c>.</summary>
    UpDownCounter,

    /// <summary>A <c>Histogram&lt;T&gt;</c> - the only kind that exports bucket series.</summary>
    Histogram,

    /// <summary>An <c>ObservableGauge&lt;T&gt;</c>.</summary>
    ObservableGauge,

    /// <summary>An <c>ObservableCounter&lt;T&gt;</c>.</summary>
    ObservableCounter,

    /// <summary>An <c>ObservableUpDownCounter&lt;T&gt;</c>.</summary>
    ObservableUpDownCounter,
}
