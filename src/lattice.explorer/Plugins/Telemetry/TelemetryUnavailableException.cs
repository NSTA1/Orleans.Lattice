namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The cluster does not serve a telemetry facade at all - the binding is not
/// registered, or the host serves no telemetry service. Distinct from a denial
/// (the surface exists and refused this caller) and from a backend fault (the
/// surface exists and its metrics backend could not answer).
/// </summary>
/// <remarks>
/// This is the signal the availability probe turns into the four-state access
/// model's unavailable state, so a telemetry surface renders nothing at all on a
/// deployment that has none rather than showing an error a user cannot act on.
/// </remarks>
public sealed class TelemetryUnavailableException : Exception
{
    /// <summary>Creates the exception with the default message.</summary>
    public TelemetryUnavailableException()
        : base("This cluster does not serve telemetry.")
    {
    }

    /// <summary>Creates the exception with an explanation.</summary>
    /// <param name="message">Why the surface is unavailable.</param>
    public TelemetryUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with an explanation and the underlying fault.</summary>
    /// <param name="message">Why the surface is unavailable.</param>
    /// <param name="innerException">The transport fault that revealed it.</param>
    public TelemetryUnavailableException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
