namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The severity class names the telemetry panels render notices and captions
/// with, matching the shared status vocabulary the other plugin stylesheets
/// already use.
/// </summary>
/// <remarks>
/// Constants rather than an enum projected through a switch at render time: a
/// panel re-renders on every refresh, and handing out an interned literal costs
/// nothing where composing one would allocate per notice.
/// </remarks>
public static class TelemetrySeverity
{
    /// <summary>An outcome the caller cannot act on by retrying: a refusal of the request.</summary>
    public const string Refused = "is-refused";

    /// <summary>A denial of the caller rather than of the request.</summary>
    public const string Denied = "is-denied";

    /// <summary>A transient fault, or a qualification on an otherwise good answer.</summary>
    public const string Warn = "is-warn";

    /// <summary>An absence rather than a fault: there is simply nothing here.</summary>
    public const string Muted = "is-muted";

    /// <summary>An outcome that went as asked.</summary>
    public const string Ok = "is-ok";
}
