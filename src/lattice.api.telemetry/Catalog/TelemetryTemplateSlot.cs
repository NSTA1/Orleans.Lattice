namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The kind of value a compiled query template substitutes at one of its slots.
/// Both values are server-decided: neither carries text a caller authored.
/// </summary>
internal enum TelemetryTemplateSlot
{
    /// <summary>
    /// The tenant (and optional tree) label matchers the facade derived from the
    /// authenticated caller.
    /// </summary>
    Scope = 0,

    /// <summary>
    /// The rate window the facade derived from the clamped resolution step, for
    /// example <c>300s</c>.
    /// </summary>
    Window = 1,
}
