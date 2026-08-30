namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One bounded control choice a panel offers: a duration and the label that
/// stands for it. Used for both the time-range control and the step control,
/// which differ only in the ladder they are filtered from and the bound they
/// are filtered against.
/// </summary>
/// <remarks>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/> over a pre-composed label, so a control that
/// re-renders on every refresh neither allocates a choice nor formats a string.
/// </para>
/// <para>
/// The label is a literal from the ladder rather than something derived from the
/// duration at render time, because a formatted <see cref="TimeSpan"/> reads as
/// <c>00:15:00</c> where a control wants <c>15m</c>. It is also the value the
/// rendered option carries, because the ladder's labels are distinct and a
/// second encoding would be a second thing to keep in step.
/// </para>
/// </remarks>
/// <param name="Duration">The duration the choice selects.</param>
/// <param name="Label">The label the control renders, and the option's value.</param>
public readonly record struct TelemetryDurationChoice(TimeSpan Duration, string Label);
