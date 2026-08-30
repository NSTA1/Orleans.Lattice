namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The evaluation window a panel asks for, and the one the facade reports it
/// actually evaluated after clamping the request against the entry's bounds.
/// </summary>
/// <remarks>
/// <para>
/// <b>An unset window is a real request, not a mistake.</b>
/// <c>default</c> - which <see cref="IsUnset"/> reports - means "you choose",
/// and it is what a panel sends before a user has touched a control. The seam
/// forwards it unset so the <em>facade</em> picks the window and step, because a
/// client that helpfully expands an unset window to an entry's maximum range
/// blows the entry's point budget at its default step and turns every range
/// query into a bounds refusal.
/// </para>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so building one per poll allocates nothing.
/// </para>
/// </remarks>
/// <param name="StartUtc">The inclusive start of the window.</param>
/// <param name="EndUtc">
/// The exclusive end of the window, and - for an
/// <see cref="ExplorerTelemetryQueryKind.Instant"/> entry - the evaluation
/// instant.
/// </param>
/// <param name="Step">
/// The resolution. <see cref="TimeSpan.Zero"/> asks the facade for the entry's
/// declared default step.
/// </param>
public readonly record struct ExplorerTelemetryWindow(
    DateTimeOffset StartUtc,
    DateTimeOffset EndUtc,
    TimeSpan Step)
{
    /// <summary>
    /// The window a panel sends before a user has chosen one: entirely unset, so
    /// the facade applies the entry's own default window and step.
    /// </summary>
    public static ExplorerTelemetryWindow Unset => default;

    /// <summary>Creates an instant window evaluated at <paramref name="instant"/>.</summary>
    /// <param name="instant">The evaluation instant.</param>
    /// <returns>An instant window.</returns>
    public static ExplorerTelemetryWindow At(DateTimeOffset instant) =>
        new(instant, instant, TimeSpan.Zero);

    /// <summary>Creates a range window.</summary>
    /// <param name="startUtc">The inclusive start.</param>
    /// <param name="endUtc">The exclusive end.</param>
    /// <param name="step">The resolution, or <see cref="TimeSpan.Zero"/> for the entry's default.</param>
    /// <returns>A range window.</returns>
    public static ExplorerTelemetryWindow Between(DateTimeOffset startUtc, DateTimeOffset endUtc, TimeSpan step) =>
        new(startUtc, endUtc, step);

    /// <summary>
    /// <see langword="true"/> when nothing has been chosen, so the facade
    /// supplies the window and step. This is the state a defaulted request is in.
    /// </summary>
    public bool IsUnset => StartUtc == default && EndUtc == default && Step == TimeSpan.Zero;

    /// <summary>The window's length.</summary>
    public TimeSpan Duration => EndUtc - StartUtc;

    /// <summary><see langword="true"/> when the window collapses to a single instant.</summary>
    public bool IsInstant => EndUtc == StartUtc;

    /// <summary><see langword="true"/> when the end is at or after the start.</summary>
    public bool IsAscending => EndUtc >= StartUtc;

    /// <summary>
    /// The number of points the window yields at its step: one for an instant or
    /// an unstepped window, and zero for a descending one.
    /// </summary>
    public long PointCount
    {
        get
        {
            if (EndUtc < StartUtc)
            {
                return 0;
            }

            return Step <= TimeSpan.Zero ? 1 : ((EndUtc - StartUtc).Ticks / Step.Ticks) + 1;
        }
    }

    /// <summary>Returns this window at a different step.</summary>
    /// <param name="step">The replacement step.</param>
    /// <returns>The re-stepped window.</returns>
    public ExplorerTelemetryWindow WithStep(TimeSpan step) => this with { Step = step };
}
