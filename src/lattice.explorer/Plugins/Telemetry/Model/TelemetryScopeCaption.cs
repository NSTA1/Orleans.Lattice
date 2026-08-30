namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The sentence a panel renders about the scope a result was served under, and
/// how loudly to render it.
/// </summary>
/// <remarks>
/// <para>
/// Composed once per response rather than per render, and every branch that can
/// be a literal is one, so the only interpolation is the branch that has to name
/// a tenant the response carried.
/// </para>
/// </remarks>
/// <param name="Severity">How loudly the caption should be rendered.</param>
/// <param name="Text">The caption itself. Never <see langword="null"/>.</param>
public readonly record struct TelemetryScopeCaption(TelemetryScopeSeverity Severity, string Text)
{
    /// <summary><see langword="true"/> when the caption reports a degraded scope.</summary>
    public bool IsDegraded => Severity == TelemetryScopeSeverity.Degraded;
}
