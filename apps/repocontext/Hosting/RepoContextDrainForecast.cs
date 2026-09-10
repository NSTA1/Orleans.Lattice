namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// What the last recorded drain says about the budget this process derived.
/// </summary>
public enum RepoContextDrainForecastVerdict
{
    /// <summary>No readable record of a previous drain: this is a first start, or the record was lost.</summary>
    NoHistory = 0,

    /// <summary>The last drain finished with real headroom against this budget.</summary>
    Fits = 1,

    /// <summary>The last drain finished, but consumed enough of this budget that the next growth may not.</summary>
    Thin = 2,

    /// <summary>The last drain reached this budget and was abandoned. The next stop is expected to be abandoned too.</summary>
    Exceeded = 3,

    /// <summary>
    /// The last drain began and recorded no terminal outcome, so the process was
    /// killed mid-drain: the container's real grace period is smaller than the drain
    /// needed, whatever the process was told.
    /// </summary>
    KilledMidDrain = 4,
}

/// <summary>
/// Relates the budget this process derived to the drain the previous process
/// actually measured, so the mismatch is reported <b>before</b> the next stop rather
/// than discovered during it.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is the answer to issue #2598, and it is deliberately not a bigger
/// budget.</b> The budget cannot be made a function of the work: it is bounded from
/// outside by the container's <c>stop_grace_period</c>, which the process cannot
/// observe and cannot exceed, and a budget raised past that grant is strictly worse
/// than leaving it alone because it arms the drain-abandoned alarm for an instant
/// the process never lives to reach. <see cref="RepoContextShutdownBudget"/> sets
/// that argument out in full. What was actually missing is not a larger number but
/// a <b>comparison</b>: the host knew the budget at startup and the drain duration
/// at failure, and never put the two in the same sentence.
/// </para>
/// <para>
/// <b>The killed-mid-drain verdict is the one genuinely new observation here.</b>
/// The premise elsewhere in this component is that the real grace period is
/// unobservable from inside the container, and that is true <i>within</i> a process:
/// nothing in the container tells it what Docker will do. It is not true
/// <i>across</i> a restart. A drain that recorded its start and never recorded a
/// terminal outcome is a process that was killed while draining, which is direct
/// evidence that the real grace period is smaller than the drain required - the
/// exact condition the declaration is supposed to prevent, detected from the
/// consequence rather than from the setting.
/// </para>
/// </remarks>
/// <param name="Verdict">The relationship between the last drain and this budget.</param>
/// <param name="Budget">The budget this process derived.</param>
/// <param name="Last">The last recorded drain, when there was one.</param>
/// <param name="ConsumedFraction">
/// How much of this budget the last drain consumed, when a duration was recorded.
/// Computed against <b>this</b> budget rather than the one in force at the time, so
/// the answer is about the stop that is coming rather than the one that has gone.
/// </param>
/// <param name="RequiredStopGracePeriod">
/// The smallest container grant whose derived budget would have covered the last
/// drain, when the last drain did not fit. This is the number to declare, and it is
/// derived from a measurement rather than chosen.
/// </param>
public readonly record struct RepoContextDrainForecast(
    RepoContextDrainForecastVerdict Verdict,
    TimeSpan Budget,
    RepoContextDrainObservation? Last,
    double? ConsumedFraction,
    TimeSpan? RequiredStopGracePeriod)
{
    /// <summary>
    /// Whether the forecast is one an operator has to act on: the last drain either
    /// did not fit this budget, or did not fit the container's real grace period.
    /// </summary>
    public bool IsFailing => Verdict is RepoContextDrainForecastVerdict.Exceeded
        or RepoContextDrainForecastVerdict.KilledMidDrain;

    /// <summary>
    /// The measured cost per resident activation carried by the last drain, when it
    /// recorded both a duration and the residency it was measured against.
    /// </summary>
    public TimeSpan? PerActivationCost => Last?.PerActivationCost;

    /// <summary>
    /// Evaluates the forecast for a derived budget against the last recorded drain.
    /// </summary>
    /// <param name="last">The last recorded drain, or <see langword="null"/>.</param>
    /// <param name="budget">The budget this process derived.</param>
    /// <returns>The forecast.</returns>
    public static RepoContextDrainForecast Evaluate(RepoContextDrainObservation? last, TimeSpan budget)
    {
        if (last is not { } observation)
        {
            return new RepoContextDrainForecast(
                RepoContextDrainForecastVerdict.NoHistory,
                budget,
                Last: null,
                ConsumedFraction: null,
                RequiredStopGracePeriod: null);
        }

        if (observation.Outcome == RepoContextDrainOutcome.Started)
        {
            // No terminal transition was ever recorded, so no duration is available
            // and none can be inferred: the process died before it could measure one.
            // The absence is the finding.
            return new RepoContextDrainForecast(
                RepoContextDrainForecastVerdict.KilledMidDrain,
                budget,
                observation,
                ConsumedFraction: null,
                RequiredStopGracePeriod: null);
        }

        if (observation.Duration is not { } duration)
        {
            // A terminal outcome with no duration is not a shape this component
            // writes. Report it as unknown rather than inventing a comparison.
            return new RepoContextDrainForecast(
                RepoContextDrainForecastVerdict.NoHistory,
                budget,
                observation,
                ConsumedFraction: null,
                RequiredStopGracePeriod: null);
        }

        var consumed = budget > TimeSpan.Zero ? duration.TotalSeconds / budget.TotalSeconds : double.PositiveInfinity;

        // Either the previous host latched an abandonment, or the duration it
        // measured does not fit the budget THIS process derived. The second arm
        // matters on its own: a deployment that lowered its declared grant since the
        // last stop has a drain that fitted then and does not fit now, and nothing
        // else would report that until the stop itself.
        if (observation.Outcome == RepoContextDrainOutcome.Abandoned || duration >= budget)
        {
            return new RepoContextDrainForecast(
                RepoContextDrainForecastVerdict.Exceeded,
                budget,
                observation,
                consumed,
                RepoContextShutdownBudget.RequiredGrantFor(duration));
        }

        return new RepoContextDrainForecast(
            consumed >= RepoContextDrainSignal.HeadroomWarningFraction
                ? RepoContextDrainForecastVerdict.Thin
                : RepoContextDrainForecastVerdict.Fits,
            budget,
            observation,
            consumed,
            RequiredStopGracePeriod: null);
    }

    /// <summary>
    /// Projects the drain the <b>current</b> resident activation set implies, using
    /// the per-activation cost the last drain measured.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the member that turns <see cref="RepoContextDrainSignal"/>'s standing
    /// remark - "drain duration tracks the resident activation set, which nothing
    /// here bounds" - into something an operator can act on while the container is
    /// still running. Nothing here bounds the resident set either; what it does is
    /// make the consequence of that set <b>visible before the stop</b>, which is the
    /// part that was missing.
    /// </para>
    /// <para>
    /// It returns nothing until a drain has been measured alongside its residency,
    /// which is honest: with no measurement the only available projection would be a
    /// per-activation constant somebody guessed, and a guessed constant multiplied by
    /// a real count reads as a measurement while being none.
    /// </para>
    /// </remarks>
    /// <param name="residentActivations">The activation count resident now.</param>
    /// <param name="projection">The projected drain and how it compares with the budget.</param>
    /// <returns><see langword="true"/> when a projection could be made.</returns>
    public bool TryProject(int residentActivations, out RepoContextDrainProjection projection)
    {
        projection = default;
        if (residentActivations < 0 || PerActivationCost is not { } cost || Budget <= TimeSpan.Zero)
        {
            return false;
        }

        var projected = cost * residentActivations;
        projection = new RepoContextDrainProjection(
            residentActivations,
            projected,
            Budget,
            projected.TotalSeconds / Budget.TotalSeconds);
        return true;
    }
}

/// <summary>
/// The drain the current resident activation set implies, against the budget in
/// force.
/// </summary>
/// <param name="ResidentActivations">The activation count the projection was made from.</param>
/// <param name="ProjectedDrain">The projected drain duration.</param>
/// <param name="Budget">The budget the projection is compared against.</param>
/// <param name="ConsumedFraction">The fraction of the budget the projection consumes.</param>
public readonly record struct RepoContextDrainProjection(
    int ResidentActivations,
    TimeSpan ProjectedDrain,
    TimeSpan Budget,
    double ConsumedFraction)
{
    /// <summary>Whether the projected drain does not fit the budget.</summary>
    public bool ExceedsBudget => ProjectedDrain >= Budget;

    /// <summary>
    /// Whether the projected drain fits but leaves less headroom than
    /// <see cref="RepoContextDrainSignal.HeadroomWarningFraction"/>.
    /// </summary>
    public bool IsThin => !ExceedsBudget && ConsumedFraction >= RepoContextDrainSignal.HeadroomWarningFraction;

    /// <summary>
    /// The smallest container grant whose derived budget would cover the projected
    /// drain.
    /// </summary>
    public TimeSpan RequiredStopGracePeriod => RepoContextShutdownBudget.RequiredGrantFor(ProjectedDrain);
}
