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

    /// <summary>
    /// The last drain was abandoned under a <b>smaller</b> budget than this process
    /// derived, and the duration it reached fits this one. What a complete drain
    /// costs here is therefore still unmeasured: the recorded duration is a floor,
    /// not a measurement, because the drain was cut short before it finished.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the verdict an operator sees immediately after raising the grant, and
    /// it exists because neither of the two verdicts that could otherwise be reported
    /// is true. <see cref="Exceeded"/> is false - it was the defect of issue #3305,
    /// which replayed the previous process's abandonment against this process's
    /// budget and so announced "does not fit" beside a percentage of 51%. But
    /// <see cref="Fits"/> would be false too, and would be the same class of error in
    /// the opposite direction: an abandoned drain never ran to completion, so
    /// reporting that it "took" its truncated duration and fits would be a confident
    /// value this component is not entitled to.
    /// </para>
    /// <para>
    /// It clears itself on the first stop that drains cleanly, which then records a
    /// real measurement for the next start to compare against.
    /// </para>
    /// </remarks>
    Unproven = 5,
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
/// drain, when the last drain either did not fit or was cut short. This is the
/// number to declare, and it is derived from a measurement rather than chosen. On
/// <see cref="RepoContextDrainForecastVerdict.Unproven"/> the measurement it inverts
/// is itself a floor, so the result is a floor too, and the report says so rather
/// than presenting it as the requirement.
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
    /// <remarks>
    /// <see cref="RepoContextDrainForecastVerdict.Unproven"/> is deliberately
    /// excluded. It reports that this budget is <b>untested</b>, not that it is
    /// expected to fail, and the whole point of issue #3305 was that an operator who
    /// had just performed the remediation was still being told the next stop would be
    /// abandoned. A signal that cannot be cleared by doing what it asks trains people
    /// to ignore it, so the failing set stays exactly the two verdicts that predict a
    /// failure.
    /// </remarks>
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

        // The verdict is computed from THIS budget and nothing else, which is the fix
        // for issue #3305. It previously also fired on a recorded Abandoned outcome,
        // unconditionally - and that outcome is a fact about the budget in force when
        // the drain was recorded, not about this one. A deployment that raised its
        // grant therefore got "does not fit this process's 180s budget (51% of it)":
        // a boolean rendered against the previous budget beside a percentage rendered
        // against the current one, both individually correct. Deriving the verdict
        // from `consumed` alone makes that divergence unrepresentable, and
        // RepoContextDrainForecastTests pins the resulting equivalence
        // (Exceeded <=> consumed >= 1) so it cannot be reintroduced.
        if (duration >= budget)
        {
            return new RepoContextDrainForecast(
                RepoContextDrainForecastVerdict.Exceeded,
                budget,
                observation,
                consumed,
                RepoContextShutdownBudget.RequiredGrantFor(duration));
        }

        if (observation.Outcome == RepoContextDrainOutcome.Abandoned)
        {
            // The drain fits this budget on the evidence available, but that evidence
            // is a truncated drain: it was cut short, so its duration is a LOWER BOUND
            // on what a complete drain costs and not a measurement of one. Reporting
            // it as Fits would replace #3305's false alarm with a false all-clear.
            //
            // The required grant is still carried, and is still derived from the
            // measurement rather than guessed - it is just a floor on a floor, which
            // the report says in as many words. It is always below the grant already
            // declared here (the duration fits a budget derived from that grant), so
            // it reads as confirmation rather than as an instruction to reduce.
            return new RepoContextDrainForecast(
                RepoContextDrainForecastVerdict.Unproven,
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
