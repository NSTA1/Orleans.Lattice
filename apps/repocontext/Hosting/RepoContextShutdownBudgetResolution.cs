namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The outcome of resolving the host's shutdown budget: the container grant it was
/// derived from, the derived budget, and whether the grant was declared by the
/// deployment or defaulted.
/// </summary>
/// <param name="StopGracePeriod">
/// The container's <c>stop_grace_period</c> as declared to the process. This is the
/// deployment's <b>declaration</b> of the grant, not the grant itself: the real
/// value lives in the container runtime and cannot be observed from inside the
/// process, so a stale declaration is undetectable here.
/// </param>
/// <param name="ShutdownBudget">
/// The budget derived from <paramref name="StopGracePeriod"/>, always strictly less
/// than it so the host can report a cut-short drain before it is killed.
/// </param>
/// <param name="GrantWasDeclared">
/// Whether <see cref="RepoContextShutdownBudget.StopGracePeriodKey"/> was set.
/// Carried so the startup log can distinguish a deployment that stated its grant
/// from one running on the default, which is the difference between a considered
/// value and an unconsidered one.
/// </param>
public readonly record struct RepoContextShutdownBudgetResolution(
    TimeSpan StopGracePeriod,
    TimeSpan ShutdownBudget,
    bool GrantWasDeclared);
