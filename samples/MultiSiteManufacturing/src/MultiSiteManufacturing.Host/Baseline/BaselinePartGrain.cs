using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Baseline;

/// <inheritdoc cref="IBaselinePartGrain"/>
internal sealed class BaselinePartGrain(
    [PersistentState(stateName: "state", storageName: "msmfgGrainState")]
    IPersistentState<BaselinePartState> state,
    ILogger<BaselinePartGrain> logger)
    : Grain, IBaselinePartGrain
{
    public async Task AppendAsync(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);

        state.State.FactsInArrivalOrder.Add(fact);

        var (newState, newRetestArmed) = NaiveFold.Step(
            state.State.State,
            state.State.RetestArmed,
            fact);

        state.State.State = newState;
        state.State.RetestArmed = newRetestArmed;

        await state.WriteStateAsync();

        logger.LogDebug(
            "Baseline grain {Serial}: applied {FactType} -> {State}",
            this.GetPrimaryKeyString(), fact.GetType().Name, newState);
    }

    public Task<ComplianceState> GetStateAsync() =>
        Task.FromResult(state.State.State);

    public Task<IReadOnlyList<Fact>> GetFactsAsync() =>
        Task.FromResult<IReadOnlyList<Fact>>(state.State.FactsInArrivalOrder.ToList());
}
