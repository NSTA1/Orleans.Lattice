using Orleans.Runtime;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// What a journey app source has published beyond what it shipped with, kept cluster-wide and
/// durably, so every silo's copy of the source lists the same versions and a silo restart
/// forgets nothing. Keyed by the source key. The harness publishes through the fault seams
/// (<c>POST /e2e/apps/publish</c>), acting as the source's publisher (A5, #5376: "vNext appears").
/// </summary>
internal interface IJourneyCatalogueGrain : IGrainWithStringKey
{
    /// <summary>Whether the source has published its next version.</summary>
    Task<bool> IsNextPublishedAsync();

    /// <summary>Publishes the source's next version. Idempotent.</summary>
    Task PublishNextAsync();
}

/// <summary>The durable state of <see cref="IJourneyCatalogueGrain"/>.</summary>
[GenerateSerializer]
[Alias("journey.silo.catalogue-state")]
internal sealed class JourneyCatalogueState
{
    /// <summary>Whether the next version is published.</summary>
    [Id(0)]
    public bool NextPublished { get; set; }
}

/// <inheritdoc cref="IJourneyCatalogueGrain" />
/// <param name="state">The durable state, in the silo's default grain storage.</param>
internal sealed class JourneyCatalogueGrain(
    [PersistentState("catalogue")] IPersistentState<JourneyCatalogueState> state) : Grain, IJourneyCatalogueGrain
{
    /// <inheritdoc />
    public Task<bool> IsNextPublishedAsync() => Task.FromResult(state.State.NextPublished);

    /// <inheritdoc />
    public async Task PublishNextAsync()
    {
        if (!state.State.NextPublished)
        {
            state.State.NextPublished = true;
            await state.WriteStateAsync();
        }
    }
}
