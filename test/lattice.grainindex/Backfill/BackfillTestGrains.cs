using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// The state the backfill integration tests project. It carries Orleans
/// serialization attributes because a real silo persists it through the memory
/// grain-storage provider.
/// </summary>
[GenerateSerializer]
public sealed class BackfillUserState
{
    /// <summary>An ordered value-type property the index projects.</summary>
    [Id(0)] public int Age { get; set; }

    /// <summary>An ordered reference-type property the index projects.</summary>
    [Id(1)] public string Country { get; set; } = string.Empty;
}

/// <summary>
/// The dormant population the backfill onboards. Its state parameter carries
/// <see cref="IndexedAttribute"/>, so it indexes itself the moment it activates
/// - which is exactly what the crawl relies on.
/// </summary>
public interface IBackfillUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's indexed state.</summary>
    /// <param name="age">The age to store.</param>
    /// <param name="country">The country to store.</param>
    /// <returns>A task that completes when the state and its entries are durable.</returns>
    Task SetAsync(int age, string country);

    /// <summary>Reads the stored age, activating the grain if it is dormant.</summary>
    /// <returns>The stored age.</returns>
    Task<int> GetAgeAsync();
}

/// <summary>The grain the backfill integration tests crawl.</summary>
public sealed class BackfillUserGrain(
    [Indexed("backfill-user")] IPersistentState<BackfillUserState> state) : Grain, IBackfillUserGrain
{
    /// <inheritdoc />
    public async Task SetAsync(int age, string country)
    {
        state.State.Age = age;
        state.State.Country = country;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public Task<int> GetAgeAsync() => Task.FromResult(state.State.Age);
}
