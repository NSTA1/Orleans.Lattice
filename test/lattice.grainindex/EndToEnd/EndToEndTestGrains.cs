namespace Orleans.Lattice.GrainIndex.Tests.EndToEnd;

/// <summary>
/// The state name each end-to-end grain's <c>[Indexed]</c> parameter declares.
/// </summary>
/// <remarks>
/// The names are constants rather than literals repeated in two places because
/// the fixture seeds a dormant population by writing straight into the storage
/// provider under the same state name, so a rename that reached only one of the
/// two sites would leave a population the grains cannot see.
/// </remarks>
internal static class EndToEndStateNames
{
    /// <summary>The state name of the grain onboarded by the activation path.</summary>
    internal const string Active = "e2e-active-user";

    /// <summary>The state name of the grain onboarded by the backfill crawl.</summary>
    internal const string Dormant = "e2e-dormant-user";

    /// <summary>The state name of the grain the drift and rebuild tests crawl.</summary>
    internal const string Drift = "e2e-drift-user";
}

/// <summary>
/// The state every end-to-end index projects. It carries Orleans serialization
/// attributes because a real silo persists it through the memory grain-storage
/// provider.
/// </summary>
/// <remarks>
/// <see cref="Nickname"/> is projected by no index the silo declares. It exists
/// so the rebuild test can build an index under an <i>older</i> declaration that
/// did project it, and then prove that the rebuilt index no longer holds those
/// entries.
/// </remarks>
[GenerateSerializer]
public sealed class EndToEndUserState
{
    /// <summary>An ordered value-type property every declaration projects.</summary>
    [Id(0)] public int Age { get; set; }

    /// <summary>An ordered reference-type property the current declarations project.</summary>
    [Id(1)] public string Country { get; set; } = string.Empty;

    /// <summary>A property only the rebuild test's superseded declaration projects.</summary>
    [Id(2)] public string Nickname { get; set; } = string.Empty;
}

/// <summary>One member of an end-to-end population: its grain key and its state.</summary>
/// <param name="Key">The grain's string key, which is also its encoded index key.</param>
/// <param name="State">The state the grain stores.</param>
public readonly record struct EndToEndPerson(string Key, EndToEndUserState State);

/// <summary>
/// A grain whose state is written through its own interface, so it is onboarded
/// entirely by the activation path and never by a crawl.
/// </summary>
public interface IEndToEndActiveUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's indexed state.</summary>
    /// <param name="age">The age to store.</param>
    /// <param name="country">The country to store.</param>
    /// <returns>A task that completes when the state and its entries are durable.</returns>
    Task SetAsync(int age, string country);
}

/// <summary>The grain the active-path convergence tests write through.</summary>
public sealed class EndToEndActiveUserGrain(
    [Indexed(EndToEndStateNames.Active)] IPersistentState<EndToEndUserState> state)
    : Grain, IEndToEndActiveUserGrain
{
    /// <inheritdoc />
    public async Task SetAsync(int age, string country)
    {
        state.State.Age = age;
        state.State.Country = country;
        await state.WriteStateAsync();
    }
}

/// <summary>
/// A grain seeded straight into storage and therefore never activated, so the
/// only thing that can onboard it is the background backfill crawl.
/// </summary>
public interface IEndToEndDormantUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's indexed state.</summary>
    /// <param name="age">The age to store.</param>
    /// <param name="country">The country to store.</param>
    /// <returns>A task that completes when the state and its entries are durable.</returns>
    Task SetAsync(int age, string country);
}

/// <summary>The grain the backfill and churn convergence tests crawl.</summary>
public sealed class EndToEndDormantUserGrain(
    [Indexed(EndToEndStateNames.Dormant)] IPersistentState<EndToEndUserState> state)
    : Grain, IEndToEndDormantUserGrain
{
    /// <inheritdoc />
    public async Task SetAsync(int age, string country)
    {
        state.State.Age = age;
        state.State.Country = country;
        await state.WriteStateAsync();
    }
}

/// <summary>
/// The grain the drift-rejection and rebuild tests index. It is separate from
/// the convergence grains so that rewriting its index's registry record cannot
/// disturb them.
/// </summary>
public interface IEndToEndDriftUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's indexed state.</summary>
    /// <param name="age">The age to store.</param>
    /// <param name="country">The country to store.</param>
    /// <returns>A task that completes when the state and its entries are durable.</returns>
    Task SetAsync(int age, string country);
}

/// <summary>The grain the drift end-to-end tests crawl.</summary>
public sealed class EndToEndDriftUserGrain(
    [Indexed(EndToEndStateNames.Drift)] IPersistentState<EndToEndUserState> state)
    : Grain, IEndToEndDriftUserGrain
{
    /// <inheritdoc />
    public async Task SetAsync(int age, string country)
    {
        state.State.Age = age;
        state.State.Country = country;
        await state.WriteStateAsync();
    }
}
