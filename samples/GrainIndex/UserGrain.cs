using Orleans.Lattice.GrainIndex;
using Orleans.Runtime;

namespace Orleans.Lattice.Samples.GrainIndex;

/// <summary>The typed state a <see cref="UserGrain"/> persists and the index projects.</summary>
[GenerateSerializer]
public sealed class UserState
{
    /// <summary>The user's age, indexed so it can be range-scanned.</summary>
    [Id(0)]
    public int Age { get; set; }

    /// <summary>The user's country, indexed so it can be matched for equality.</summary>
    [Id(1)]
    public string Country { get; set; } = string.Empty;
}

/// <summary>A user grain whose state is tracked in a grain index.</summary>
public interface IUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the user's profile, which re-projects its index entries.</summary>
    /// <param name="age">The user's age.</param>
    /// <param name="country">The user's country.</param>
    Task SetProfileAsync(int age, string country);

    /// <summary>Reads the user's age straight from grain state.</summary>
    Task<int> GetAgeAsync();
}

/// <summary>
/// The grain implementation. <c>[Indexed]</c> stands in for
/// <c>[PersistentState]</c> and installs the index projection on the grain's
/// activation and write path, so no index maintenance appears in this code.
/// </summary>
/// <param name="state">The persistent, indexed state.</param>
public sealed class UserGrain([Indexed("user")] IPersistentState<UserState> state)
    : IndexedGrain<UserState>(state), IUserGrain
{
    /// <inheritdoc />
    public async Task SetProfileAsync(int age, string country)
    {
        State.Age = age;
        State.Country = country;

        // WriteStateAsync persists the state AND republishes this grain's index
        // entries as one atomic reconciliation.
        await WriteStateAsync();
    }

    /// <inheritdoc />
    public Task<int> GetAgeAsync() => Task.FromResult(State.Age);
}
