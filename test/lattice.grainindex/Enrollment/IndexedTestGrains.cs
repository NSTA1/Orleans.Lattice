using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// The state the enrolment integration tests project. It carries Orleans
/// serialization attributes because a real silo persists it through the memory
/// grain-storage provider.
/// </summary>
[GenerateSerializer]
public sealed class IndexedUserState
{
    /// <summary>An ordered value-type property the indexes project.</summary>
    [Id(0)] public int Age { get; set; }

    /// <summary>An ordered reference-type property the indexes project.</summary>
    [Id(1)] public string Country { get; set; } = string.Empty;

    /// <summary>A property no index projects, so exclusion stays observable.</summary>
    [Id(2)] public string Secret { get; set; } = string.Empty;
}

/// <summary>A grain tracked by the synchronous index.</summary>
public interface IIndexedUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's indexed state.</summary>
    /// <param name="age">The age to store.</param>
    /// <param name="country">The country to store.</param>
    /// <returns>A task that completes when the state and its entries are durable.</returns>
    Task SetAsync(int age, string country);

    /// <summary>Reads the stored age, activating the grain if needed.</summary>
    /// <returns>The stored age.</returns>
    Task<int> GetAgeAsync();

    /// <summary>Deletes the grain's stored state.</summary>
    /// <returns>A task that completes when the state and its entries are gone.</returns>
    Task ClearAsync();

    /// <summary>Requests deactivation so the next call re-activates the grain.</summary>
    /// <returns>A completed task.</returns>
    Task DeactivateAsync();
}

/// <summary>
/// The primary adoption shape: a plain <see cref="Grain"/> whose state
/// parameter carries <see cref="IndexedAttribute"/>. No base class, no
/// interface, no call into the index package.
/// </summary>
public sealed class IndexedUserGrain(
    [Indexed("user")] IPersistentState<IndexedUserState> state) : Grain, IIndexedUserGrain
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

    /// <inheritdoc />
    public Task ClearAsync() => state.ClearStateAsync();

    /// <inheritdoc />
    public Task DeactivateAsync()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }
}

/// <summary>A grain tracked by an index configured for eventual projection.</summary>
public interface IEventualUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's indexed state.</summary>
    /// <param name="age">The age to store.</param>
    /// <param name="country">The country to store.</param>
    /// <returns>A task that completes when the state is durable.</returns>
    Task SetAsync(int age, string country);
}

/// <summary>The eventual-mode counterpart of <see cref="IndexedUserGrain"/>.</summary>
public sealed class EventualUserGrain(
    [Indexed("eventual")] IPersistentState<IndexedUserState> state) : Grain, IEventualUserGrain
{
    /// <inheritdoc />
    public async Task SetAsync(int age, string country)
    {
        state.State.Age = age;
        state.State.Country = country;
        await state.WriteStateAsync();
    }
}

/// <summary>A grain tracked through the optional base-class facade.</summary>
public interface IBaseClassUserGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's indexed state.</summary>
    /// <param name="age">The age to store.</param>
    /// <param name="country">The country to store.</param>
    /// <returns>A task that completes when the state and its entries are durable.</returns>
    Task SetAsync(int age, string country);
}

/// <summary>
/// The fallback adoption shape: the <c>Grain&lt;TState&gt;</c>-flavoured base
/// class over the same <see cref="IndexedAttribute"/> state, proving the base
/// class adds ergonomics and not a second enrolment mechanism.
/// </summary>
public sealed class BaseClassUserGrain(
    [Indexed("baseclass")] IPersistentState<IndexedUserState> state)
    : IndexedGrain<IndexedUserState>(state), IBaseClassUserGrain
{
    /// <inheritdoc />
    public async Task SetAsync(int age, string country)
    {
        State.Age = age;
        State.Country = country;
        await WriteStateAsync();
    }
}

/// <summary>
/// A grain whose state type no declared index projects, so the attribute binds
/// to a plain state object and the grain is never touched by the index package.
/// </summary>
public interface IUntrackedGrain : IGrainWithStringKey
{
    /// <summary>Writes the grain's state.</summary>
    /// <param name="age">The age to store.</param>
    /// <returns>A task that completes when the state is durable.</returns>
    Task SetAsync(int age);
}

/// <summary>The state no index projects.</summary>
[GenerateSerializer]
public sealed class UntrackedState
{
    /// <summary>An arbitrary property.</summary>
    [Id(0)] public int Age { get; set; }
}

/// <summary>The untracked counterpart of <see cref="IndexedUserGrain"/>.</summary>
public sealed class UntrackedGrain(
    [Indexed("untracked")] IPersistentState<UntrackedState> state) : Grain, IUntrackedGrain
{
    /// <inheritdoc />
    public async Task SetAsync(int age)
    {
        state.State.Age = age;
        await state.WriteStateAsync();
    }
}
