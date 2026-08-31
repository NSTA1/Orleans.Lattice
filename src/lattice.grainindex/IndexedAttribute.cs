using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Opts a grain into its declared grain indexes. Applied to the constructor
/// parameter through which the grain receives its persistent state, in place of
/// <see cref="PersistentStateAttribute"/>:
/// <code>
/// public sealed class UserGrain(
///     [Indexed("user")] IPersistentState&lt;UserState&gt; state) : Grain, IUserGrain
/// {
///     public async Task SetAgeAsync(int age)
///     {
///         state.State.Age = age;
///         await state.WriteStateAsync();
///     }
/// }
/// </code>
/// </summary>
/// <remarks>
/// <para>
/// The attribute is the whole opt-in. It carries the same state name and
/// storage name <see cref="PersistentStateAttribute"/> does, so the grain's
/// persistence is configured exactly as before, and additionally binds the
/// state object to the index-enrolment path. Nothing else about the grain
/// changes: no base class, no interface, no call into the index package.
/// </para>
/// <para>
/// Marking the state rather than the grain class is deliberate. The state
/// object is the one place that knows both what the grain's durable state
/// <i>is</i> and exactly <i>when</i> it changes, so binding there gives the
/// package a precise hook - the grain's own <c>WriteStateAsync</c> - instead of
/// an approximation such as intercepting every grain call or wrapping a storage
/// provider that untracked grains also use. A grain with no
/// <c>[Indexed]</c> parameter is never touched by this package.
/// </para>
/// <para>
/// What the binding does:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// On activation, once the state has been read, the grain's current state is
/// projected into every declared index over that state type whose grain
/// interface the grain implements. Re-activating an unchanged grain writes
/// nothing at all.
/// </description>
/// </item>
/// <item>
/// <description>
/// On every <c>WriteStateAsync</c>, the state is re-projected and the
/// difference applied to the index as one atomic batch, so a moved value's old
/// entry is tombstoned in the same visibility flip that publishes its new one.
/// </description>
/// </item>
/// <item>
/// <description>
/// On <c>ClearStateAsync</c>, the grain's entries are withdrawn from the index.
/// </description>
/// </item>
/// </list>
/// <para>
/// A grain whose state type no declared index projects, or which implements no
/// indexed grain interface, keeps the plain persistence behaviour and pays
/// nothing beyond the state object's own indirection.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class IndexedAttribute : Attribute, IFacetMetadata, IPersistentStateConfiguration
{
    /// <summary>
    /// Declares an indexed persistent state facet.
    /// </summary>
    /// <param name="stateName">
    /// The state name passed to the storage provider. Defaults to the annotated
    /// parameter's name, exactly as <see cref="PersistentStateAttribute"/> does.
    /// </param>
    /// <param name="storageName">
    /// The named grain-storage provider to use, or <c>null</c> for the default
    /// provider.
    /// </param>
    public IndexedAttribute(string? stateName = null, string? storageName = null)
    {
        StateName = stateName!;
        StorageName = storageName!;
    }

    /// <summary>The state name passed to the storage provider.</summary>
    public string StateName { get; }

    /// <summary>The named grain-storage provider, or <c>null</c> for the default.</summary>
    public string StorageName { get; }
}
