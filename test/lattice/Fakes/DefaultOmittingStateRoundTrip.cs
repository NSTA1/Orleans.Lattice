using System.Reflection;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Models the one behaviour of the production grain-storage serializer that makes
/// a non-default initializer on a persisted state POCO a corruption: it OMITS any
/// member whose value equals <c>default(T)</c>, and the load path reconstructs the
/// row by running the POCO's parameterless constructor - initializers and all -
/// and then assigning only the members that were actually present.
/// <para>
/// This exists because no fixture in this repository can otherwise reproduce the
/// hazard. <see cref="FakePersistentState{T}"/> hands back the very same object
/// reference with no serialization at all, and every cluster fixture registers
/// <c>AddMemoryGrainStorage</c>, which uses Orleans' own binary serializer - and
/// that one PRESERVES a written default. A round trip through either therefore
/// passes whether or not the defect is present, so a regression test built on one
/// would be a check that cannot fail. Simulating the omission directly is the only
/// way to assert the behaviour a real omitting provider produces.
/// </para>
/// <para>
/// Scope: the top-level <c>[Id]</c> members of one state POCO, which is where the
/// hazard lives (a member is omitted, and the reconstructing initializer supplies a
/// different value). Nested object graphs are copied by reference and are not
/// re-derived.
/// </para>
/// </summary>
internal static class DefaultOmittingStateRoundTrip
{
    /// <summary>
    /// Returns what <paramref name="state"/> reconstructs as after being persisted
    /// by a serializer that omits type-default members and reloaded into a fresh
    /// instance. A member whose value is <c>default(T)</c> is dropped, so the
    /// reconstructed instance carries whatever that member's initializer supplies.
    /// </summary>
    internal static T Simulate<T>(T state)
        where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(state);

        var reconstructed = new T();

        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanRead || !property.CanWrite)
            {
                continue;
            }

            if (!property.GetCustomAttributes().Any(a => a.GetType().Name == "IdAttribute"))
            {
                continue;
            }

            var value = property.GetValue(state);

            // The omission rule. A value type's default is compared structurally;
            // note that default(int?) is null, so a nullable member carrying 0 is
            // NOT a default and therefore survives - which is precisely why making
            // a sentinel member nullable repairs the round trip.
            var omitted = property.PropertyType.IsValueType
                ? Equals(value, Activator.CreateInstance(property.PropertyType))
                : value is null;

            if (omitted)
            {
                continue;
            }

            property.SetValue(reconstructed, value);
        }

        return reconstructed;
    }
}
