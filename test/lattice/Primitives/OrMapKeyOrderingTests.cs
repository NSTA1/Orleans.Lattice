using System.Globalization;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regression for cross-replica enumeration order on
/// <see cref="OrMap{TKey, TValue}.Keys"/>.
/// <para>
/// <c>Keys()</c> sorted with <c>Comparer&lt;TKey&gt;.Default</c>, which for the
/// only production key type (<c>string</c>) resolves to the culture-sensitive
/// <c>String.CompareTo</c> - full ICU collation. Two silos running under
/// different <see cref="CultureInfo.CurrentCulture"/> values, or different ICU
/// versions, therefore enumerated the same converged map in different orders.
/// Every sibling primitive (<c>GSet.Values</c>, <c>OrSet.Elements</c>,
/// <c>RwSet.Elements</c>) already sorts with <see cref="StringComparer.Ordinal"/>.
/// </para>
/// </summary>
[TestFixture]
public class OrMapKeyOrderingTests
{
    // Under ICU collation these sort case-insensitively first ("a" before "B");
    // ordinally the uppercase code unit (0x42) sorts before the lowercase (0x61).
    private static readonly string[] CultureSensitiveKeys = ["a", "B"];

    private static OrMap<string, PnCounter> MapWithKeys(params string[] keys)
    {
        var map = new OrMap<string, PnCounter>();
        foreach (var key in keys)
        {
            map.Set(key, "r1", new PnCounter());
        }
        return map;
    }

    private static IReadOnlyList<string> KeysUnderCulture(OrMap<string, PnCounter> map, CultureInfo culture)
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = culture;
            return [.. map.Keys()];
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Test]
    public void Keys_returns_string_keys_in_ordinal_order()
    {
        var map = MapWithKeys(CultureSensitiveKeys);

        Assert.That(KeysUnderCulture(map, new CultureInfo("en-US")), Is.EqualTo(new[] { "B", "a" }),
            "string keys must enumerate in ordinal order, matching GSet.Values / OrSet.Elements / RwSet.Elements");
    }

    [Test]
    public void Keys_returns_the_same_order_under_two_different_cultures()
    {
        var map = MapWithKeys(CultureSensitiveKeys);

        var underInvariant = KeysUnderCulture(map, CultureInfo.InvariantCulture);
        var underSwedish = KeysUnderCulture(map, new CultureInfo("sv-SE"));

        Assert.That(underSwedish, Is.EqualTo(underInvariant),
            "two replicas with different ambient cultures must enumerate a converged map identically");
    }

    [Test]
    public void Keys_orders_non_string_keys_by_the_default_comparer()
    {
        // The ordinal specialisation must not disturb the generic path.
        var map = new OrMap<int, PnCounter>();
        foreach (var key in new[] { 30, 4, 100 })
        {
            map.Set(key, "r1", new PnCounter());
        }

        Assert.That(map.Keys(), Is.EqualTo(new[] { 4, 30, 100 }));
    }
}
