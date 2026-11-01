using System.Text.RegularExpressions;

namespace Orleans.Lattice;

/// <summary>
/// Makes JSON grain state written with a compiler-synthesized collection type
/// readable again (issue #5579).
/// <para>
/// A collection expression assigned to an interface-typed member
/// (<c>IReadOnlyList&lt;string&gt; x = [.. values]</c>) is materialised by the
/// compiler as an internal type such as <c>&lt;&gt;z__ReadOnlyArray&lt;T&gt;</c>,
/// <c>&lt;&gt;z__ReadOnlySingleElementList&lt;T&gt;</c> or
/// <c>&lt;&gt;z__ReadOnlyList&lt;T&gt;</c>. The JSON grain-storage serializer writes
/// every value with <c>TypeNameHandling.All</c>, so the row records that type
/// name, and Newtonsoft cannot construct it on read: the state is then unreadable
/// for good, on every activation. Removing the <c>$type</c> tag for those types
/// leaves <c>{"$values":[...]}</c>, which Newtonsoft reads into the member's
/// declared interface type, so the contents survive unchanged.
/// </para>
/// </summary>
internal static class SynthesizedCollectionTypeNames
{
    // A cheap, allocation-free pre-check on the raw UTF-8 payload. It also
    // matches the escaped spellings, because the marker contains neither
    // angle bracket.
    private static ReadOnlySpan<byte> Marker => "z__ReadOnly"u8;

    // A "$type" property whose value names a compiler-synthesized collection,
    // plus the comma that separates it from the next property. Anchored on the
    // property name with unescaped quotes, so text inside a string value (where
    // every quote is escaped) can never match.
    private static readonly Regex TypeTag = new(
        """
        "\$type"\s*:\s*"(?:[^"\\]|\\.)*?(?:<|\\u003[cC])(?:>|\\u003[eE])z__ReadOnly(?:[^"\\]|\\.)*"\s*,?
        """,
        RegexOptions.CultureInvariant | RegexOptions.Compiled,
        TimeSpan.FromSeconds(5));

    /// <summary>
    /// Returns whether <paramref name="payload"/> may carry a synthesized
    /// collection type name and therefore needs <see cref="Repair"/>.
    /// </summary>
    /// <param name="payload">The stored JSON payload.</param>
    public static bool MayContain(ReadOnlySpan<byte> payload) => payload.IndexOf(Marker) >= 0;

    /// <summary>
    /// Removes every <c>$type</c> tag naming a compiler-synthesized collection
    /// from <paramref name="json"/>.
    /// </summary>
    /// <param name="json">The stored JSON document.</param>
    /// <returns>The repaired document, or <paramref name="json"/> when nothing matched.</returns>
    public static string Repair(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        return TypeTag.Replace(json, string.Empty);
    }
}
