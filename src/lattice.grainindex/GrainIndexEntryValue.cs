namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The field-name contract of an index entry's JSON payload - the seam between
/// what the projector writes and what a typed query translates against.
/// </summary>
/// <remarks>
/// <para>
/// An entry payload is a flat UTF-8 JSON object of exactly three fields:
/// </para>
/// <code>
/// {"Age":17,"$grain":"user-1","$property":"Age"}
/// </code>
/// <para>
/// The first field's <b>name is the projected property's name, verbatim</b>.
/// That is the load-bearing part. Server-side predicate push-down resolves a
/// predicate's member path against the payload by name (ordinal, then
/// case-insensitive), and
/// <see cref="LatticePredicateTranslator.Translate{T}(System.Linq.Expressions.Expression{System.Func{T, bool}})"/>
/// derives that member path from the CLR member name - so a lambda written over
/// the grain's own state type, such as <c>state =&gt; state.Age &gt;= 18</c>,
/// resolves against the entry with no name translation at all. Renaming the
/// field, camel-casing it, or nesting the value would silently break every
/// query over the index.
/// </para>
/// <para>
/// The two metadata fields are named with a leading <c>$</c>, which cannot
/// appear in a C# identifier, so they can never collide with a projected
/// property's name.
/// </para>
/// <para>
/// Values are written in the shape the predicate evaluator compares against:
/// numbers as JSON numbers, <see cref="bool"/> as a JSON boolean,
/// <see cref="string"/> and <see cref="DateTime"/>/<see cref="DateTimeOffset"/>
/// as JSON strings, a null property value as JSON <c>null</c>, and any other
/// type as the JSON form its predicate constant would be captured in (an
/// enum as its underlying number, <see cref="decimal"/> as a number, anything
/// else as its <see cref="object.ToString"/> text). Note that <b>ordering</b>
/// for dates is served from the entry key, not from the payload: the key
/// carries an order-preserving encoding, whereas payload strings compare
/// ordinally.
/// </para>
/// </remarks>
public static class GrainIndexEntryValue
{
    /// <summary>
    /// The payload field carrying the encoded grain key, so a matching entry
    /// resolves back to exactly one grain without re-parsing the entry key.
    /// </summary>
    public const string GrainKeyField = "$grain";

    /// <summary>
    /// The payload field carrying the projected property's name, so an entry is
    /// self-describing when read out of a scan.
    /// </summary>
    public const string PropertyField = "$property";
}
