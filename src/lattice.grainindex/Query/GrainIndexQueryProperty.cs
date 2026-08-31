namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// One projected property as the query planner sees it: its name, the key
/// bounds of the range it owns inside the shared index tree, and everything the
/// planner needs to decide whether a comparison over it can be answered as a key
/// range or has to fall back to a payload-predicate scan.
/// <para>
/// Every field is computed once, when the index is constructed. Nothing here is
/// recomputed per query, let alone per entry.
/// </para>
/// </summary>
internal sealed class GrainIndexQueryProperty
{
    internal GrainIndexQueryProperty(int ordinal, string name, Type propertyType)
    {
        Ordinal = ordinal;
        Name = name;
        PropertyType = propertyType;

        var underlying = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        IsOrderPreserving = GrainIndexKeyEncoder.IsOrderPreserving(propertyType);
        IsFloatingPoint = underlying == typeof(double) || underlying == typeof(float);
        IsTemporal = underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset);
        Binder = GrainIndexValueBinder.Create(propertyType);

        RangeStartInclusive = GrainIndexKeyEncoder.PropertyRangeStartInclusive(name);
        RangeEndExclusive = GrainIndexKeyEncoder.PropertyRangeEndExclusive(name);

        // Null sorts below every present value and the unordered-type empty
        // component sorts below that, so "the present values" is simply
        // everything at or above the presence flag. A relational comparison
        // starts here rather than at the property range start, because a null
        // operand makes a relational comparison false in C#.
        PresentStartInclusive = RangeStartInclusive + GrainIndexKeyEncoder.PresentFlag;

        FullRange = [new GrainIndexKeyRange(RangeStartInclusive, RangeEndExclusive)];
    }

    /// <summary>The property's position in the definition's declaration order.</summary>
    internal int Ordinal { get; }

    /// <summary>The projected property name, which is also the key-range prefix.</summary>
    internal string Name { get; }

    /// <summary>The property's declared CLR type.</summary>
    internal Type PropertyType { get; }

    /// <summary>Whether the key carries an order-preserving projection of the value.</summary>
    internal bool IsOrderPreserving { get; }

    /// <summary>
    /// Whether the property is <see cref="float"/> or <see cref="double"/>, whose
    /// key order places NaN below every real value even though every comparison
    /// against NaN is false. A relational scan over one therefore keeps its
    /// payload predicate to re-apply IEEE semantics.
    /// </summary>
    internal bool IsFloatingPoint { get; }

    /// <summary>
    /// Whether the property is <see cref="DateTime"/> or
    /// <see cref="DateTimeOffset"/>. The projected payload writes those in the
    /// ISO-8601 round-trip form while the core translator captures a date literal
    /// through <c>ToString()</c>, so the two never meet as the same text and a
    /// payload comparison over one is not trustworthy. Such a property is served
    /// from the key range or not at all.
    /// </summary>
    internal bool IsTemporal { get; }

    /// <summary>Encodes a query bound into the property's value component.</summary>
    internal GrainIndexValueBinder Binder { get; }

    /// <summary>The inclusive lower bound of every entry for this property.</summary>
    internal string RangeStartInclusive { get; }

    /// <summary>The exclusive upper bound of every entry for this property.</summary>
    internal string RangeEndExclusive { get; }

    /// <summary>
    /// The inclusive lower bound of the entries whose value is present, i.e.
    /// everything except the null slot.
    /// </summary>
    internal string PresentStartInclusive { get; }

    /// <summary>
    /// The whole property range as a one-element range set, shared so an
    /// unrouted clause allocates nothing.
    /// </summary>
    internal GrainIndexKeyRange[] FullRange { get; }
}
