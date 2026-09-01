using System.Globalization;

namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// Turns a single comparison over one projected property into the ordinal key
/// ranges that hold every entry it can match.
/// <para>
/// The contract every case here honours is <b>never under-include</b>: a range
/// set is always a superset of the comparison's true result set, and the
/// <c>exact</c> flag says whether it is also a subset. An inexact range keeps
/// the predicate so the tree re-applies it to each entry; an exact one drops the
/// predicate and lets the range stand on its own.
/// </para>
/// <para>
/// The awkward cases are the ones the key order and C# disagree about. A
/// <c>null</c> sorts below every present value but makes every relational
/// comparison false, so <c>&lt;</c> and <c>&lt;=</c> start above the null slot.
/// A NaN sorts below every real value but is likewise false under every
/// comparison, so a floating-point <c>&lt;</c>/<c>&lt;=</c> keeps its predicate
/// while <c>&gt;</c>/<c>&gt;=</c> - which already exclude the lowest slot - do
/// not. And <c>-0.0</c> and <c>+0.0</c> occupy adjacent but distinct slots while
/// comparing equal, so an equality against zero spans both.
/// </para>
/// </summary>
internal static class GrainIndexRangeBuilder
{
    /// <summary>
    /// Builds the key ranges for <paramref name="op"/> applied to
    /// <paramref name="property"/> against <paramref name="constant"/>.
    /// </summary>
    /// <returns>
    /// <c>false</c> when no sound range can be derived, in which case the caller
    /// scans the whole property range and leans on the payload predicate.
    /// </returns>
    internal static bool TryBuild(
        GrainIndexQueryProperty property,
        LatticeComparisonOperator op,
        object? constant,
        out GrainIndexKeyRange[] ranges,
        out bool exact)
    {
        ranges = GrainIndexRangeSet.Empty;
        exact = false;

        if (!property.IsOrderPreserving)
            return false;

        if (constant is null)
            return TryBuildNull(property, op, out ranges, out exact);

        if (property.IsFloatingPoint && TryAsDouble(constant, out double real))
        {
            if (double.IsNaN(real))
                return TryBuildNotANumber(property, op, out ranges, out exact);

            if (real == 0.0 && op is LatticeComparisonOperator.Equal or LatticeComparisonOperator.NotEqual)
                return TryBuildSignedZero(property, op, out ranges, out exact);
        }

        if (!property.Binder.TryEncode(constant, out string encoded))
            return false;

        switch (op)
        {
            case LatticeComparisonOperator.Equal:
                ranges = [ValueRange(property, encoded)];
                exact = true;
                return true;

            case LatticeComparisonOperator.NotEqual:
                // Everything the equality slot leaves behind, which deliberately
                // keeps the null slot: in C# a null operand makes != true.
                ranges = GrainIndexRangeSet.Complement(
                    [ValueRange(property, encoded)],
                    property.RangeStartInclusive,
                    property.RangeEndExclusive);
                exact = true;
                return true;

            case LatticeComparisonOperator.LessThan:
                ranges = NonEmpty(new GrainIndexKeyRange(
                    property.PresentStartInclusive,
                    GrainIndexKeyEncoder.ValueRangeStartInclusive(property.Name, encoded)));
                exact = !property.IsFloatingPoint;
                return true;

            case LatticeComparisonOperator.LessThanOrEqual:
                ranges = NonEmpty(new GrainIndexKeyRange(
                    property.PresentStartInclusive,
                    GrainIndexKeyEncoder.ValueRangeEndExclusive(property.Name, encoded)));
                exact = !property.IsFloatingPoint;
                return true;

            case LatticeComparisonOperator.GreaterThan:
                ranges = NonEmpty(new GrainIndexKeyRange(
                    GrainIndexKeyEncoder.ValueRangeEndExclusive(property.Name, encoded),
                    property.RangeEndExclusive));
                exact = true;
                return true;

            case LatticeComparisonOperator.GreaterThanOrEqual:
                ranges = NonEmpty(new GrainIndexKeyRange(
                    GrainIndexKeyEncoder.ValueRangeStartInclusive(property.Name, encoded),
                    property.RangeEndExclusive));
                exact = true;
                return true;

            default:
                return false;
        }
    }

    /// <summary>
    /// Builds the ranges for a prefix match, which the string encoding supports
    /// directly: escaping is applied per character, so the encoding of a string
    /// starting with <paramref name="prefix"/> starts with the encoding of
    /// <paramref name="prefix"/>.
    /// </summary>
    internal static bool TryBuildPrefix(
        GrainIndexQueryProperty property,
        string prefix,
        out GrainIndexKeyRange[] ranges)
    {
        ranges = GrainIndexRangeSet.Empty;
        if (property.PropertyType != typeof(string))
            return false;

        string encoded = GrainIndexKeyEncoder.EncodeValue(prefix);
        if (encoded.Length == 0)
            return false;

        char last = encoded[^1];
        if (last == char.MaxValue)
            return false;

        string start = property.RangeStartInclusive + encoded;
        string end = property.RangeStartInclusive + encoded[..^1] + (char)(last + 1);

        ranges = NonEmpty(new GrainIndexKeyRange(start, end));
        return true;
    }

    private static bool TryBuildNull(
        GrainIndexQueryProperty property,
        LatticeComparisonOperator op,
        out GrainIndexKeyRange[] ranges,
        out bool exact)
    {
        ranges = GrainIndexRangeSet.Empty;
        exact = true;

        switch (op)
        {
            case LatticeComparisonOperator.Equal:
                if (!property.Binder.TryEncodeNull(out string encoded))
                {
                    // The property cannot hold null, so nothing can equal null.
                    return true;
                }

                ranges = [ValueRange(property, encoded)];
                return true;

            case LatticeComparisonOperator.NotEqual:
                ranges = NonEmpty(new GrainIndexKeyRange(
                    property.PresentStartInclusive,
                    property.RangeEndExclusive));
                return true;

            default:
                // A relational comparison with a null operand is false in C#.
                return true;
        }
    }

    private static bool TryBuildNotANumber(
        GrainIndexQueryProperty property,
        LatticeComparisonOperator op,
        out GrainIndexKeyRange[] ranges,
        out bool exact)
    {
        // Every comparison against NaN is false except !=, which is true for
        // every value including another NaN.
        exact = true;
        ranges = op == LatticeComparisonOperator.NotEqual
            ? property.FullRange
            : GrainIndexRangeSet.Empty;
        return true;
    }

    private static bool TryBuildSignedZero(
        GrainIndexQueryProperty property,
        LatticeComparisonOperator op,
        out GrainIndexKeyRange[] ranges,
        out bool exact)
    {
        ranges = GrainIndexRangeSet.Empty;
        exact = false;

        if (!property.Binder.TryEncode(-0.0, out string negative)
            || !property.Binder.TryEncode(0.0, out string positive))
        {
            return false;
        }

        // The two zero slots are distinct in the key order but compare equal, so
        // an equality against zero spans from the lower to the upper of the two.
        if (string.CompareOrdinal(negative, positive) > 0)
        {
            (negative, positive) = (positive, negative);
        }

        var span = new GrainIndexKeyRange(
            GrainIndexKeyEncoder.ValueRangeStartInclusive(property.Name, negative),
            GrainIndexKeyEncoder.ValueRangeEndExclusive(property.Name, positive));

        exact = true;
        ranges = op == LatticeComparisonOperator.Equal
            ? NonEmpty(span)
            : GrainIndexRangeSet.Complement(
                NonEmpty(span),
                property.RangeStartInclusive,
                property.RangeEndExclusive);
        return true;
    }

    private static GrainIndexKeyRange ValueRange(GrainIndexQueryProperty property, string encoded) =>
        new(
            GrainIndexKeyEncoder.ValueRangeStartInclusive(property.Name, encoded),
            GrainIndexKeyEncoder.ValueRangeEndExclusive(property.Name, encoded));

    private static GrainIndexKeyRange[] NonEmpty(GrainIndexKeyRange range) =>
        range.IsEmpty ? GrainIndexRangeSet.Empty : [range];

    private static bool TryAsDouble(object constant, out double value)
    {
        switch (constant)
        {
            case double d:
                value = d;
                return true;
            case float f:
                value = f;
                return true;
            case IConvertible convertible:
                try
                {
                    value = convertible.ToDouble(CultureInfo.InvariantCulture);
                    return true;
                }
                catch (Exception exception) when (
                    exception is InvalidCastException or FormatException or OverflowException)
                {
                    value = 0.0;
                    return false;
                }

            default:
                value = 0.0;
                return false;
        }
    }
}
