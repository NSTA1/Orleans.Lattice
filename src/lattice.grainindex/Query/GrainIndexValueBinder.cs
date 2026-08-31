using System.Globalization;

namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// Encodes a query bound - a literal lifted out of the user's lambda - into the
/// same value component the projector wrote, so a comparison can be answered as
/// a contiguous key range instead of a payload scan.
/// <para>
/// The binder exists because <see cref="GrainIndexKeyEncoder.EncodeValue{TValue}(TValue)"/>
/// picks its encoding from <c>typeof(TValue)</c>, not from the runtime type of
/// the value, and a captured literal arrives as an <see cref="object"/> whose
/// type frequently differs from the property's (the C# compiler widens
/// <c>u.Score &gt; 1</c> to a <see cref="double"/> comparison but the literal is
/// still an <see cref="int"/>). One binder is built per projected property when
/// the index is constructed, never per query and never per entry.
/// </para>
/// </summary>
internal abstract class GrainIndexValueBinder
{
    /// <summary>
    /// Builds the binder for <paramref name="propertyType"/>.
    /// </summary>
    internal static GrainIndexValueBinder Create(Type propertyType)
    {
        var closed = typeof(TypedGrainIndexValueBinder<>).MakeGenericType(propertyType);
        return (GrainIndexValueBinder)Activator.CreateInstance(closed)!;
    }

    /// <summary>
    /// Converts <paramref name="value"/> to the property's declared type and
    /// encodes it, or reports <c>false</c> when no lossless conversion exists -
    /// in which case the planner falls back to a payload-predicate scan rather
    /// than inventing a bound.
    /// </summary>
    internal abstract bool TryEncode(object? value, out string encoded);

    /// <summary>
    /// Encodes the null slot, which only exists for a property that can actually
    /// hold <c>null</c>.
    /// </summary>
    internal abstract bool TryEncodeNull(out string encoded);
}

/// <summary>
/// The closed binder for one property type. See
/// <see cref="GrainIndexValueBinder"/>.
/// </summary>
/// <typeparam name="TProperty">The property's declared CLR type.</typeparam>
internal sealed class TypedGrainIndexValueBinder<TProperty> : GrainIndexValueBinder
{
    private static readonly Type Underlying = Nullable.GetUnderlyingType(typeof(TProperty)) ?? typeof(TProperty);

    private static readonly bool AcceptsNull =
        default(TProperty) is null;

    /// <inheritdoc />
    internal override bool TryEncode(object? value, out string encoded)
    {
        if (value is null)
            return TryEncodeNull(out encoded);

        if (value is TProperty direct)
        {
            encoded = GrainIndexKeyEncoder.EncodeValue(direct);
            return true;
        }

        if (!TryConvert(value, out var converted))
        {
            encoded = string.Empty;
            return false;
        }

        encoded = GrainIndexKeyEncoder.EncodeValue(converted);
        return true;
    }

    /// <inheritdoc />
    internal override bool TryEncodeNull(out string encoded)
    {
        if (!AcceptsNull)
        {
            encoded = string.Empty;
            return false;
        }

        encoded = GrainIndexKeyEncoder.EncodeValue(default(TProperty)!);
        return true;
    }

    private static bool TryConvert(object value, out TProperty converted)
    {
        try
        {
            // The compiler routinely widens a literal to match the property
            // (u.Score > 1 captures an int against a double property), and the
            // translator unwraps that Convert node, so the raw literal has to be
            // brought back to the property's type before it is encoded.
            object boxed = value;
            if (Underlying == typeof(DateTimeOffset) && value is DateTime dateTime)
            {
                boxed = new DateTimeOffset(dateTime);
            }
            else if (Underlying != value.GetType())
            {
                if (value is not IConvertible)
                {
                    converted = default!;
                    return false;
                }

                boxed = Convert.ChangeType(value, Underlying, CultureInfo.InvariantCulture);
            }

            // Unboxing a boxed underlying value into Nullable<T> is supported by
            // the runtime, so this covers both the plain and the nullable form.
            converted = (TProperty)boxed;
            return true;
        }
        catch (Exception exception) when (
            exception is InvalidCastException
                or FormatException
                or OverflowException
                or ArgumentException)
        {
            converted = default!;
            return false;
        }
    }
}
