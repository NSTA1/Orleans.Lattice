using System.ComponentModel;
using System.Globalization;
using Orleans.Runtime;

namespace Orleans.Lattice.Internal;

/// <summary>
/// Registers a <see cref="TypeConverter"/> for
/// <see cref="GrainId"/> so JSON serialisers (notably
/// <c>Newtonsoft.Json</c>, which backs <c>AzureTableGrainStorage</c>'s
/// default JSON storage format) can round-trip <see cref="GrainId"/>
/// values used as dictionary keys.
/// <para>
/// Without this converter, Newtonsoft serialises <see cref="GrainId"/>
/// keys via <see cref="object.ToString"/> on write (producing
/// <c>"&lt;grain-type&gt;/&lt;key&gt;"</c>) but fails on read with
/// <c>JsonSerializationException: Could not convert string '…' to
/// dictionary key type 'Orleans.Runtime.GrainId'</c> because no
/// converter is registered. This is the failure mode reproduced by
/// <c>InternalNodeStateJsonRoundTripTests</c>.
/// </para>
/// <para>
/// The lattice persists <see cref="GrainId"/>-keyed dictionaries (e.g.
/// <c>InternalNodeState.ChildDigests</c>), so the converter is a
/// correctness requirement, not a performance hint. It is registered
/// from <see cref="LatticeServiceCollectionExtensions.AddLattice"/> so
/// any host that wires lattice in gets the fix automatically before
/// the first grain activation runs.
/// </para>
/// </summary>
internal static class GrainIdTypeConverterRegistration
{
    private static readonly object _gate = new();
    private static bool _registered;

    /// <summary>
    /// Attaches a <see cref="TypeConverterAttribute"/> pointing at
    /// <see cref="GrainIdTypeConverter"/> to <see cref="GrainId"/> via
    /// <see cref="TypeDescriptor.AddAttributes(System.Type, System.Attribute[])"/>.
    /// Idempotent: a second invocation is a no-op.
    /// </summary>
    public static void EnsureRegistered()
    {
        if (Volatile.Read(ref _registered)) return;
        lock (_gate)
        {
            if (_registered) return;
            TypeDescriptor.AddAttributes(
                typeof(GrainId),
                new TypeConverterAttribute(typeof(GrainIdTypeConverter)));
            Volatile.Write(ref _registered, true);
        }
    }
}

/// <summary>
/// Converts <see cref="GrainId"/> values to and from their canonical
/// string form (<c>&lt;grain-type&gt;/&lt;key&gt;</c>) so JSON
/// serialisers that consult <see cref="TypeDescriptor"/> for a
/// dictionary-key converter (notably <c>Newtonsoft.Json</c> via the
/// default Orleans <c>AzureTableGrainStorage</c> registration) can
/// round-trip dictionaries keyed by <see cref="GrainId"/>.
/// <para>
/// Allocation profile per call (intentional, minimal for the
/// <see cref="TypeConverter"/> API contract):
/// <list type="bullet">
/// <item><description>
/// Write (<see cref="ConvertTo"/>): one <see cref="string"/> for
/// <c>"{type}/{key}"</c>. Unavoidable - this is the payload the JSON
/// serialiser writes.
/// </description></item>
/// <item><description>
/// Read (<see cref="ConvertFrom"/>): one <see cref="GrainId"/> with
/// its two <c>IdSpan</c> byte-array fields. Unavoidable - we must
/// materialise the result. Plus one implicit struct box into
/// <c>object?</c> for the API return type; the caller (Newtonsoft)
/// unboxes immediately when assigning to the target dictionary key.
/// </description></item>
/// </list>
/// No extra allocations on the predicate methods
/// (<see cref="CanConvertFrom"/> / <see cref="CanConvertTo"/>) - those
/// short-circuit on <see cref="string"/> without delegating to the
/// base implementation's <c>typeof(InstanceDescriptor)</c> check.
/// </para>
/// </summary>
internal sealed class GrainIdTypeConverter : TypeConverter
{
    public override bool CanConvertFrom(ITypeDescriptorContext? context, Type sourceType)
        => sourceType == typeof(string);

    public override bool CanConvertTo(ITypeDescriptorContext? context, Type? destinationType)
        => destinationType == typeof(string);

    public override object ConvertFrom(ITypeDescriptorContext? context, CultureInfo? culture, object value)
    {
        // String is the only supported source; anything else surfaces
        // the standard NotSupportedException from the base. The hot
        // path - dictionary-key deserialisation through
        // TypeDescriptor.GetConverter(typeof(GrainId)) - always lands
        // here with a string, so the pattern check resolves on the
        // first arm without falling through to base.
        if (value is string s)
        {
            return GrainId.Parse(s);
        }
        return base.ConvertFrom(context, culture, value)!;
    }

    public override object? ConvertTo(ITypeDescriptorContext? context, CultureInfo? culture, object? value, Type destinationType)
    {
        // GrainId is a readonly struct so `value` arrives already
        // boxed; the pattern match below unboxes without allocating.
        // GrainId.ToString() does the canonical "{type}/{key}" format -
        // matches GrainId.Parse's round-trip contract exactly.
        if (destinationType == typeof(string) && value is GrainId gid)
        {
            return gid.ToString();
        }
        return base.ConvertTo(context, culture, value, destinationType);
    }
}
