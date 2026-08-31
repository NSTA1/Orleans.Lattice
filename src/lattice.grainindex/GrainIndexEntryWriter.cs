using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Builds one grain's index entries: the on-tree key and the UTF-8 JSON payload
/// for each projected property.
/// </summary>
/// <remarks>
/// <para>
/// This runs once per property per grain mutation, so everything reusable is
/// reused. A writer holds its JSON buffer, its <see cref="Utf8JsonWriter"/>,
/// and its key scratch array for its whole lifetime and is itself cached per
/// thread, so a steady-state projection allocates only what it must hand back:
/// the entry array, one key string per entry, and one payload array per entry.
/// </para>
/// <para>
/// A writer is rented and returned around a single synchronous projection.
/// Renting nulls the cache slot, so the pathological case of a property
/// accessor re-entering the projector gets a fresh writer instead of
/// corrupting the one in flight.
/// </para>
/// </remarks>
internal sealed class GrainIndexEntryWriter
{
    private static readonly JsonEncodedText GrainKeyField =
        JsonEncodedText.Encode(GrainIndexEntryValue.GrainKeyField);

    private static readonly JsonEncodedText PropertyField =
        JsonEncodedText.Encode(GrainIndexEntryValue.PropertyField);

    [ThreadStatic]
    private static GrainIndexEntryWriter? _cached;

    private readonly ArrayBufferWriter<byte> _payload = new(256);
    private readonly Utf8JsonWriter _json;
    private readonly char[] _keyScratch = new char[GrainIndexKeyEncoder.StackBufferLength];

    private GrainIndexEntry[] _entries = [];
    private string _grainKey = string.Empty;
    private int _count;

    private GrainIndexEntryWriter() =>
        _json = new Utf8JsonWriter(_payload, new JsonWriterOptions { SkipValidation = true });

    /// <summary>
    /// Takes the calling thread's cached writer, or a fresh one when the cache
    /// is empty or already lent out.
    /// </summary>
    internal static GrainIndexEntryWriter Rent()
    {
        var writer = _cached;
        if (writer is null)
            return new GrainIndexEntryWriter();

        _cached = null;
        return writer;
    }

    /// <summary>Clears the writer's per-projection state and re-caches it.</summary>
    internal static void Return(GrainIndexEntryWriter writer)
    {
        writer._entries = [];
        writer._grainKey = string.Empty;
        writer._count = 0;
        _cached = writer;
    }

    /// <summary>
    /// Starts a projection for <paramref name="grainKey"/> that will produce
    /// exactly <paramref name="entryCount"/> entries.
    /// </summary>
    internal void Begin(string grainKey, int entryCount)
    {
        _grainKey = grainKey;
        _entries = entryCount == 0 ? [] : new GrainIndexEntry[entryCount];
        _count = 0;
    }

    /// <summary>Returns the entries built since <see cref="Begin(string, int)"/>.</summary>
    internal GrainIndexEntry[] Complete() => _entries;

    /// <summary>
    /// Appends the entry for one projected property. The value is read through
    /// its declared type, so a value-type property is never boxed on the way
    /// into either the key or the payload.
    /// </summary>
    /// <typeparam name="TProperty">The property's declared CLR type.</typeparam>
    /// <param name="name">The property's name.</param>
    /// <param name="encodedName">The property's name, pre-escaped for JSON at declaration time.</param>
    /// <param name="value">The property's current value.</param>
    internal void Append<TProperty>(string name, JsonEncodedText encodedName, TProperty value)
    {
        _entries[_count++] = new GrainIndexEntry(
            BuildKey(name, value),
            BuildPayload(name, encodedName, value));
    }

    private string BuildKey<TProperty>(string name, TProperty value)
    {
        var builder = new KeyBuilder(_keyScratch);
        try
        {
            builder.Append(name);
            builder.Append(GrainIndexKeyEncoder.Separator);
            GrainIndexKeyEncoder.AppendValue(ref builder, value);
            builder.Append(GrainIndexKeyEncoder.Separator);
            builder.Append(_grainKey);
            return builder.ToString();
        }
        finally
        {
            builder.Dispose();
        }
    }

    private byte[] BuildPayload<TProperty>(string name, JsonEncodedText encodedName, TProperty value)
    {
        _payload.ResetWrittenCount();
        _json.Reset(_payload);

        _json.WriteStartObject();
        _json.WritePropertyName(encodedName);
        WriteValue(value);
        _json.WriteString(GrainKeyField, _grainKey);
        _json.WriteString(PropertyField, name);
        _json.WriteEndObject();
        _json.Flush();

        return _payload.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Writes the property value in the JSON shape the server-side predicate
    /// evaluator compares against, mirroring how the predicate translator
    /// captures a constant of the same type - so a lambda's literal and the
    /// stored value always meet as the same JSON kind.
    /// </summary>
    private void WriteValue<TProperty>(TProperty value)
    {
        if (typeof(TProperty) == typeof(int)) { _json.WriteNumberValue(Unsafe.As<TProperty, int>(ref value)); return; }
        if (typeof(TProperty) == typeof(long)) { _json.WriteNumberValue(Unsafe.As<TProperty, long>(ref value)); return; }
        if (typeof(TProperty) == typeof(short)) { _json.WriteNumberValue(Unsafe.As<TProperty, short>(ref value)); return; }
        if (typeof(TProperty) == typeof(sbyte)) { _json.WriteNumberValue(Unsafe.As<TProperty, sbyte>(ref value)); return; }
        if (typeof(TProperty) == typeof(byte)) { _json.WriteNumberValue(Unsafe.As<TProperty, byte>(ref value)); return; }
        if (typeof(TProperty) == typeof(ushort)) { _json.WriteNumberValue(Unsafe.As<TProperty, ushort>(ref value)); return; }
        if (typeof(TProperty) == typeof(uint)) { _json.WriteNumberValue(Unsafe.As<TProperty, uint>(ref value)); return; }
        if (typeof(TProperty) == typeof(ulong)) { _json.WriteNumberValue(Unsafe.As<TProperty, ulong>(ref value)); return; }
        if (typeof(TProperty) == typeof(decimal)) { _json.WriteNumberValue(Unsafe.As<TProperty, decimal>(ref value)); return; }
        if (typeof(TProperty) == typeof(double)) { WriteReal(Unsafe.As<TProperty, double>(ref value)); return; }
        if (typeof(TProperty) == typeof(float)) { WriteReal(Unsafe.As<TProperty, float>(ref value)); return; }
        if (typeof(TProperty) == typeof(bool)) { _json.WriteBooleanValue(Unsafe.As<TProperty, bool>(ref value)); return; }
        if (typeof(TProperty) == typeof(char)) { WriteChar(Unsafe.As<TProperty, char>(ref value)); return; }
        if (typeof(TProperty) == typeof(DateTime)) { _json.WriteStringValue(Unsafe.As<TProperty, DateTime>(ref value)); return; }
        if (typeof(TProperty) == typeof(DateTimeOffset)) { _json.WriteStringValue(Unsafe.As<TProperty, DateTimeOffset>(ref value)); return; }
        if (typeof(TProperty) == typeof(Guid)) { _json.WriteStringValue(Unsafe.As<TProperty, Guid>(ref value)); return; }
        if (typeof(TProperty) == typeof(string)) { _json.WriteStringValue(Unsafe.As<TProperty, string>(ref value)); return; }

        if (typeof(TProperty) == typeof(int?)) { ref var v = ref Unsafe.As<TProperty, int?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(long?)) { ref var v = ref Unsafe.As<TProperty, long?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(short?)) { ref var v = ref Unsafe.As<TProperty, short?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(sbyte?)) { ref var v = ref Unsafe.As<TProperty, sbyte?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(byte?)) { ref var v = ref Unsafe.As<TProperty, byte?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(ushort?)) { ref var v = ref Unsafe.As<TProperty, ushort?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(uint?)) { ref var v = ref Unsafe.As<TProperty, uint?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(ulong?)) { ref var v = ref Unsafe.As<TProperty, ulong?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(decimal?)) { ref var v = ref Unsafe.As<TProperty, decimal?>(ref value); if (v.HasValue) { _json.WriteNumberValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(double?)) { ref var v = ref Unsafe.As<TProperty, double?>(ref value); if (v.HasValue) { WriteReal(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(float?)) { ref var v = ref Unsafe.As<TProperty, float?>(ref value); if (v.HasValue) { WriteReal(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(bool?)) { ref var v = ref Unsafe.As<TProperty, bool?>(ref value); if (v.HasValue) { _json.WriteBooleanValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(char?)) { ref var v = ref Unsafe.As<TProperty, char?>(ref value); if (v.HasValue) { WriteChar(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(DateTime?)) { ref var v = ref Unsafe.As<TProperty, DateTime?>(ref value); if (v.HasValue) { _json.WriteStringValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(DateTimeOffset?)) { ref var v = ref Unsafe.As<TProperty, DateTimeOffset?>(ref value); if (v.HasValue) { _json.WriteStringValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }
        if (typeof(TProperty) == typeof(Guid?)) { ref var v = ref Unsafe.As<TProperty, Guid?>(ref value); if (v.HasValue) { _json.WriteStringValue(v.GetValueOrDefault()); } else { _json.WriteNullValue(); } return; }

        WriteFallback(value, Nullable.GetUnderlyingType(typeof(TProperty)) ?? typeof(TProperty));
    }

    /// <summary>
    /// The last-resort shape for a property type outside the recognised set. It
    /// reproduces the predicate translator's own constant-capture rule - an
    /// enum becomes its underlying number, anything else becomes its text - so
    /// a predicate literal of that type still meets the stored value as the
    /// same JSON kind. Text is rendered with the invariant culture, because a
    /// persisted index entry must not depend on the culture of the silo that
    /// wrote it.
    /// <para>
    /// This is the one path in the writer that boxes a value-type property: the
    /// enum conversion and the <see cref="IFormattable"/> test both need the
    /// value as a reference. It is confined to types with no order-preserving
    /// key encoding, which are already on the payload-predicate-scan path rather
    /// than the ranged one, and reading an arbitrary enum's underlying value or
    /// dispatching an arbitrary type's formatter without boxing is not
    /// expressible in a generic method. Every type in the ordered set - and
    /// every other type the writer recognises by name above - reaches its writer
    /// through a reinterpret cast and allocates nothing.
    /// </para>
    /// </summary>
    private void WriteFallback<TProperty>(TProperty value, Type declaredType)
    {
        if (value is null)
        {
            _json.WriteNullValue();
            return;
        }

        if (declaredType.IsEnum)
        {
            _json.WriteNumberValue(Convert.ToInt64(value, CultureInfo.InvariantCulture));
            return;
        }

        _json.WriteStringValue(
            value is IFormattable formattable
                ? formattable.ToString(null, CultureInfo.InvariantCulture)
                : value.ToString());
    }

    private void WriteChar(char value) =>
        _json.WriteStringValue(MemoryMarshal.CreateReadOnlySpan(ref value, 1));

    /// <summary>
    /// Writes a floating-point value, spelling out the non-finite values as the
    /// named literals System.Text.Json uses for them. JSON has no number form
    /// for them, and the writer would otherwise throw.
    /// </summary>
    private void WriteReal(double value)
    {
        if (double.IsFinite(value))
            _json.WriteNumberValue(value);
        else if (double.IsNaN(value))
            _json.WriteStringValue("NaN");
        else
            _json.WriteStringValue(double.IsPositiveInfinity(value) ? "Infinity" : "-Infinity");
    }
}
