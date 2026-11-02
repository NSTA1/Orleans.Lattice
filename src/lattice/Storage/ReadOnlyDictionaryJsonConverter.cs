using System.Collections;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Orleans.Lattice;

/// <summary>
/// Reads read-only and immutable dictionaries that the JSON grain-storage
/// serializer wrote (issue #5579). Writing is left to Newtonsoft unchanged.
/// <para>
/// Orleans' JSON settings preserve object references, so every dictionary is
/// written with a <c>$id</c>. Newtonsoft then refuses to read one it cannot
/// populate in place - <see cref="ReadOnlyDictionary{TKey,TValue}"/>,
/// <see cref="ImmutableDictionary{TKey,TValue}"/> and their interfaces - with
/// "Cannot preserve reference to readonly dictionary", so any grain state
/// holding a non-empty one was unreadable after its first deactivation. This
/// converter reads the entries into a plain dictionary, builds the declared
/// type from them, and registers the <c>$id</c> itself, so a later
/// <c>$ref</c> to the same map still resolves.
/// </para>
/// </summary>
internal sealed class ReadOnlyDictionaryJsonConverter : JsonConverter
{
    /// <inheritdoc />
    public override bool CanWrite => false;

    /// <inheritdoc />
    public override bool CanConvert(Type objectType)
    {
        if (!objectType.IsGenericType)
        {
            return false;
        }

        var definition = objectType.GetGenericTypeDefinition();
        return definition == typeof(IReadOnlyDictionary<,>)
            || definition == typeof(ReadOnlyDictionary<,>)
            || definition == typeof(ImmutableDictionary<,>)
            || definition == typeof(IImmutableDictionary<,>)
            || definition == typeof(ImmutableSortedDictionary<,>);
    }

    /// <inheritdoc />
    public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer) =>
        throw new NotSupportedException("This converter only reads.");

    /// <inheritdoc />
    public override object? ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
        {
            return null;
        }

        var json = JObject.Load(reader);
        var resolver = serializer.ReferenceResolver;

        if (json.TryGetValue("$ref", StringComparison.Ordinal, out var reference) && resolver is not null)
        {
            return resolver.ResolveReference(serializer, (string)reference!);
        }

        var id = (string?)json["$id"];
        var typeName = (string?)json["$type"];
        json.Remove("$id");
        json.Remove("$type");

        var arguments = objectType.GetGenericArguments();
        var plainType = typeof(Dictionary<,>).MakeGenericType(arguments);
        var entries = json.ToObject(plainType, serializer);

        var result = Build(objectType, typeName, arguments, (IDictionary)(entries ?? Activator.CreateInstance(plainType)!));

        if (id is not null && resolver is not null)
        {
            resolver.AddReference(serializer, id, result);
        }

        return result;
    }

    private static object Build(Type objectType, string? typeName, Type[] arguments, IDictionary entries)
    {
        var definition = objectType.GetGenericTypeDefinition();
        var target = definition;

        // An interface member records the concrete type it held; honour the
        // three families a Lattice state can hold behind it.
        if (definition == typeof(IReadOnlyDictionary<,>) || definition == typeof(IImmutableDictionary<,>))
        {
            var head = typeName is null ? string.Empty : typeName[..IndexOfAny(typeName, '[', ',')];
            target = head switch
            {
                _ when head.EndsWith("ImmutableSortedDictionary`2", StringComparison.Ordinal) => typeof(ImmutableSortedDictionary<,>),
                _ when head.EndsWith("ImmutableDictionary`2", StringComparison.Ordinal) => typeof(ImmutableDictionary<,>),
                _ when definition == typeof(IImmutableDictionary<,>) => typeof(ImmutableDictionary<,>),
                _ when head.EndsWith("Generic.Dictionary`2", StringComparison.Ordinal) => typeof(Dictionary<,>),
                _ => typeof(ReadOnlyDictionary<,>),
            };
        }

        if (target == typeof(Dictionary<,>))
        {
            return entries;
        }

        if (target == typeof(ReadOnlyDictionary<,>))
        {
            return Activator.CreateInstance(typeof(ReadOnlyDictionary<,>).MakeGenericType(arguments), entries)!;
        }

        var factory = target == typeof(ImmutableSortedDictionary<,>) ? typeof(ImmutableSortedDictionary) : typeof(ImmutableDictionary);
        var createRange = factory.GetMethods()
            .Single(m => m.Name == nameof(ImmutableDictionary.CreateRange)
                && m.GetParameters().Length == 1)
            .MakeGenericMethod(arguments);
        return createRange.Invoke(null, [entries])!;
    }

    private static int IndexOfAny(string value, char first, char second)
    {
        var index = value.AsSpan().IndexOfAny(first, second);
        return index < 0 ? value.Length : index;
    }
}
