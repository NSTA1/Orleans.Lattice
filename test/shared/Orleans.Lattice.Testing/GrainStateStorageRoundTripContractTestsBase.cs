using System.Collections;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable guard proving every grain persistent-state type a package declares
/// survives the grain-storage round trip a JSON provider such as Azure Table
/// storage performs on every reactivation (issue #5579).
/// <para>
/// Memory storage and the Orleans binary serializer never round-trip state
/// through JSON, so a state type the JSON provider cannot rebuild passes every
/// cluster test and then becomes unreadable in production after the first
/// deactivation. The defect that motivated this guard was a collection
/// expression (<c>[.. values]</c>) assigned to an <c>IReadOnlyList&lt;T&gt;</c>
/// member: the compiler materialises it as an internal
/// <c>&lt;&gt;z__ReadOnly*</c> type whose name the JSON serializer records and
/// cannot construct.
/// </para>
/// <para>
/// The population is discovered by reflection: every type <c>T</c> that a class
/// in <see cref="PackageAssembly"/> takes as an <c>IPersistentState&lt;T&gt;</c>
/// constructor parameter. Each is built with every persisted member set to a
/// non-default value and every read-only collection interface member holding a
/// compiler-synthesized collection, once per synthesized shape, then
/// round-tripped through <see cref="RoundTrip{T}"/> and compared member by
/// member. Every offender is collected and reported together, and the guard
/// fails when it discovers nothing, so it cannot pass vacuously.
/// </para>
/// <para>
/// Like the other bases here it references no Orleans type at compile time; the
/// few Orleans value types a state can hold are built through reflection by name.
/// </para>
/// </summary>
public abstract class GrainStateStorageRoundTripContractTestsBase
{
    private const int MaxDepth = 6;

    private static readonly MethodInfo RoundTripDefinition =
        typeof(GrainStateStorageRoundTripContractTestsBase).GetMethod(
            nameof(RoundTrip), BindingFlags.Instance | BindingFlags.NonPublic)!;

    private int seed;

    /// <summary>The package assembly whose grain persistent-state types are audited.</summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// Writes <paramref name="state"/> and reads it back through the grain-storage
    /// serializer a silo hands its JSON storage provider.
    /// </summary>
    /// <typeparam name="T">The persisted state type.</typeparam>
    /// <param name="state">The state to round-trip.</param>
    /// <returns>The state read back.</returns>
    protected abstract T RoundTrip<T>(T state);

    /// <summary>The shapes a collection expression can take behind a read-only interface.</summary>
    public enum CollectionShape
    {
        /// <summary><c>[.. array]</c>: a known-length spread.</summary>
        ReadOnlyArray,

        /// <summary><c>[item]</c>: a single element.</summary>
        SingleElement,

        /// <summary><c>[.. sequence]</c>: an unknown-length spread.</summary>
        ReadOnlyList,
    }

    /// <summary>
    /// Every discovered persistent-state type round-trips through JSON grain
    /// storage with all of its persisted members intact, in every collection shape.
    /// </summary>
    [Test]
    public void Every_persisted_grain_state_round_trips_through_json_grain_storage()
    {
        var stateTypes = PersistentStateTypes();
        var offenders = new List<string>();

        TestContext.Progress.WriteLine(
            $"{GetType().Name}: discovered {stateTypes.Count} persistent-state type(s) in {PackageAssembly.GetName().Name}: "
            + string.Join(", ", stateTypes.Select(t => t.FullName)));

        foreach (var stateType in stateTypes)
        {
            foreach (var shape in Enum.GetValues<CollectionShape>())
            {
                var problem = Probe(stateType, shape);
                if (problem is not null)
                {
                    offenders.Add($"{stateType.FullName} [{shape}]: {problem}");
                }
            }
        }

        Assert.That(stateTypes, Is.Not.Empty,
            $"No IPersistentState<T> state types were discovered in {PackageAssembly.GetName().Name}; "
            + "the guard would be inert. Verify PackageAssembly.");

        Assert.That(offenders, Is.Empty,
            $"{offenders.Count} of {stateTypes.Count * Enum.GetValues<CollectionShape>().Length} probes failed. "
            + "Every grain persistent-state type must round-trip through the JSON grain-storage serializer a silo "
            + "uses for Azure Table storage. Persist concrete collection types (an array, List<T>, "
            + "ReadOnlyCollection<T>) rather than collection expressions typed as an interface, and give every "
            + "persisted member a public setter. Offenders:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// The state types taken as <c>IPersistentState&lt;T&gt;</c> by any constructor
    /// declared in <see cref="PackageAssembly"/>, ordered for a stable report.
    /// </summary>
    protected IReadOnlyList<Type> PersistentStateTypes()
    {
        Type[] types;
        try
        {
            types = PackageAssembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            types = ex.Types.OfType<Type>().ToArray();
        }

        return types
            .Where(t => t.IsClass && !t.IsAbstract)
            .SelectMany(t => t.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
            .SelectMany(c => c.GetParameters())
            .Select(p => p.ParameterType)
            .Where(p => p.IsGenericType
                && !p.ContainsGenericParameters
                && p.GetGenericTypeDefinition().FullName == "Orleans.Runtime.IPersistentState`1")
            .Select(p => p.GetGenericArguments()[0])
            .Distinct()
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();
    }

    private string? Probe(Type stateType, CollectionShape shape)
    {
        object original;
        try
        {
            this.seed = 0;
            original = Create(stateType, 0, shape) ?? throw new InvalidOperationException("no instance was built");
        }
        catch (Exception ex)
        {
            return $"could not build a probe instance: {Describe(Unwrap(ex))}";
        }

        object? reloaded;
        try
        {
            reloaded = RoundTripDefinition.MakeGenericMethod(stateType).Invoke(this, [original]);
        }
        catch (Exception ex)
        {
            return $"round trip threw {Describe(Unwrap(ex))}";
        }

        var expected = Render(original, 0);
        var actual = Render(reloaded, 0);
        return string.Equals(expected, actual, StringComparison.Ordinal)
            ? null
            : $"read back differently at {FirstDifference(expected, actual)}";
    }

    private object? Create(Type type, int depth, CollectionShape shape)
    {
        if (depth > MaxDepth)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
        }

        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null)
        {
            return Create(underlying, depth, shape);
        }

        if (TryCreateLeaf(type, out var leaf))
        {
            return leaf;
        }

        if (type.IsArray)
        {
            var element = type.GetElementType()!;
            var array = Array.CreateInstance(element, 2);
            for (var i = 0; i < array.Length; i++)
            {
                array.SetValue(Create(element, depth + 1, shape), i);
            }

            return array;
        }

        if (TryCreateCollection(type, depth, shape, out var collection))
        {
            return collection;
        }

        if (type.IsInterface || type.IsAbstract)
        {
            var concrete = ConcreteImplementation(type);
            return concrete is null ? null : Create(concrete, depth, shape);
        }

        if (type == typeof(object) || typeof(Delegate).IsAssignableFrom(type))
        {
            return null;
        }

        var instance = Construct(type, depth, shape);
        foreach (var member in PersistedMembers(type))
        {
            var value = Create(MemberType(member), depth + 1, shape);
            SetMember(instance, member, value);
        }

        return instance;
    }

    private bool TryCreateLeaf(Type type, out object? value)
    {
        var n = ++this.seed;
        value = null;

        if (type == typeof(string))
        {
            value = $"v{n}";
        }
        else if (type == typeof(bool))
        {
            value = true;
        }
        else if (type == typeof(char))
        {
            value = 'c';
        }
        else if (type.IsEnum)
        {
            var values = Enum.GetValues(type);
            value = values.Length == 0 ? Activator.CreateInstance(type) : values.GetValue(values.Length - 1);
        }
        else if (type.IsPrimitive || type == typeof(decimal))
        {
            value = Convert.ChangeType(n % 100 + 1, type, CultureInfo.InvariantCulture);
        }
        else if (type == typeof(DateTime))
        {
            value = new DateTime(2026, 10, 1, 0, 0, n % 60, DateTimeKind.Utc);
        }
        else if (type == typeof(DateTimeOffset))
        {
            value = new DateTimeOffset(2026, 10, 1, 0, 0, n % 60, TimeSpan.Zero);
        }
        else if (type == typeof(TimeSpan))
        {
            value = TimeSpan.FromSeconds(n);
        }
        else if (type == typeof(Guid))
        {
            value = new Guid(n, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        }
        else if (type == typeof(Uri))
        {
            value = new Uri($"https://example.test/{n}");
        }
        else if (type == typeof(IPAddress))
        {
            value = IPAddress.Loopback;
        }
        else if (type == typeof(IPEndPoint))
        {
            value = new IPEndPoint(IPAddress.Loopback, 11000 + n);
        }
        else if (type.FullName == "Orleans.Runtime.GrainId")
        {
            value = Invoke(type, "Create", [typeof(string), typeof(string)], ["probe", $"key{n}"]);
        }
        else if (type.FullName == "Orleans.Runtime.SiloAddress")
        {
            value = Invoke(type, "New", [typeof(IPEndPoint), typeof(int)], [new IPEndPoint(IPAddress.Loopback, 11000 + n), n]);
        }
        else if (type.FullName is "Orleans.Runtime.GrainType" or "Orleans.Runtime.IdSpan" or "Orleans.Runtime.GrainInterfaceType")
        {
            value = Invoke(type, "Create", [typeof(string)], [$"probe{n}"]);
        }
        else if (type.FullName == "Orleans.Runtime.MembershipVersion")
        {
            value = Activator.CreateInstance(type, (long)n);
        }
        else
        {
            return false;
        }

        return true;
    }

    private bool TryCreateCollection(Type type, int depth, CollectionShape shape, out object? value)
    {
        value = null;
        if (!type.IsGenericType)
        {
            return false;
        }

        var definition = type.GetGenericTypeDefinition();
        var arguments = type.GetGenericArguments();

        if (definition == typeof(IReadOnlyList<>)
            || definition == typeof(IReadOnlyCollection<>)
            || definition == typeof(IEnumerable<>))
        {
            value = Synthesized(arguments[0], depth, shape);
            return true;
        }

        if (definition == typeof(IReadOnlyDictionary<,>) || definition == typeof(IDictionary<,>))
        {
            var dictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(arguments))!;
            FillDictionary(dictionary, arguments, depth, shape);
            value = definition == typeof(IReadOnlyDictionary<,>)
                ? Activator.CreateInstance(typeof(System.Collections.ObjectModel.ReadOnlyDictionary<,>).MakeGenericType(arguments), dictionary)
                : dictionary;
            return true;
        }

        if (definition == typeof(IList<>) || definition == typeof(ICollection<>))
        {
            return TryCreateCollection(typeof(List<>).MakeGenericType(arguments), depth, shape, out value);
        }

        if (definition == typeof(ISet<>) || definition == typeof(IReadOnlySet<>))
        {
            return TryCreateCollection(typeof(HashSet<>).MakeGenericType(arguments), depth, shape, out value);
        }

        if (type.Namespace == "System.Collections.Immutable")
        {
            value = CreateImmutable(type, definition, arguments, depth, shape);
            return value is not null;
        }

        if (type.IsInterface || type.IsAbstract || type.GetConstructor(Type.EmptyTypes) is null)
        {
            return false;
        }

        if (Implements(type, typeof(IDictionary<,>)) is { } dictionaryInterface && typeof(IDictionary).IsAssignableFrom(type))
        {
            var dictionary = (IDictionary)Activator.CreateInstance(type)!;
            FillDictionary(dictionary, dictionaryInterface.GetGenericArguments(), depth, shape);
            value = dictionary;
            return true;
        }

        if (Implements(type, typeof(ICollection<>)) is { } collectionInterface)
        {
            var instance = Activator.CreateInstance(type)!;
            var add = collectionInterface.GetMethod("Add")!;
            var element = collectionInterface.GetGenericArguments()[0];
            add.Invoke(instance, [Create(element, depth + 1, shape)]);
            add.Invoke(instance, [Create(element, depth + 1, shape)]);
            value = instance;
            return true;
        }

        foreach (var adder in new[] { "Enqueue", "Push" })
        {
            var method = type.GetMethod(adder, [arguments[0]]);
            if (arguments.Length == 1 && method is not null)
            {
                var instance = Activator.CreateInstance(type)!;
                method.Invoke(instance, [Create(arguments[0], depth + 1, shape)]);
                method.Invoke(instance, [Create(arguments[0], depth + 1, shape)]);
                value = instance;
                return true;
            }
        }

        return false;
    }

    private void FillDictionary(IDictionary dictionary, Type[] arguments, int depth, CollectionShape shape)
    {
        for (var i = 0; i < 2; i++)
        {
            var key = Create(arguments[0], depth + 1, shape);
            if (key is not null && !dictionary.Contains(key))
            {
                dictionary.Add(key, Create(arguments[1], depth + 1, shape));
            }
        }
    }

    private object? CreateImmutable(Type type, Type definition, Type[] arguments, int depth, CollectionShape shape)
    {
        var name = definition.Name[..definition.Name.IndexOf('`', StringComparison.Ordinal)];
        if (definition.IsInterface && name.StartsWith("IImmutable", StringComparison.Ordinal))
        {
            name = name[1..];
        }

        var factory = type.Assembly.GetType($"System.Collections.Immutable.{name}");
        var createRange = factory?.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(m => m.Name == "CreateRange"
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == arguments.Length
                && m.GetParameters().Length == 1);
        if (createRange is null)
        {
            return null;
        }

        IList items;
        if (arguments.Length == 2)
        {
            var pair = typeof(KeyValuePair<,>).MakeGenericType(arguments);
            items = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(pair))!;
            var dictionary = new Dictionary<object, object?>();
            for (var i = 0; i < 2; i++)
            {
                var key = Create(arguments[0], depth + 1, shape);
                if (key is not null && dictionary.TryAdd(key, null))
                {
                    items.Add(Activator.CreateInstance(pair, key, Create(arguments[1], depth + 1, shape)));
                }
            }
        }
        else
        {
            items = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(arguments[0]))!;
            items.Add(Create(arguments[0], depth + 1, shape));
            items.Add(Create(arguments[0], depth + 1, shape));
        }

        return createRange.MakeGenericMethod(arguments).Invoke(null, [items]);
    }

    // A collection expression typed as a read-only interface, built the way the
    // compiler builds it in production code.
    private object? Synthesized(Type element, int depth, CollectionShape shape)
    {
        var template = shape switch
        {
            CollectionShape.ReadOnlyArray => ReadOnlyArrayTemplate(),
            CollectionShape.SingleElement => SingleElementTemplate(),
            _ => ReadOnlyListTemplate(),
        };

        var count = shape == CollectionShape.SingleElement ? 1 : 2;
        var items = Array.CreateInstance(element, count);
        for (var i = 0; i < count; i++)
        {
            items.SetValue(Create(element, depth + 1, shape), i);
        }

        var synthesized = template.GetType();
        if (!synthesized.IsGenericType)
        {
            return items;
        }

        var closed = synthesized.GetGenericTypeDefinition().MakeGenericType(element);
        var constructor = closed.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Single(c => c.GetParameters().Length == 1);
        var parameter = constructor.GetParameters()[0].ParameterType;

        object argument;
        if (parameter.IsArray)
        {
            argument = items;
        }
        else if (parameter == element)
        {
            argument = items.GetValue(0)!;
        }
        else
        {
            var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(element))!;
            foreach (var item in items)
            {
                list.Add(item);
            }

            argument = list;
        }

        return constructor.Invoke([argument]);
    }

    private static IReadOnlyList<object> ReadOnlyArrayTemplate()
    {
        IReadOnlyList<object> values = [.. new object[] { 1, 2 }];
        return values;
    }

    private static IReadOnlyList<object> SingleElementTemplate()
    {
        IReadOnlyList<object> values = [1];
        return values;
    }

    private static IReadOnlyList<object> ReadOnlyListTemplate()
    {
        IReadOnlyList<object> values = [.. Enumerable.Range(0, 2).Select(i => (object)i)];
        return values;
    }

    private object Construct(Type type, int depth, CollectionShape shape)
    {
        if (type.IsValueType)
        {
            return Activator.CreateInstance(type)!;
        }

        var constructors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(c => !c.GetParameters().Any(p => p.ParameterType == type))
            .OrderBy(c => c.GetParameters().Length)
            .ToList();

        var parameterless = constructors.FirstOrDefault(c => c.GetParameters().Length == 0);
        if (parameterless is not null)
        {
            return parameterless.Invoke(null);
        }

        var constructor = constructors.LastOrDefault(c => c.IsPublic) ?? constructors.LastOrDefault();
        if (constructor is null)
        {
            return RuntimeHelpers.GetUninitializedObject(type);
        }

        var arguments = constructor.GetParameters()
            .Select(p => Create(p.ParameterType, depth + 1, shape))
            .ToArray();
        return constructor.Invoke(arguments);
    }

    private Type? ConcreteImplementation(Type type)
    {
        IEnumerable<Assembly> assemblies = [type.Assembly, PackageAssembly];
        return assemblies
            .Distinct()
            .Where(a => a.GetName().Name?.StartsWith("Orleans.Lattice", StringComparison.Ordinal) == true)
            .SelectMany(SafeTypes)
            .Where(t => !t.IsAbstract
                && !t.IsInterface
                && !t.ContainsGenericParameters
                && type.IsAssignableFrom(t)
                && t.GetCustomAttributes(inherit: false).Any(a => a.GetType().Name == "GenerateSerializerAttribute"))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .FirstOrDefault();
    }

    private static IEnumerable<Type> SafeTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.OfType<Type>();
        }
    }

    // The members a grain-storage serializer persists: those carrying an Orleans
    // [Id] when the type declares any, otherwise its public settable properties.
    private static IReadOnlyList<MemberInfo> PersistedMembers(Type type)
    {
        var members = new List<MemberInfo>();
        for (var current = type; current is not null && current != typeof(object) && current != typeof(ValueType); current = current.BaseType)
        {
            const BindingFlags Declared = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
            members.AddRange(current.GetProperties(Declared).Where(p => HasId(p) && p.GetIndexParameters().Length == 0));
            members.AddRange(current.GetFields(Declared).Where(HasId));
        }

        if (members.Count > 0)
        {
            return members;
        }

        return type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.CanRead && p.SetMethod is { IsPublic: true } && p.GetIndexParameters().Length == 0)
            .ToList();
    }

    private static bool HasId(MemberInfo member) =>
        member.GetCustomAttributes(inherit: false).Any(a => a.GetType().FullName == "Orleans.IdAttribute");

    private static Type MemberType(MemberInfo member) =>
        member is PropertyInfo property ? property.PropertyType : ((FieldInfo)member).FieldType;

    private static void SetMember(object instance, MemberInfo member, object? value)
    {
        if (member is FieldInfo field)
        {
            field.SetValue(instance, value);
            return;
        }

        var property = (PropertyInfo)member;
        if (property.SetMethod is not null)
        {
            property.SetValue(instance, value);
            return;
        }

        property.DeclaringType!
            .GetField($"<{property.Name}>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)
            ?.SetValue(instance, value);
    }

    private static object? GetMember(object instance, MemberInfo member) =>
        member is PropertyInfo property ? property.GetValue(instance) : ((FieldInfo)member).GetValue(instance);

    // A deterministic rendering of everything a reader can observe, independent
    // of the concrete collection types, so a state that reads back with equal
    // contents in a different collection type compares equal.
    private static string Render(object? value, int depth)
    {
        if (value is null)
        {
            return "null";
        }

        if (depth > MaxDepth + 2)
        {
            return "...";
        }

        var type = value.GetType();
        switch (value)
        {
            case string s:
                return '"' + s + '"';
            case DateTimeOffset dto:
                return dto.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
            case DateTime dt:
                return dt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
            case IFormattable formattable when type.IsPrimitive || type.IsEnum || value is decimal or TimeSpan or Guid:
                return formattable.ToString(null, CultureInfo.InvariantCulture);
            case bool or char or Uri or IPAddress or IPEndPoint:
                return value.ToString()!;
            case byte[] bytes:
                return Convert.ToBase64String(bytes);
            case IDictionary dictionary:
                return "{" + string.Join(",", dictionary.Cast<DictionaryEntry>()
                    .Select(e => Render(e.Key, depth + 1) + "=" + Render(e.Value, depth + 1))
                    .Order(StringComparer.Ordinal)) + "}";
        }

        if (type.Namespace == "Orleans.Runtime")
        {
            return value.ToString()!;
        }

        if (value is IEnumerable sequence)
        {
            var items = sequence.Cast<object?>().Select(i => Render(i, depth + 1));
            var keyed = Implements(type, typeof(IEnumerable<>))?.GetGenericArguments()[0] is { IsGenericType: true } element
                && element.GetGenericTypeDefinition() == typeof(KeyValuePair<,>);
            var unordered = keyed || Implements(type, typeof(ISet<>)) is not null || Implements(type, typeof(IReadOnlySet<>)) is not null;
            return "[" + string.Join(",", unordered ? items.Order(StringComparer.Ordinal) : items) + "]";
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
        {
            return Render(type.GetProperty("Key")!.GetValue(value), depth + 1) + "=" + Render(type.GetProperty("Value")!.GetValue(value), depth + 1);
        }

        var members = PersistedMembers(type);
        if (members.Count == 0)
        {
            return value.ToString() ?? type.Name;
        }

        var builder = new StringBuilder("{");
        foreach (var member in members.OrderBy(m => m.Name, StringComparer.Ordinal))
        {
            builder.Append(member.Name).Append('=').Append(Render(GetMember(value, member), depth + 1)).Append(';');
        }

        return builder.Append('}').ToString();
    }

    private static Type? Implements(Type type, Type genericInterface) =>
        type.IsGenericType && type.GetGenericTypeDefinition() == genericInterface
            ? type
            : type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == genericInterface);

    private static string FirstDifference(string expected, string actual)
    {
        var index = 0;
        while (index < expected.Length && index < actual.Length && expected[index] == actual[index])
        {
            index++;
        }

        var start = Math.Max(0, index - 80);
        static string Slice(string s, int from) => s.Length <= from ? string.Empty : s.Substring(from, Math.Min(200, s.Length - from));
        return $"offset {index}; expected ...{Slice(expected, start)} but read ...{Slice(actual, start)}";
    }

    private static object? Invoke(Type type, string name, Type[] parameters, object?[] arguments) =>
        type.GetMethod(name, BindingFlags.Public | BindingFlags.Static, parameters)?.Invoke(null, arguments);

    private static Exception Unwrap(Exception ex)
    {
        while (ex is TargetInvocationException { InnerException: { } inner })
        {
            ex = inner;
        }

        return ex;
    }

    private static string Describe(Exception ex)
    {
        var message = ex.Message.ReplaceLineEndings(" ");
        return $"{ex.GetType().Name}: {(message.Length > 400 ? message[..400] + "..." : message)}";
    }
}
