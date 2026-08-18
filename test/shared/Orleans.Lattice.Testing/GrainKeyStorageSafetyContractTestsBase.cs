using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, product-agnostic guard proving that every static grain-key
/// composer in a package produces a key that is safe to use as an Orleans grain
/// <b>primary key</b> on a keyed storage backend. The composed key becomes the
/// grain identity, and Azure Table grain storage carries that identity into the
/// Partition/Row key columns and the request URL, both of which reject the
/// control characters <c>0x00-0x1F</c> and <c>0x7F-0x9F</c> and the characters
/// <c>/</c>, <c>\</c>, <c>#</c> and <c>?</c>. A composer that joins its parts
/// with such a character (the historical defect was an ASCII Unit Separator,
/// <c>0x1F</c>, in <c>LatticeCrossTreeReceiverGrain.ComputeKey</c>) yields a
/// grain that cannot activate on that backend - an opaque HTTP 400 that no
/// in-memory test storage reproduces, so the whole suite stays green while a
/// real Azure deployment fails.
/// <para>
/// Discovery is by reflection over a marker attribute (matched by name so this
/// library needs no compile-time product reference, exactly as the serializable
/// exception guard keys off <c>GenerateSerializerAttribute</c>): a package marks
/// each static key-composition method with an attribute named
/// <c>GrainKeyBuilderAttribute</c>, and a concrete subclass only names its
/// <see cref="PackageAssembly"/>. Because the method list is rebuilt from
/// reflection on every run, a newly added composer is audited automatically - a
/// future control-character delimiter fails <c>build-and-test</c> rather than
/// surfacing in production.
/// </para>
/// <para>
/// The base is <see langword="abstract"/> so it is never discovered on its own;
/// the inherited <c>[Test]</c> runs through the concrete subclass in the
/// consuming assembly.
/// </para>
/// </summary>
public abstract class GrainKeyStorageSafetyContractTestsBase
{
    /// <summary>Simple name of the marker attribute a package applies to its static grain-key composers.</summary>
    private const string MarkerAttributeName = "GrainKeyBuilderAttribute";

    /// <summary>
    /// Clean, storage-safe probe seeds (each yielding a value drawn only from
    /// <c>[A-Za-z0-9_-]</c>) parameterised by argument position. Any storage-unsafe
    /// character in a composed key built from these can only have been introduced
    /// by the composer itself - for example a control-character delimiter - which
    /// is exactly the defect under audit. The underscore seed guards against a
    /// guard that would wrongly flag a length-prefixed encoding's separator.
    /// </summary>
    private static readonly Func<int, string>[] ProbeSeeds =
    {
        _ => "abc",
        i => $"seg-{i}",
        _ => "Cluster-EastUS2-01",
        i => $"{i}0-x",
        _ => "a_b-c",
    };

    /// <summary>
    /// The package assembly whose marked static grain-key composers are audited.
    /// Only methods declared in this assembly are considered, so each package
    /// audits exactly its own composers.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// For every static method marked with the grain-key builder attribute in
    /// <see cref="PackageAssembly"/>, invokes it with representative clean inputs
    /// and asserts the composed key contains no character that a keyed storage
    /// backend (Azure Table grain storage in particular) rejects in a grain
    /// primary key.
    /// </summary>
    [Test]
    public void Every_grain_key_builder_produces_a_storage_safe_key()
    {
        var audited = 0;
        var offenders = new List<string>();

        foreach (var method in GrainKeyBuilders())
        {
            audited++;
            var label = $"{method.DeclaringType!.FullName}.{method.Name}";

            if (method.ReturnType != typeof(string))
            {
                offenders.Add($"{label}: a grain-key builder must return string");
                continue;
            }

            var parameters = method.GetParameters();
            foreach (var args in ProbeArgumentSets(parameters))
            {
                string key;
                try
                {
                    key = (string)method.Invoke(null, args)!;
                }
                catch (Exception ex)
                {
                    var inner = Unwrap(ex);
                    offenders.Add($"{label}({Describe(args)}): threw {inner.GetType().Name}: {inner.Message}");
                    continue;
                }

                if (FirstUnsafeChar(key) is { } unsafeChar)
                {
                    offenders.Add(
                        $"{label}({Describe(args)}) -> \"{Printable(key)}\": composed key contains "
                        + $"storage-unsafe character U+{(int)unsafeChar:X4}");
                }
            }
        }

        Assert.That(audited, Is.GreaterThan(0),
            $"No [{MarkerAttributeName}] grain-key builders were discovered in "
            + $"{PackageAssembly.GetName().Name}; the guard would be inert. Verify PackageAssembly, and "
            + "that every compound grain key is composed through a marked static builder rather than inline.");

        Assert.That(offenders, Is.Empty,
            "Every static grain-key composer must produce a key usable as an Orleans grain primary key on a "
            + "keyed storage backend: no control character (0x00-0x1F, 0x7F-0x9F) and none of '/', '\\', '#', "
            + "'?', because Azure Table grain storage carries the key into the Partition/Row key and the request "
            + "URL, both of which reject them. Join compound key parts with a length prefix or another "
            + "storage-safe, unambiguous encoding rather than a control-character delimiter. Offenders:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// The static methods in <see cref="PackageAssembly"/> marked with the
    /// grain-key builder attribute, ordered for a stable report. Non-public
    /// methods are included so an internal composer is audited too.
    /// </summary>
    protected IEnumerable<MethodInfo> GrainKeyBuilders() =>
        PackageAssembly.GetTypes()
            .SelectMany(t => t.GetMethods(
                BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            .Where(m => m.GetCustomAttributesData()
                .Any(d => d.AttributeType.Name == MarkerAttributeName))
            .OrderBy(m => $"{m.DeclaringType!.FullName}.{m.Name}", StringComparer.Ordinal);

    private static IEnumerable<object?[]> ProbeArgumentSets(ParameterInfo[] parameters)
    {
        foreach (var seed in ProbeSeeds)
        {
            var args = new object?[parameters.Length];
            for (var i = 0; i < parameters.Length; i++)
            {
                var type = parameters[i].ParameterType;
                args[i] = type == typeof(string)
                    ? seed(i)
                    : type.IsValueType ? Activator.CreateInstance(type) : null;
            }

            yield return args;
        }
    }

    private static char? FirstUnsafeChar(string value)
    {
        foreach (var c in value)
        {
            if (IsUnsafe(c))
            {
                return c;
            }
        }

        return null;
    }

    private static bool IsUnsafe(char c) =>
        c <= '\u001f'
        || (c >= '\u007f' && c <= '\u009f')
        || c is '/' or '\\' or '#' or '?';

    private static string Describe(object?[] args) =>
        string.Join(", ", args.Select(a => a is string s ? $"\"{Printable(s)}\"" : a?.ToString() ?? "null"));

    private static string Printable(string value)
    {
        var builder = new System.Text.StringBuilder(value.Length);
        foreach (var c in value)
        {
            builder.Append(c < ' ' || (c >= '\u007f' && c <= '\u009f')
                ? $"\\u{(int)c:X4}"
                : c.ToString());
        }

        return builder.ToString();
    }

    private static Exception Unwrap(Exception ex) =>
        ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;
}
