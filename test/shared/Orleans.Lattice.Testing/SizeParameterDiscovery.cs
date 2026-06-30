using System.Reflection;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reflection helpers that enumerate the public methods of a set of API types
/// and surface every size- or limit-like <see cref="int"/> parameter as a
/// <see cref="SizeParameterTarget"/>. This is the reusable core of the
/// public-API size contract guard: discovery is shared so that a newly added
/// size parameter on any audited type is picked up automatically, rather than
/// re-listed by hand per call site (the failure mode that let a sibling of the
/// cursor pre-allocation slip through review for a release).
/// </summary>
public static class SizeParameterDiscovery
{
    /// <summary>
    /// The pathological boundary values every discovered size parameter is
    /// exercised with. <see cref="int.MaxValue"/> is the amplification case
    /// (and, after a <c>+ 1</c>, the overflow-prone boundary);
    /// <see cref="int.MinValue"/>, <c>0</c> and <c>-1</c> are the
    /// negative/zero rejection cases.
    /// </summary>
    public static readonly IReadOnlyList<int> PathologicalBoundaryValues =
        [int.MaxValue, int.MinValue, 0, -1];

    /// <summary>
    /// The default set of parameter names treated as size/limit-like. Matching
    /// is case-insensitive and exact (not a substring) so an unrelated
    /// structural parameter such as <c>newMaxLeafKeys</c> is not swept in.
    /// Consumers can supply their own set to <see cref="Discover"/>.
    /// </summary>
    public static readonly IReadOnlySet<string> DefaultSizeParameterNames =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "pageSize",
            "limit",
            "maxToDelete",
            "count",
            "capacity",
            "size",
            "take",
            "batchSize",
            "maxCount",
            "maxResults",
        };

    /// <summary>
    /// Enumerates every public instance method (including inherited interface
    /// members) on <paramref name="apiTypes"/> and yields one
    /// <see cref="SizeParameterTarget"/> per <see cref="int"/> parameter whose
    /// name matches <paramref name="sizeParameterNames"/>. Property accessors
    /// and other compiler-generated <c>special name</c> methods are skipped.
    /// Results are de-duplicated and ordered deterministically so the produced
    /// test table is stable across runs.
    /// </summary>
    /// <param name="apiTypes">The interfaces or classes to audit.</param>
    /// <param name="sizeParameterNames">
    /// The size/limit parameter names to match, or <see langword="null"/> to use
    /// <see cref="DefaultSizeParameterNames"/>.
    /// </param>
    public static IReadOnlyList<SizeParameterTarget> Discover(
        IEnumerable<Type> apiTypes,
        IReadOnlySet<string>? sizeParameterNames = null)
    {
        ArgumentNullException.ThrowIfNull(apiTypes);
        var names = sizeParameterNames ?? DefaultSizeParameterNames;

        var targets = new List<SizeParameterTarget>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var apiType in apiTypes)
        {
            ArgumentNullException.ThrowIfNull(apiType, nameof(apiTypes));

            foreach (var method in EnumerateMethods(apiType))
            {
                if (method.IsSpecialName)
                {
                    continue;
                }

                foreach (var parameter in method.GetParameters())
                {
                    if (parameter.ParameterType != typeof(int)
                        || parameter.Name is null
                        || !names.Contains(parameter.Name))
                    {
                        continue;
                    }

                    // De-dupe on the full signature: a method reachable via
                    // both the declaring interface and an inherited interface
                    // would otherwise appear twice.
                    var key = $"{apiType.FullName}|{method}|{parameter.Name}";
                    if (seen.Add(key))
                    {
                        targets.Add(new SizeParameterTarget(apiType, method, parameter));
                    }
                }
            }
        }

        return targets
            .OrderBy(t => t.ApiType.FullName, StringComparer.Ordinal)
            .ThenBy(t => t.Method.Name, StringComparer.Ordinal)
            .ThenBy(t => t.Parameter.Name, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>
    /// Returns the public instance methods declared on <paramref name="apiType"/>
    /// plus, when it is an interface, the methods of every interface it extends
    /// (which <see cref="Type.GetMethods()"/> does not include for interfaces).
    /// </summary>
    private static IEnumerable<MethodInfo> EnumerateMethods(Type apiType)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

        foreach (var method in apiType.GetMethods(flags))
        {
            yield return method;
        }

        if (apiType.IsInterface)
        {
            foreach (var inherited in apiType.GetInterfaces())
            {
                foreach (var method in inherited.GetMethods(flags))
                {
                    yield return method;
                }
            }
        }
    }
}
