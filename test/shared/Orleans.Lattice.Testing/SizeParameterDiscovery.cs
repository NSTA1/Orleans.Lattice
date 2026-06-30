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
            "maxNodes",
            "valuePreviewBudget",
            "previewBudget",
            "budget",
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
    /// Enumerates every public method on <paramref name="apiTypes"/> and, for
    /// each parameter that is a request DTO (a non-<see cref="string"/> reference
    /// type, for example a paging-request record), yields one
    /// <see cref="RequestSizePropertyTarget"/> per settable <see cref="int"/>
    /// property whose name matches <paramref name="sizeParameterNames"/>. This is
    /// the request-object analogue of <see cref="Discover"/>: a read facade whose
    /// caller-influenced sizes live on request records rather than on bare method
    /// parameters (such as the State-API <c>ILatticeStateQuery</c> surface, whose
    /// <c>CatalogRequest.PageSize</c> / <c>EntryScanRequest.PageSize</c> /
    /// <c>EntryHistoryRequest.Limit</c> sizes are properties) is audited by the
    /// same reflection-driven guard. Computed (get-only) properties such as a
    /// clamped <c>EffectivePageSize</c> carry no caller value and are skipped.
    /// Results are de-duplicated and ordered deterministically so the produced
    /// test table is stable across runs.
    /// </summary>
    /// <param name="apiTypes">The interfaces or classes whose request DTOs are audited.</param>
    /// <param name="sizeParameterNames">
    /// The size/limit property names to match, or <see langword="null"/> to use
    /// <see cref="DefaultSizeParameterNames"/>.
    /// </param>
    public static IReadOnlyList<RequestSizePropertyTarget> DiscoverRequestSizeProperties(
        IEnumerable<Type> apiTypes,
        IReadOnlySet<string>? sizeParameterNames = null)
    {
        ArgumentNullException.ThrowIfNull(apiTypes);
        var names = sizeParameterNames ?? DefaultSizeParameterNames;

        var targets = new List<RequestSizePropertyTarget>();
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
                    var requestType = parameter.ParameterType;
                    if (!IsRequestType(requestType))
                    {
                        continue;
                    }

                    foreach (var property in EnumerateSizeProperties(requestType, names))
                    {
                        // De-dupe on the full path: the same request type reached
                        // through two methods is two distinct targets (each method
                        // is exercised), but the same (method, request, property)
                        // triple appears once.
                        var key = $"{apiType.FullName}|{method}|{parameter.Name}|{requestType.FullName}|{property.Name}";
                        if (seen.Add(key))
                        {
                            targets.Add(new RequestSizePropertyTarget(apiType, method, parameter, requestType, property));
                        }
                    }
                }
            }
        }

        return targets
            .OrderBy(t => t.ApiType.FullName, StringComparer.Ordinal)
            .ThenBy(t => t.Method.Name, StringComparer.Ordinal)
            .ThenBy(t => t.RequestType.FullName, StringComparer.Ordinal)
            .ThenBy(t => t.SizeProperty.Name, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>
    /// Whether <paramref name="type"/> is treated as a request DTO worth
    /// reflecting size properties out of: a reference type that is not
    /// <see cref="string"/> and not an array. Value types (for example
    /// <see cref="System.Threading.CancellationToken"/>) and strings carry no
    /// size-named <see cref="int"/> properties, so excluding them keeps discovery
    /// off framework noise; any other class that happens to have none simply
    /// yields no targets.
    /// </summary>
    private static bool IsRequestType(Type type) =>
        type.IsClass && type != typeof(string) && !type.IsArray;

    /// <summary>
    /// Yields the public instance <see cref="int"/> properties of
    /// <paramref name="requestType"/> that have a public setter (an
    /// <see langword="init"/> accessor counts) and whose name matches
    /// <paramref name="names"/>. A get-only computed property has no setter and
    /// is skipped, so a clamped <c>Effective*</c> projection is never mistaken
    /// for a caller-supplied size.
    /// </summary>
    private static IEnumerable<PropertyInfo> EnumerateSizeProperties(
        Type requestType,
        IReadOnlySet<string> names)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

        foreach (var property in requestType.GetProperties(flags))
        {
            if (property.PropertyType == typeof(int)
                && property.Name is not null
                && names.Contains(property.Name)
                && property.GetMethod is not null
                && property.SetMethod is { IsPublic: true })
            {
                yield return property;
            }
        }
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
