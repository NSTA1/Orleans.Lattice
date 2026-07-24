using System.Collections.Frozen;
using System.Reflection;

namespace Orleans.Lattice.Views;

/// <summary>
/// Resolves a persisted projection type name (a
/// <see cref="RuntimeViewRegistration.ProjectionTypeName"/> loaded from durable
/// registry state) to a concrete <see cref="Type"/>, constrained to an
/// allow-list of projection types this silo is actually configured with.
/// <para>
/// The allow-list is the set of concrete, non-abstract types <b>already loaded
/// into this process</b> that implement <see cref="ILatticeViewProjection"/> or
/// <see cref="ILatticeAggregationProjection"/>. Those are exactly the projection
/// types the silo was built with (its own assembly plus the referenced
/// projection assemblies loaded at startup), so this narrows the re-hydration
/// trust surface from "any type in any loadable assembly that happens to
/// implement the projection interface" (which <c>Type.GetType</c> would resolve,
/// triggering a load of an arbitrary assembly named by an attacker-controlled
/// wire field) to "a projection this silo already has loaded". A persisted type
/// name that does not match a loaded projection type is rejected without ever
/// loading a new assembly or constructing the type.
/// </para>
/// <para>
/// This is a defence-in-depth hardening layered under the interface check: it
/// only matters against an attacker who can already write to the durable
/// registry / WAL store, and it never widens what a legitimate silo can
/// re-hydrate, because a projection assembly the host references is loaded before
/// the hosted re-hydration path runs.
/// </para>
/// </summary>
internal static class RuntimeViewProjectionAllowList
{
    private static readonly object SyncRoot = new();
    private static int _assembliesSnapshotCount = -1;
    private static FrozenDictionary<string, Type> _byName = FrozenDictionary<string, Type>.Empty;

    /// <summary>
    /// Resolves <paramref name="projectionTypeName"/> to a concrete projection
    /// type on the allow-list whose kind matches <paramref name="isAggregation"/>,
    /// or <see langword="null"/> when the name is not an allow-listed projection
    /// type of the expected kind.
    /// </summary>
    /// <param name="projectionTypeName">The persisted assembly-qualified type name.</param>
    /// <param name="isAggregation">
    /// <see langword="true"/> to require an <see cref="ILatticeAggregationProjection"/>;
    /// <see langword="false"/> to require an <see cref="ILatticeViewProjection"/>.
    /// </param>
    public static Type? Resolve(string projectionTypeName, bool isAggregation)
    {
        if (string.IsNullOrEmpty(projectionTypeName))
        {
            return null;
        }

        var allowList = GetAllowList();
        if (!allowList.TryGetValue(projectionTypeName, out var type))
        {
            // Version-bump resilience: the persisted name is the projection's
            // assembly-qualified name, which pins the assembly version at the time
            // the view was created. After the projection's assembly is upgraded
            // (a package bump), that exact AQN no longer matches the loaded type's
            // AQN, so the exact-name lookup above misses. The allow-list is also
            // keyed on the version-free full name, so fall back to that. This stays
            // within the loaded-projection set (it never loads a new assembly or
            // widens the trust surface); it only lets a still-loaded projection
            // re-hydrate a view persisted by an older build of the same assembly.
            var fullName = ExtractTypeFullName(projectionTypeName);
            if (fullName is null || !allowList.TryGetValue(fullName, out type))
            {
                return null;
            }
        }

        var required = isAggregation
            ? typeof(ILatticeAggregationProjection)
            : typeof(ILatticeViewProjection);
        return required.IsAssignableFrom(type) ? type : null;
    }

    /// <summary>
    /// Extracts the version-free type full name (the namespace-qualified type name
    /// without the trailing assembly identity) from an assembly-qualified name, or
    /// <see langword="null"/> when the name is already a bare full name (no
    /// assembly suffix to strip). The scan is bracket-depth aware so a generic
    /// type's argument list - whose own assembly-qualified names contain commas -
    /// is not mistaken for the type/assembly separator.
    /// </summary>
    private static string? ExtractTypeFullName(string assemblyQualifiedName)
    {
        var depth = 0;
        for (var i = 0; i < assemblyQualifiedName.Length; i++)
        {
            var c = assemblyQualifiedName[i];
            if (c == '[')
            {
                depth++;
            }
            else if (c == ']')
            {
                depth--;
            }
            else if (c == ',' && depth == 0)
            {
                return assemblyQualifiedName[..i].Trim();
            }
        }

        return null;
    }

    private static FrozenDictionary<string, Type> GetAllowList()
    {
        // Rebuild the allow-list only when the loaded-assembly set changes.
        // Re-hydration is a rare (startup / reactivation) path, so the scan
        // cost is negligible, and the snapshot is cached across the common
        // case where several views re-hydrate back to back.
        var currentCount = AppDomain.CurrentDomain.GetAssemblies().Length;
        var snapshot = Volatile.Read(ref _byName);
        if (Volatile.Read(ref _assembliesSnapshotCount) == currentCount)
        {
            return snapshot;
        }

        lock (SyncRoot)
        {
            var assemblies = AppDomain.CurrentDomain.GetAssemblies();
            if (_assembliesSnapshotCount == assemblies.Length)
            {
                return _byName;
            }

            var builder = new Dictionary<string, Type>(StringComparer.Ordinal);
            foreach (var assembly in assemblies)
            {
                if (assembly.IsDynamic)
                {
                    continue;
                }

                Type?[] types;
                try
                {
                    types = assembly.GetTypes();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    // A partially-loadable assembly still surfaces the types it
                    // could load; the rest are null and skipped below.
                    types = ex.Types;
                }
                catch (Exception)
                {
                    // A wholly unreadable assembly contributes nothing.
                    continue;
                }

                foreach (var type in types)
                {
                    if (type is not { IsClass: true, IsAbstract: false })
                    {
                        continue;
                    }

                    if (!typeof(ILatticeViewProjection).IsAssignableFrom(type)
                        && !typeof(ILatticeAggregationProjection).IsAssignableFrom(type))
                    {
                        continue;
                    }

                    // Key on the assembly-qualified name (the persisted shape)
                    // and, as a resilience fallback for an assembly-version bump
                    // between the persisting and re-hydrating processes, on the
                    // full name. The full-name fallback stays within the
                    // projection-implementer set, so it never broadens the trust
                    // surface beyond "a projection this silo has loaded".
                    if (type.AssemblyQualifiedName is { } aqn)
                    {
                        builder[aqn] = type;
                    }

                    if (type.FullName is { } fullName)
                    {
                        builder.TryAdd(fullName, type);
                    }
                }
            }

            _byName = builder.ToFrozenDictionary(StringComparer.Ordinal);
            Volatile.Write(ref _assembliesSnapshotCount, assemblies.Length);
            return _byName;
        }
    }
}
