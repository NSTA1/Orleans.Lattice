using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reads the compiled exception-handling clauses of a package's gRPC facade services,
/// shared by the facade status-mapping contract guards.
/// </summary>
/// <remarks>
/// Reading compiled clauses is the only way to observe a <c>catch</c> by reflection.
/// Async dispatch compiles its try/catch into a generated state machine, so the
/// service's nested types are scanned alongside its own methods. Matching is by type
/// name, so callers need no compile-time reference to the product assemblies. A
/// filtered clause (<c>catch (Exception) when (...)</c>) carries no catch type and is
/// not counted: only a clause naming the type is.
/// </remarks>
internal static class GrpcFacadeCatchClauses
{
    private const string ServiceTypeSuffix = "GrpcService";

    /// <summary>
    /// Returns every concrete <c>*GrpcService</c> type in <paramref name="assembly"/>,
    /// failing when there is none so a guard can never pass vacuously.
    /// </summary>
    /// <param name="assembly">The package assembly to scan.</param>
    /// <returns>The services, ordered by full name.</returns>
    public static IReadOnlyList<Type> DiscoverServices(Assembly assembly)
    {
        var services = assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && t.Name.EndsWith(ServiceTypeSuffix, StringComparison.Ordinal))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        Assert.That(services, Is.Not.Empty,
            $"No '*{ServiceTypeSuffix}' types were discovered in {assembly.GetName().Name}; "
            + "the guard would be inert. Verify PackageAssembly.");

        return services;
    }

    /// <summary>
    /// Returns whether <paramref name="service"/>, or a type nested in it, has a typed
    /// catch clause for an exception type named <paramref name="exceptionTypeName"/>.
    /// </summary>
    /// <param name="service">The service type.</param>
    /// <param name="exceptionTypeName">The simple name of the caught exception type.</param>
    /// <returns><see langword="true"/> when such a clause exists.</returns>
    public static bool Catches(Type service, string exceptionTypeName)
    {
        const BindingFlags Members = BindingFlags.Instance | BindingFlags.Static
            | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        var candidates = new List<Type> { service };
        candidates.AddRange(service.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic));

        foreach (var type in candidates)
        {
            foreach (var method in type.GetMethods(Members))
            {
                MethodBody? body;
                try
                {
                    body = method.GetMethodBody();
                }
                catch (Exception)
                {
                    continue;
                }

                if (body is null)
                {
                    continue;
                }

                foreach (var clause in body.ExceptionHandlingClauses)
                {
                    if (clause.Flags != ExceptionHandlingClauseOptions.Clause)
                    {
                        continue;
                    }

                    Type? caught;
                    try
                    {
                        caught = clause.CatchType;
                    }
                    catch (Exception)
                    {
                        continue;
                    }

                    if (caught is not null && string.Equals(caught.Name, exceptionTypeName, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Returns one description per catch chain in <paramref name="service"/> (or a type
    /// nested in it) that handles <paramref name="anchorTypeName"/> but either has no
    /// typed clause for <paramref name="requiredTypeName"/>, or has one only after a
    /// typed clause for any of <paramref name="broaderTypeNames"/>, which would catch
    /// it first. A chain is the set of clauses that share one protected region.
    /// </summary>
    /// <param name="service">The service type.</param>
    /// <param name="anchorTypeName">A clause every facade chain carries, identifying the chains to audit.</param>
    /// <param name="requiredTypeName">The clause each audited chain must carry.</param>
    /// <param name="broaderTypeNames">Clauses that would catch the required type first if they preceded it.</param>
    /// <returns>The offending chains; empty when every audited chain is in order.</returns>
    public static IReadOnlyList<string> ChainsMissingOrShadowing(
        Type service, string anchorTypeName, string requiredTypeName, IReadOnlyCollection<string> broaderTypeNames)
    {
        const BindingFlags Members = BindingFlags.Instance | BindingFlags.Static
            | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        var offenders = new List<string>();
        var candidates = new List<Type> { service };
        candidates.AddRange(service.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic));
        foreach (var type in candidates)
        {
            foreach (var method in type.GetMethods(Members))
            {
                IList<ExceptionHandlingClause> clauses;
                try
                {
                    clauses = method.GetMethodBody()?.ExceptionHandlingClauses ?? [];
                }
                catch (Exception)
                {
                    continue;
                }

                foreach (var chain in clauses.GroupBy(c => (c.TryOffset, c.TryLength)))
                {
                    var names = chain.Select(CaughtName).ToList();
                    if (!names.Contains(anchorTypeName))
                    {
                        continue;
                    }

                    var required = names.IndexOf(requiredTypeName);
                    var broader = names.FindIndex(n => n is not null && broaderTypeNames.Contains(n));
                    if (required < 0)
                    {
                        offenders.Add($"{type.FullName}.{method.Name}: no catch clause for {requiredTypeName}.");
                    }
                    else if (broader >= 0 && broader < required)
                    {
                        offenders.Add($"{type.FullName}.{method.Name}: {names[broader]} is caught before {requiredTypeName}.");
                    }
                }
            }
        }

        return offenders;
    }

    private static string? CaughtName(ExceptionHandlingClause clause)
    {
        if (clause.Flags != ExceptionHandlingClauseOptions.Clause)
        {
            return null;
        }

        try
        {
            return clause.CatchType?.Name;
        }
        catch (Exception)
        {
            return null;
        }
    }
}
