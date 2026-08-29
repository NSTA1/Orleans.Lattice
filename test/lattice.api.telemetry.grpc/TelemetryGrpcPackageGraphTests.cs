using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Guards the <b>load-bearing constraint</b> of this binding: the client a head
/// consumes telemetry through must stay client-safe. Its reference closure must
/// contain no MCP package and no Orleans grain interface, so a desktop or web
/// client can take telemetry without dragging in the MCP server surface - which is
/// the entire reason the PromQL machinery was hoisted into the neutral facade.
/// </summary>
/// <remarks>
/// <para>
/// <b>A green build proves nothing here.</b> A sibling package in this same epic
/// was found reaching the neutral facade <em>transitively</em> through the MCP
/// binding while compiling perfectly, so the graph has to be asserted explicitly.
/// The same review also found that checking <c>ProjectReference</c> alone leaves a
/// gap: a <c>PackageReference</c> to a published MCP package restores the same
/// surface into a consuming head. Both are asserted here, and both are walked
/// <em>transitively</em>, because a re-coupling one hop away is exactly as fatal
/// as a direct one.
/// </para>
/// <para>
/// The project-file assertions are the load-bearing ones: they pin the shipped
/// package graph, which the C# compiler's reference pruning would hide from an
/// IL-only check. The assembly assertions catch a re-coupling introduced in code
/// rather than in the project file.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TelemetryGrpcPackageGraphTests
{
    private const string ContractPackage = "Orleans.Lattice.Api.Abstractions";
    private const string BindingAssemblyName = "Orleans.Lattice.Api.Telemetry.Grpc";
    private const string NeutralFacade = "Orleans.Lattice.Api.Telemetry";
    private const string McpPrefix = "Orleans.Lattice.Api.Mcp";

    private static readonly Assembly BindingAssembly = typeof(LatticeTelemetryApiGrpcClient).Assembly;

    private static readonly Regex ProjectReferenceRegex = new(
        "<ProjectReference\\s+Include\\s*=\\s*\"(?<path>[^\"]+)\"",
        RegexOptions.CultureInvariant);

    private static readonly Regex PackageReferenceRegex = new(
        "<PackageReference\\s+Include\\s*=\\s*\"(?<id>[^\"]+)\"",
        RegexOptions.CultureInvariant);

    private static string BindingProjectPath() => Path.Combine(
        HygieneRepository.FindRepoRoot(),
        "src",
        "lattice.api.telemetry.grpc",
        "Orleans.Lattice.Api.Telemetry.Grpc.csproj");

    private static IReadOnlyList<string> DirectProjectReferences()
    {
        var csproj = BindingProjectPath();
        Assert.That(File.Exists(csproj), Is.True, $"Expected the package project file at {csproj}.");

        return [.. ProjectReferenceRegex
            .Matches(File.ReadAllText(csproj))
            .Select(match => match.Groups["path"].Value.Replace('\\', '/'))];
    }

    /// <summary>
    /// Walks the whole transitive <c>ProjectReference</c> closure from the binding
    /// project, returning every reachable project file path. A re-coupling that
    /// hides one hop away is caught here rather than by the direct-reference check.
    /// </summary>
    private static IReadOnlyCollection<string> TransitiveProjectClosure()
    {
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pending = new Stack<string>();
        pending.Push(Path.GetFullPath(BindingProjectPath()));

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            if (!visited.Add(current) || !File.Exists(current))
            {
                continue;
            }

            var directory = Path.GetDirectoryName(current)!;
            foreach (Match match in ProjectReferenceRegex.Matches(File.ReadAllText(current)))
            {
                pending.Push(Path.GetFullPath(Path.Combine(directory, match.Groups["path"].Value)));
            }
        }

        // The seed is the binding itself; the closure of interest is what it drags in.
        visited.Remove(Path.GetFullPath(BindingProjectPath()));
        return visited;
    }

    /// <summary>
    /// Every <c>PackageReference</c> id declared anywhere in the transitive project
    /// closure, plus the binding project's own.
    /// </summary>
    private static IReadOnlyCollection<string> TransitivePackageReferences()
    {
        var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var project in TransitiveProjectClosure().Append(Path.GetFullPath(BindingProjectPath())))
        {
            if (!File.Exists(project))
            {
                continue;
            }

            foreach (Match match in PackageReferenceRegex.Matches(File.ReadAllText(project)))
            {
                ids.Add(match.Groups["id"].Value);
            }
        }

        return ids;
    }

    [Test]
    public void The_binding_references_only_the_shared_contract_package()
        => Assert.That(
            DirectProjectReferences(),
            Is.EqualTo(new[] { $"../lattice.api.abstractions/{ContractPackage}.csproj" }),
            "The client head takes exactly the contract and nothing else. Every additional project "
            + "reference is a new dependency forced onto every consumer of the client.");

    [Test]
    public void The_binding_references_no_mcp_project_directly()
    {
        var offenders = DirectProjectReferences()
            .Where(path => path.Contains("mcp", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "A client head must be able to consume telemetry without the MCP server surface.");
    }

    [Test]
    public void The_binding_reaches_no_mcp_project_transitively()
    {
        var offenders = TransitiveProjectClosure()
            .Where(path => Path.GetFileName(path).Contains(".Mcp.", StringComparison.OrdinalIgnoreCase))
            .Select(Path.GetFileName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "A transitive MCP dependency compiles perfectly and ships the MCP surface anyway. "
            + "Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void The_binding_reaches_no_mcp_nuget_package_transitively()
    {
        // A ProjectReference is not the only way an MCP dependency could return: a
        // PackageReference to a published Orleans.Lattice.Api.Mcp* package would
        // restore the same surface into a consuming head.
        var offenders = TransitivePackageReferences()
            .Where(id => id.StartsWith(McpPrefix, StringComparison.OrdinalIgnoreCase))
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray();

        Assert.That(offenders, Is.Empty, "Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void The_binding_reaches_no_neutral_facade_implementation()
    {
        // The binding adapts the contract, not the implementation. Referencing the
        // facade package would drag its PromQL machinery and HTTP backend client
        // into every client head - the coupling the hoist removed.
        var offenders = TransitiveProjectClosure()
            .Select(Path.GetFileName)
            .Where(name => string.Equals(name, $"{NeutralFacade}.csproj", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.That(offenders, Is.Empty);
    }

    [Test]
    public void Every_fault_type_the_binding_maps_lives_in_the_contract_package()
    {
        // The binding maps three typed refusals to three distinct statuses. Each
        // must be nameable from the contract alone - a fault parked in the facade
        // implementation would be one a client-safe binding could not catch by type,
        // forcing it to either take the facade reference (breaking the closure) or
        // let the fault fall through to an opaque Internal.
        Type[] mappedFaults =
        [
            typeof(TelemetryQueryNotFoundException),
            typeof(TelemetryQueryBoundsException),
            typeof(TelemetryBackendException),
        ];

        var strays = mappedFaults
            .Where(type => !string.Equals(type.Assembly.GetName().Name, ContractPackage, StringComparison.Ordinal))
            .Select(type => $"{type.Name} -> {type.Assembly.GetName().Name}")
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(strays, Is.Empty, "Strays: " + string.Join(", ", strays));
    }

    [Test]
    public void The_built_assembly_references_the_contract_package()
        => Assert.That(
            BindingAssembly.GetReferencedAssemblies().Select(name => name.Name),
            Has.One.EqualTo(ContractPackage));

    [Test]
    public void The_transitive_closure_walk_is_not_vacuous()
    {
        // The closure assertions above are only meaningful if the walk actually
        // resolves projects. A broken regex or a moved project file would make every
        // "is empty" assertion pass for the wrong reason.
        var closure = TransitiveProjectClosure().Select(Path.GetFileName).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(closure, Is.Not.Empty);
            Assert.That(closure, Has.One.EqualTo($"{ContractPackage}.csproj"));
        });
    }

    [Test]
    public void The_mcp_detection_predicate_actually_flags_an_mcp_project()
    {
        // The smoke-detector battery test: prove the substring the closure assertion
        // keys on really does match a real MCP project file name, so the guard cannot
        // be silently disarmed by a rename of the detection signal.
        Assert.Multiple(() =>
        {
            Assert.That(
                "Orleans.Lattice.Api.Mcp.Telemetry.csproj".Contains(".Mcp.", StringComparison.OrdinalIgnoreCase),
                Is.True);
            Assert.That(
                "Orleans.Lattice.Api.Mcp.csproj".StartsWith(McpPrefix, StringComparison.OrdinalIgnoreCase),
                Is.True);
            Assert.That(
                $"{ContractPackage}.csproj".Contains(".Mcp.", StringComparison.OrdinalIgnoreCase),
                Is.False);
        });
    }

    [Test]
    public void The_built_assembly_references_no_mcp_assembly()
    {
        var offenders = BindingAssembly.GetReferencedAssemblies()
            .Select(name => name.Name)
            .Where(name => name is not null && name.StartsWith(McpPrefix, StringComparison.Ordinal))
            .ToArray();

        Assert.That(offenders, Is.Empty);
    }

    [Test]
    public void The_built_assembly_references_no_neutral_facade_assembly()
    {
        var offenders = BindingAssembly.GetReferencedAssemblies()
            .Select(name => name.Name)
            .Where(name => string.Equals(name, NeutralFacade, StringComparison.Ordinal))
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "The binding adapts ILatticeTelemetry from the contract package; reaching the facade "
            + "implementation would re-couple every client head to the PromQL machinery.");
    }

    [Test]
    public void No_type_the_binding_declares_is_an_orleans_grain_interface()
    {
        var offenders = BindingAssembly.GetTypes()
            .Where(IsGrainInterface)
            .Select(type => type.FullName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "The binding is a transport over a facade contract, never a grain proxy. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void No_grain_interface_appears_on_the_client_surface()
    {
        var offenders = ClientSurfaceTypes()
            .Where(IsGrainInterface)
            .Select(type => type.FullName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "A client head consumes telemetry through DTOs, never through a grain proxy. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void The_client_surface_is_built_only_from_the_contract_and_this_binding()
    {
        // The load-bearing half: consuming the client must force no further Lattice
        // or Orleans dependency on a head - not the core (where the grain interfaces
        // live), not the neutral facade, and certainly not an MCP assembly.
        var offenders = ClientSurfaceTypes()
            .Select(type => type.Assembly.GetName().Name)
            .Where(name => name is not null
                && name.StartsWith("Orleans.", StringComparison.Ordinal)
                && !string.Equals(name, ContractPackage, StringComparison.Ordinal)
                && !string.Equals(name, BindingAssemblyName, StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "Every Lattice type on the client's public surface must come from the shared contract or "
            + "this binding. Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void The_client_surface_adds_no_third_party_dependency_beyond_grpc()
    {
        // Whatever is not Lattice on the surface must be the BCL or the gRPC call
        // abstraction the caller already supplies. Anything else is a new package a
        // consuming head is forced to install.
        var offenders = ClientSurfaceTypes()
            .Select(type => type.Assembly.GetName().Name)
            .Where(name => name is not null
                && !name.StartsWith("Orleans.", StringComparison.Ordinal)
                && !name.StartsWith("System.", StringComparison.Ordinal)
                && !string.Equals(name, "Grpc.Core.Api", StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(offenders, Is.Empty, "Offenders: " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The distinct types reachable from the public surface of
    /// <see cref="LatticeTelemetryApiGrpcClient"/>: every public method's return
    /// type (unwrapping <see cref="Task{TResult}"/>) and parameter types, plus every
    /// public property type. This is what a consuming head must be able to name.
    /// </summary>
    private static IEnumerable<Type> ClientSurfaceTypes()
    {
        var client = typeof(LatticeTelemetryApiGrpcClient);

        foreach (var method in client.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
        {
            if (method.DeclaringType != client)
            {
                continue;
            }

            yield return Unwrap(method.ReturnType);
            foreach (var parameter in method.GetParameters())
            {
                yield return Unwrap(parameter.ParameterType);
            }
        }

        foreach (var property in client.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
        {
            yield return Unwrap(property.PropertyType);
        }
    }

    private static Type Unwrap(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>)
            ? type.GetGenericArguments()[0]
            : type;

    /// <summary>
    /// Whether <paramref name="type"/> is an Orleans grain interface - anything
    /// assignable to <c>Orleans.IAddressable</c>, the root every grain interface
    /// derives from (via <c>IGrain</c> / <c>IGrainWithStringKey</c>).
    /// </summary>
    private static bool IsGrainInterface(Type type)
        => typeof(IAddressable).IsAssignableFrom(type) && type != typeof(IAddressable);
}
