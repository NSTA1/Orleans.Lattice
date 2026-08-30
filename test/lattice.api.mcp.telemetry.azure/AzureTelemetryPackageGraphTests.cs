using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure.Tests;

/// <summary>
/// Guards the packaging half of the T2 hoist: this companion binds the
/// <see cref="ITelemetryBackendTokenProvider"/> seam that lives in the neutral
/// <c>Orleans.Lattice.Api.Telemetry</c> facade, so it must reference that facade
/// <b>directly</b> and must not reach it transitively through the MCP binding.
/// </summary>
/// <remarks>
/// The point of the hoist is that a client head wanting an Azure-authenticated
/// telemetry backend does not have to install the MCP server surface to get it. A
/// transitive reference compiles perfectly well, so nothing else in the build
/// notices the regression - hence this guard. The project-file assertion is the
/// load-bearing one (it pins the shipped package graph, which the C# compiler's
/// reference pruning would hide from an IL-only check); the assembly assertion
/// catches a re-coupling introduced in code rather than in the project file.
/// </remarks>
[TestFixture]
public sealed class AzureTelemetryPackageGraphTests
{
    private const string NeutralFacade = "Orleans.Lattice.Api.Telemetry";
    private const string McpAssemblyPrefix = "Orleans.Lattice.Api.Mcp";

    private static readonly Regex ProjectReferenceRegex = new(
        "<ProjectReference\\s+Include\\s*=\\s*\"(?<path>[^\"]+)\"",
        RegexOptions.CultureInvariant);

    private static readonly Regex PackageReferenceRegex = new(
        "<PackageReference\\s+Include\\s*=\\s*\"(?<id>[^\"]+)\"",
        RegexOptions.CultureInvariant);

    private static string ProjectFileText()
    {
        var csproj = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src",
            "lattice.api.mcp.telemetry.azure",
            "Orleans.Lattice.Api.Mcp.Telemetry.Azure.csproj");

        Assert.That(File.Exists(csproj), Is.True, $"Expected the package project file at {csproj}.");
        return File.ReadAllText(csproj);
    }

    private static IReadOnlyList<string> ProjectReferences()
        => ProjectReferenceRegex.Matches(ProjectFileText())
            .Select(match => match.Groups["path"].Value.Replace('\\', '/'))
            .ToArray();

    private static IReadOnlyList<string> PackageReferences()
        => PackageReferenceRegex.Matches(ProjectFileText())
            .Select(match => match.Groups["id"].Value)
            .ToArray();

    [Test]
    public void The_package_references_the_neutral_telemetry_facade_directly()
        => Assert.That(
            ProjectReferences(),
            Has.One.EqualTo($"../lattice.api.telemetry/{NeutralFacade}.csproj"),
            "The token-provider seam lives in the neutral facade, so reference it directly.");

    [Test]
    public void The_package_references_no_mcp_project()
    {
        var mcpReferences = ProjectReferences()
            .Where(path => path.Contains("mcp", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.That(
            mcpReferences,
            Is.Empty,
            "A client head must be able to take the Azure token provider without the MCP server surface.");
    }

    [Test]
    public void The_package_references_no_mcp_nuget_package()
    {
        // A ProjectReference is not the only way an MCP dependency could return:
        // a PackageReference to a published Orleans.Lattice.Api.Mcp* package would
        // restore the same surface into a consuming head.
        var mcpPackages = PackageReferences()
            .Where(id => id.StartsWith(McpAssemblyPrefix, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.That(mcpPackages, Is.Empty);
    }

    [Test]
    public void The_built_assembly_references_the_neutral_facade()
    {
        var referenced = typeof(AzureTelemetryBackendTokenOptions).Assembly
            .GetReferencedAssemblies()
            .Select(name => name.Name)
            .ToArray();

        Assert.That(referenced, Has.One.EqualTo(NeutralFacade));
    }

    [Test]
    public void The_built_assembly_references_no_mcp_assembly()
    {
        var mcpReferences = typeof(AzureTelemetryBackendTokenOptions).Assembly
            .GetReferencedAssemblies()
            .Select(name => name.Name)
            .Where(name => name is not null
                && name.StartsWith(McpAssemblyPrefix, StringComparison.Ordinal))
            .ToArray();

        Assert.That(mcpReferences, Is.Empty);
    }
}
