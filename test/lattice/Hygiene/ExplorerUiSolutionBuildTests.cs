using System.Xml.Linq;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Keeps the UI host's project-reference closure in the solution so a Release
/// solution build and its subsequent no-build publish use the same configuration.
/// </summary>
[TestFixture]
public sealed class ExplorerUiSolutionBuildTests
{
    [Test]
    public void Every_ui_host_project_reference_is_in_the_solution()
    {
        var root = HygieneRepository.FindRepoRoot();
        var solutionProjects = XDocument.Load(Path.Combine(root, "Orleans.Lattice.slnx"))
            .Descendants("Project")
            .Select(project => Path.GetFullPath(Path.Combine(root,
                ((string)project.Attribute("Path")!).Replace('/', Path.DirectorySeparatorChar))))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var host = Path.Combine(root, "test", "lattice.explorer.uitests",
            "Orleans.Lattice.Explorer.UiTests.csproj");
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pending = new Queue<string>();
        pending.Enqueue(host);

        while (pending.TryDequeue(out var project))
        {
            if (!visited.Add(project))
            {
                continue;
            }

            foreach (var reference in XDocument.Load(project).Descendants("ProjectReference"))
            {
                var include = (string?)reference.Attribute("Include");
                Assert.That(include, Is.Not.Null.And.Not.Empty,
                    $"ProjectReference in {project} must name a project.");
                pending.Enqueue(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(project)!,
                    include!.Replace('\\', Path.DirectorySeparatorChar)
                        .Replace('/', Path.DirectorySeparatorChar))));
            }
        }

        Assert.That(visited, Has.Count.GreaterThan(10),
            "The UI host must still reference its control planes; an empty graph proves nothing.");
        var missing = visited.Except(solutionProjects, StringComparer.OrdinalIgnoreCase)
            .Select(project => Path.GetRelativePath(root, project).Replace('\\', '/'))
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.That(missing, Is.Empty,
            "Add these UI host dependencies to Orleans.Lattice.slnx. MSBuild can build projects "
            + "outside the solution in Debug even during a Release solution build. The UI host's "
            + "Release --no-build publish then fails with a missing dependency assembly, as "
            + "TaskBoard did in multiple Coverage legs.");
    }
}
