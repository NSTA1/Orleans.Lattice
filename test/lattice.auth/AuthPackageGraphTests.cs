using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Guards the security invariant that <c>Orleans.Lattice.Auth</c> never references
/// tenancy (#6195). The auth seams default to null implementations registered with
/// <c>TryAdd</c> so that tenancy can layer its own over them; tenancy depends on auth,
/// never the reverse. Both the shipped project graph (project and package references,
/// walked transitively) and the built assembly's references are asserted, because a
/// published <c>PackageReference</c> restores the same coupling a
/// <c>ProjectReference</c> would.
/// </summary>
[TestFixture]
public sealed class AuthPackageGraphTests
{
    private const string TenancyPrefix = "Orleans.Lattice.Tenancy";

    private static readonly Assembly AuthAssembly = typeof(ILatticeDecisionEngine).Assembly;

    private static readonly Regex ProjectReferenceRegex = new(
        "<ProjectReference\\s+Include\\s*=\\s*\"(?<path>[^\"]+)\"",
        RegexOptions.CultureInvariant);

    private static readonly Regex PackageReferenceRegex = new(
        "<PackageReference\\s+Include\\s*=\\s*\"(?<id>[^\"]+)\"",
        RegexOptions.CultureInvariant);

    private static string AuthProjectPath() => Path.GetFullPath(Path.Combine(
        HygieneRepository.FindRepoRoot(),
        "src",
        "lattice.auth",
        "Orleans.Lattice.Auth.csproj"));

    /// <summary>
    /// Every project file reachable from the auth project through
    /// <c>ProjectReference</c>, including the auth project itself. Windows separators are
    /// normalised before combining, and an unresolvable hop fails rather than being
    /// skipped, so the walk cannot go inert on one platform.
    /// </summary>
    private static IReadOnlyCollection<string> ProjectClosure()
    {
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pending = new Stack<string>();
        pending.Push(AuthProjectPath());

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            if (!visited.Add(current))
            {
                continue;
            }

            Assert.That(File.Exists(current), Is.True, $"The project-reference walk resolved {current}, which does not exist.");

            var directory = Path.GetDirectoryName(current)!;
            foreach (Match match in ProjectReferenceRegex.Matches(File.ReadAllText(current)))
            {
                var reference = match.Groups["path"].Value.Replace('\\', '/');
                pending.Push(Path.GetFullPath(Path.Combine(directory, reference)));
            }
        }

        return visited;
    }

    private static bool IsTenancy(string? name)
        => name is not null && name.StartsWith(TenancyPrefix, StringComparison.OrdinalIgnoreCase);

    [Test]
    public void Auth_reaches_no_tenancy_project_transitively()
    {
        var offenders = ProjectClosure()
            .Select(Path.GetFileName)
            .Where(IsTenancy)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "lattice.auth must never reference tenancy; tenancy layers over the auth seams. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void Auth_reaches_no_tenancy_nuget_package_transitively()
    {
        var offenders = ProjectClosure()
            .SelectMany(project => PackageReferenceRegex.Matches(File.ReadAllText(project)))
            .Select(match => match.Groups["id"].Value)
            .Where(IsTenancy)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        Assert.That(offenders, Is.Empty, "Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void The_built_auth_assembly_references_no_tenancy_assembly()
    {
        var offenders = AuthAssembly.GetReferencedAssemblies()
            .Select(name => name.Name)
            .Where(IsTenancy)
            .ToArray();

        Assert.That(offenders, Is.Empty, "Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void The_auth_graph_walks_are_not_vacuous()
    {
        // The emptiness assertions above are only meaningful if the walk resolves real
        // projects and the anchor is the real auth assembly.
        var closure = ProjectClosure().Select(Path.GetFileName).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(closure, Has.One.EqualTo("Orleans.Lattice.csproj"));
            Assert.That(closure, Has.One.EqualTo("Orleans.Lattice.Membership.csproj"));
            Assert.That(AuthAssembly.GetName().Name, Is.EqualTo("Orleans.Lattice.Auth"));
            Assert.That(
                AuthAssembly.GetReferencedAssemblies().Select(name => name.Name),
                Has.Some.EqualTo("Orleans.Lattice"));
        });
    }

    [Test]
    public void The_tenancy_predicate_flags_the_tenancy_package()
    {
        Assert.Multiple(() =>
        {
            Assert.That(IsTenancy("Orleans.Lattice.Tenancy.csproj"), Is.True);
            Assert.That(IsTenancy("Orleans.Lattice.Tenancy"), Is.True);
            Assert.That(IsTenancy("Orleans.Lattice.Membership.csproj"), Is.False);
        });
    }
}
