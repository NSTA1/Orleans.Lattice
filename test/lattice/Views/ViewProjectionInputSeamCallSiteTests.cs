using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Pins the single projection-input seam of the view maintainer (#5240): every
/// invocation of <c>ILatticeViewProjection.Project</c> or
/// <c>ILatticeAggregationProjection.Project</c> in the maintainer's partials must hand
/// the projection <c>ProjectionInput(registration, ...)</c>, so a versioned source's
/// value reaches the projection envelope-stripped at its stamped version on every
/// derivation path. Joint repair cannot be driven deterministically from a caller-level
/// fixture, and a newly added derivation path is exactly where a bypass would appear,
/// so the call sites are scanned from source rather than trusted.
/// </summary>
[TestFixture]
public sealed class ViewProjectionInputSeamCallSiteTests
{
    private static readonly Regex ProjectCall = new(
        @"Projection!?\.Project\((?<argument>[^)]*)",
        RegexOptions.Compiled);

    [Test]
    public void Every_projection_call_in_the_view_maintainer_goes_through_the_projection_input_seam()
    {
        var directory = Path.Combine(HygieneRepository.FindRepoRoot(), "src", "lattice", "Views");
        var files = Directory.GetFiles(directory, "ViewMaintainerGrain*.cs");
        Assert.That(files, Is.Not.Empty, "the view maintainer partials were not found");

        var sites = new List<string>();
        var bypasses = new List<string>();
        foreach (var file in files)
        {
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                if (lines[i].TrimStart().StartsWith("//", StringComparison.Ordinal))
                {
                    continue;
                }

                foreach (Match match in ProjectCall.Matches(lines[i]))
                {
                    var site = $"{Path.GetFileName(file)}:{i + 1}";
                    sites.Add(site);
                    if (!match.Groups["argument"].Value.TrimStart().StartsWith("ProjectionInput(registration,", StringComparison.Ordinal))
                    {
                        bypasses.Add($"{site}: {lines[i].Trim()}");
                    }
                }
            }
        }

        Assert.That(sites, Is.Not.Empty, "the scan matched no projection call; the pattern no longer fits the source");
        Assert.That(bypasses, Is.Empty,
            "a projection call bypasses ProjectionInput, so a versioned source would reach it in a different shape on this path");
    }
}
