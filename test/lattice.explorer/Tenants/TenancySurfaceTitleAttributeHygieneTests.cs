using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The tenancy surfaces explain themselves in a way a keyboard or touch caller
/// can actually reach.
/// </summary>
/// <remarks>
/// <para>
/// A <c>title</c> attribute is the classic way to lose that: it appears on hover
/// only, is unreachable by keyboard, is invisible on touch, cannot be styled,
/// and is announced inconsistently. These two areas carry the densest jargon in
/// the product - quota, residency, region, grant, admin subject, lifecycle, the
/// reserved default tenant - and every one of those explanations used to hide in
/// one.
/// </para>
/// <para>
/// The replacements are a real help disclosure (a focusable button with
/// <c>aria-expanded</c> and a <c>role="note"</c> panel), a visually-hidden
/// expansion inside the control's own accessible name, or an
/// <c>aria-describedby</c> pointing at a rendered element. A scan is used rather
/// than a rendered assertion so a title added to a surface no fixture happens to
/// render is still caught.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TenancySurfaceTitleAttributeHygieneTests
{
    private static readonly string[] Areas =
    [
        Path.Combine("src", "lattice.explorer", "Plugins", "Tenants"),
        Path.Combine("src", "lattice.explorer", "Plugins", "MyTenant"),
    ];

    [Test]
    public void No_tenancy_surface_explains_itself_in_a_title_attribute()
    {
        var offenders = new List<string>();

        foreach (var file in TenancyRazorFiles())
        {
            var text = File.ReadAllText(file);
            var index = text.IndexOf("title=", StringComparison.Ordinal);
            if (index >= 0)
            {
                offenders.Add($"{file} (at offset {index})");
            }
        }

        Assert.That(
            offenders,
            Is.Empty,
            "a title attribute is unreachable by keyboard and by touch; use LatticeHelp, a "
            + "visually-hidden expansion, or aria-describedby instead");
    }

    [Test]
    public void The_scan_actually_reads_the_tenancy_surfaces()
    {
        // Without this the gate above would pass vacuously if either directory
        // moved or was renamed.
        Assert.That(TenancyRazorFiles(), Is.Not.Empty);
    }

    private static IReadOnlyList<string> TenancyRazorFiles()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var files = new List<string>();

        foreach (var area in Areas)
        {
            var directory = Path.Combine(repoRoot, area);
            if (Directory.Exists(directory))
            {
                files.AddRange(Directory.EnumerateFiles(directory, "*.razor", SearchOption.AllDirectories));
            }
        }

        return files;
    }
}
