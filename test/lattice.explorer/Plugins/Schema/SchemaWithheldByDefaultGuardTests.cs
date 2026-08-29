using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The structural guard behind the Schema area's withheld-by-default posture.
/// <para>
/// The area has always been off unless a head asked for it, because its
/// versioning UI cannot yet express what differs between schema versions. That
/// used to be a <c>LatticeExplorerWebOptions.EnableSchemaArea</c> flag the shared
/// navigation layer special-cased by name; it is now simply "the head does not
/// register the plugin". The behavioural half of that is asserted by the
/// assembled-host smoke tests; this is the source-level half, because the failure
/// mode is a well-intentioned edit (or a line-level merge between the four
/// concurrent area conversions) adding <c>AddExplorerSchemaPlugin()</c> alongside
/// its siblings in the head composition. That would silently surface an area the
/// product deliberately withholds, and it would still compile and still pass every
/// other test.
/// </para>
/// </summary>
[TestFixture]
public sealed class SchemaWithheldByDefaultGuardTests
{
    private const string WebHeadComposition =
        "src/lattice.explorer/WebHosting/LatticeExplorerWebServiceCollectionExtensions.cs";

    private const string RegistrationMethod = "AddExplorerSchemaPlugin";

    [Test]
    public void The_web_head_composition_does_not_register_the_schema_plugin()
    {
        var offenders = ExecutableLines(ReadWebHeadComposition())
            .Where(line => line.Contains(RegistrationMethod, StringComparison.Ordinal))
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "the Schema area is withheld by default: a head opts in by calling "
            + RegistrationMethod
            + "() itself, and the shared web head must not call it for every consumer."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void The_web_head_still_wires_the_schema_services_it_withholds_the_plugin_for()
    {
        var source = ReadWebHeadComposition();

        Assert.That(
            source,
            Does.Contain("AddExplorerSchema()"),
            "not registered is not deleted: the schema control services stay wired so a "
            + "head can surface the area without new plumbing");
    }

    [Test]
    public void No_head_option_flag_survives_to_gate_the_schema_area_by_name()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var options = Path.Combine(
            repoRoot,
            "src/lattice.explorer/WebHosting/LatticeExplorerWebOptions.cs".Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(options), Is.True, "expected the web head options at " + options);
        Assert.That(
            File.ReadAllText(options),
            Does.Not.Contain("EnableSchemaArea"),
            "the per-area flag is retired: head opt-in is registering the plugin, not setting a bool");
    }

    private static string ReadWebHeadComposition()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WebHeadComposition.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, "expected the web head composition at " + path);
        return File.ReadAllText(path);
    }

    /// <summary>
    /// The source's non-comment lines. The composition documents the opt-in call
    /// in prose so a head can find it, so the guard must distinguish naming the
    /// method from calling it.
    /// </summary>
    private static IEnumerable<string> ExecutableLines(string source) => source
        .Split('\n')
        .Select(line => line.Trim())
        .Where(line => !line.StartsWith("//", StringComparison.Ordinal)
            && !line.StartsWith("///", StringComparison.Ordinal)
            && !line.StartsWith('*')
            && !line.StartsWith("/*", StringComparison.Ordinal));

    [Test]
    public void The_guard_separates_calling_the_registration_from_naming_it()
    {
        // Battery test for the smoke detector: a change that neuters the comment
        // filter must fail here rather than silently passing the guard above.
        Assert.Multiple(() =>
        {
            Assert.That(
                ExecutableLines("        // call services.AddExplorerSchemaPlugin() to opt in"),
                Is.Empty.Or.None.Contains(RegistrationMethod));
            Assert.That(
                ExecutableLines("        services.AddExplorerSchemaPlugin();").Single(),
                Does.Contain(RegistrationMethod));
        });
    }
}
