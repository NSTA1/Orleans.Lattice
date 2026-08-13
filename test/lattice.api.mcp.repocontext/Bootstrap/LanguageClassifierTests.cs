namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="LanguageClassifier"/>: the best-effort, extension-based
/// language detection that stamps a stable language identifier onto a scanned
/// file, or the empty string when the extension is not recognised.
/// </summary>
[TestFixture]
public sealed class LanguageClassifierTests
{
    [TestCase("src/Program.cs", "csharp")]
    [TestCase("app/index.ts", "typescript")]
    [TestCase("main.py", "python")]
    [TestCase("cmd/root.go", "go")]
    [TestCase("README.md", "markdown")]
    [TestCase("build.ps1", "powershell")]
    public void Classify_maps_a_known_extension_to_its_language(string path, string expected)
        => Assert.That(LanguageClassifier.Classify(path), Is.EqualTo(expected));

    [Test]
    public void Classify_is_case_insensitive_over_the_extension()
        => Assert.That(LanguageClassifier.Classify("SRC/PROGRAM.CS"), Is.EqualTo("csharp"));

    [Test]
    public void Classify_returns_empty_for_an_unrecognised_extension()
        => Assert.That(LanguageClassifier.Classify("data.unknownext"), Is.Empty);

    [Test]
    public void Classify_returns_empty_for_a_file_with_no_extension()
        => Assert.That(LanguageClassifier.Classify("LICENSE"), Is.Empty);

    [Test]
    public void Classify_rejects_a_null_path()
        => Assert.Throws<ArgumentNullException>(() => LanguageClassifier.Classify(null!));
}
