using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Guards the deliberate duplication introduced by issue #2613. The canonical
/// CPU-grant reader lives at
/// <c>src/lattice.api.mcp.repocontext/Runtime/ContainerCpuGrant.cs</c>, but the
/// standalone ONNX embedding companion keeps a byte-identical private copy at
/// <c>apps/embedding-onnx/Embedding/ContainerCpuGrant.cs</c> because that
/// container image deliberately has no project reference into <c>src/</c> and its
/// Docker build context is only its own directory, so it cannot compile the
/// shared type. This fixture fails CI if the executable body of the two ever
/// drifts, which is the only thing that keeps the duplication safe.
/// </summary>
/// <remarks>
/// The comparison is over executable code only: it discards everything above the
/// class declaration (namespace, usings, and the class-level doc comment, which
/// legitimately differ), then drops comment and blank lines and normalises the
/// class-declaration access modifier (<c>public</c> on the canonical,
/// <c>internal</c> on the mirror). What remains is the parsing logic, which must
/// match exactly.
/// </remarks>
[TestFixture]
public sealed class ContainerCpuGrantMirrorDriftTests
{
    private const string CanonicalRelative =
        "src/lattice.api.mcp.repocontext/Runtime/ContainerCpuGrant.cs";

    private const string MirrorRelative =
        "apps/embedding-onnx/Embedding/ContainerCpuGrant.cs";

    private static readonly Regex ClassDeclaration = new(
        @"^\s*(?:public|internal)\s+static\s+class\s+ContainerCpuGrant\b", RegexOptions.Compiled);

    [Test]
    public void The_embedding_mirror_matches_the_canonical_reader_body()
    {
        var root = HygieneRepository.FindRepoRoot();
        var canonicalPath = Path.Combine(root, CanonicalRelative.Replace('/', Path.DirectorySeparatorChar));
        var mirrorPath = Path.Combine(root, MirrorRelative.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(canonicalPath), Is.True,
            $"Canonical reader not found at {CanonicalRelative}. If it moved, update this guard and "
            + "the mirror's doc pointer, or the duplication is no longer guarded.");
        Assert.That(File.Exists(mirrorPath), Is.True,
            $"Embedding mirror not found at {MirrorRelative}. If the embedding app gained a project "
            + "reference into src/ it can consume the canonical reader directly and this mirror (and "
            + "guard) should be removed.");

        var canonical = NormalizeBody(File.ReadAllText(canonicalPath), canonicalPath);
        var mirror = NormalizeBody(File.ReadAllText(mirrorPath), mirrorPath);

        Assert.That(mirror, Is.EqualTo(canonical),
            "The embedding-onnx ContainerCpuGrant mirror has drifted from the canonical reader in "
            + "src/lattice.api.mcp.repocontext/Runtime/. They are duplicated on purpose (the image "
            + "has no src reference) and must stay byte-identical in their executable body. Re-sync "
            + "the two, changing both.");
    }

    /// <summary>
    /// Proves the normaliser actually captures the parsing logic (so an empty or
    /// truncated extraction cannot make the comparison pass vacuously) and that it
    /// detects a one-line divergence.
    /// </summary>
    [Test]
    public void Normaliser_captures_the_body_and_detects_drift()
    {
        const string canonical =
            "namespace X;\n"
            + "/// <summary>doc a</summary>\n"
            + "public static class ContainerCpuGrant\n"
            + "{\n"
            + "    /// <summary>member doc</summary>\n"
            + "    public static int? Read() => ParseCpuQuota(\"1\", \"1\");\n"
            + "}\n";

        const string mirrorSame =
            "namespace Y;\n"
            + "/// <summary>a different doc</summary>\n"
            + "internal static class ContainerCpuGrant\n"
            + "{\n"
            + "\n"
            + "    // an inline note the canonical lacks\n"
            + "    /// <summary>member doc, reworded</summary>\n"
            + "    public static int? Read() => ParseCpuQuota(\"1\", \"1\");\n"
            + "}\n";

        const string mirrorDrifted =
            "internal static class ContainerCpuGrant\n"
            + "{\n"
            + "    public static int? Read() => ParseCpuQuota(\"2\", \"1\");\n"
            + "}\n";

        var canon = NormalizeBody(canonical, "canonical");
        var same = NormalizeBody(mirrorSame, "mirror-same");
        var drifted = NormalizeBody(mirrorDrifted, "mirror-drift");

        Assert.Multiple(() =>
        {
            Assert.That(canon, Does.Contain("ParseCpuQuota"),
                "The normaliser dropped the body; the comparison would pass vacuously.");
            Assert.That(same, Is.EqualTo(canon),
                "Namespace, access modifier, and comment differences must be ignored.");
            Assert.That(drifted, Is.Not.EqualTo(canon),
                "A change to the executable body must be detected.");
        });
    }

    /// <summary>
    /// Reduces a source file to its executable class body: the class declaration
    /// (with its access modifier stripped) and every non-comment, non-blank line
    /// below it, each trimmed.
    /// </summary>
    private static string NormalizeBody(string text, string path)
    {
        var lines = text.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n');

        var start = -1;
        for (var i = 0; i < lines.Length; i++)
        {
            if (ClassDeclaration.IsMatch(lines[i])) { start = i; break; }
        }

        Assert.That(start, Is.GreaterThanOrEqualTo(0),
            $"Could not find the ContainerCpuGrant class declaration in {path}; the drift guard "
            + "cannot compare what it cannot locate.");

        var body = new List<string>();
        for (var i = start; i < lines.Length; i++)
        {
            var trimmed = lines[i].Trim();
            if (trimmed.Length == 0) continue;
            if (trimmed.StartsWith("///", StringComparison.Ordinal)) continue;
            if (trimmed.StartsWith("//", StringComparison.Ordinal)) continue;

            if (i == start)
            {
                trimmed = Regex.Replace(trimmed, @"^(?:public|internal)\s+", string.Empty);
            }

            body.Add(trimmed);
        }

        return string.Join("\n", body);
    }
}
