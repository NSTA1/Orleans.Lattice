using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Keeps <c>docs/agents</c>, the machine-readable specification set for agents,
/// honest against the repository: the manifest lists exactly the files present,
/// every file is deterministic ASCII in its declared format and schema, every
/// uncertainty marker is complete, and every source reference names a path that
/// exists and, where it names a symbol, a file that contains it. A renamed or
/// deleted type therefore fails here instead of leaving a specification that
/// points at nothing.
/// </summary>
[TestFixture]
[Category("Docs")]
public sealed class AgentDocsConsistencyTests
{
    private const string ManifestSchema = "lattice.agents/index/v1";

    private static readonly string[] RequiredArtifacts =
    {
        "capabilities.yaml",
        "concepts.yaml",
        "deployment.yaml",
        "invariants.yaml",
    };

    private static readonly string[] RequiredDirectories = { "api/", "governance/", "procedures/" };

    private static readonly Regex QuotedString = new("\"((?:[^\"\\\\]|\\\\.)*)\"", RegexOptions.Compiled);

    private static readonly Regex RootedPathStart = new(
        @"^(src|docs|test|samples|spec|tools|benchmark|reference-architecture|\.github)/",
        RegexOptions.Compiled);

    private static readonly Regex RootFileStart = new(
        @"^(FEATURES\.md|PACKAGES\.md|README\.md|reference-architecture\.md)(#|$)",
        RegexOptions.Compiled);

    private static readonly Regex StrictReference = new(
        @"^(?<path>(?:(?:src|docs|test|samples|spec|tools|benchmark|reference-architecture|\.github)/[A-Za-z0-9_./\-]+)|FEATURES\.md|PACKAGES\.md|README\.md|reference-architecture\.md)(?:#(?<symbol>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*))?$",
        RegexOptions.Compiled);

    private static readonly Regex UnquotedPath = new(
        @"(?<![\w./\-])(src|docs|test|samples|spec|tools|benchmark|reference-architecture|\.github)/[A-Za-z0-9_.\-]",
        RegexOptions.Compiled);

    private static readonly Dictionary<string, string> FileTextCache = new(StringComparer.Ordinal);

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    private static string AgentsRoot => Path.Combine(RepoRoot, "docs", "agents");

    [Test]
    public void Manifest_lists_exactly_the_files_in_docs_agents()
    {
        var manifest = ReadManifest();
        var declared = manifest.Select(a => a.Path).ToList();
        var onDisk = ListAgentFiles().Where(p => p != "index.json").ToList();

        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.Unique, "index.json lists a path twice.");
            Assert.That(declared, Is.Ordered.Using((IComparer<string>)StringComparer.Ordinal), "index.json artifacts must be sorted by path (ordinal).");
            Assert.That(onDisk.Except(declared), Is.Empty, "Files in docs/agents missing from index.json.");
            Assert.That(declared.Except(onDisk), Is.Empty, "index.json lists files that do not exist.");
            foreach (var required in RequiredArtifacts)
            {
                Assert.That(declared, Does.Contain(required), $"docs/agents must carry {required}.");
            }

            foreach (var directory in RequiredDirectories)
            {
                Assert.That(declared.Any(p => p.StartsWith(directory, StringComparison.Ordinal)), Is.True, $"docs/agents must carry at least one {directory} artifact.");
            }

            foreach (var artifact in manifest)
            {
                Assert.That(artifact.Summary, Is.Not.Empty, $"{artifact.Path} has no summary; it is its llms.txt line.");
                Assert.That(artifact.Schema, Does.StartWith("lattice.agents/").And.EndWith("/v1"), $"{artifact.Path} declares schema '{artifact.Schema}'.");
                Assert.That(Path.GetExtension(artifact.Path).TrimStart('.'), Is.EqualTo(artifact.Format), $"{artifact.Path} declares format '{artifact.Format}'.");
            }
        });
    }

    [Test]
    public void Agent_docs_are_deterministic_ascii_json_or_yaml()
    {
        var files = ListAgentFiles();
        Assert.That(files, Is.Not.Empty, "docs/agents holds no files, so nothing was examined.");

        Assert.Multiple(() =>
        {
            foreach (var relative in files)
            {
                var extension = Path.GetExtension(relative);
                Assert.That(extension, Is.EqualTo(".json").Or.EqualTo(".yaml"), $"{relative}: docs/agents holds only .json and .yaml; anything else could render as a page humans see.");

                var bytes = File.ReadAllBytes(Path.Combine(AgentsRoot, relative));
                var offending = Array.FindIndex(bytes, b => b > 0x7E || (b < 0x20 && b != (byte)'\n' && b != (byte)'\r'));
                Assert.That(offending, Is.EqualTo(-1), $"{relative}: non-ASCII or tab byte at offset {offending}; the set is ASCII.");
                Assert.That(bytes.Length > 0 && bytes[^1] == (byte)'\n', Is.True, $"{relative}: must end with a newline.");
            }
        });
    }

    [Test]
    public void Every_artifact_declares_its_manifest_schema()
    {
        var manifest = ReadManifest().ToDictionary(a => a.Path, a => a.Schema, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            foreach (var (relative, schema) in manifest)
            {
                var text = ReadAgentText(relative);
                if (relative.EndsWith(".json", StringComparison.Ordinal))
                {
                    using var document = ParseJson(relative, text);
                    var root = document.RootElement;
                    Assert.That(root.ValueKind, Is.EqualTo(JsonValueKind.Object), $"{relative}: top level must be an object.");
                    Assert.That(root.TryGetProperty("schema", out var declared) ? declared.GetString() : null, Is.EqualTo(schema), $"{relative}: \"schema\" must match index.json.");
                }
                else
                {
                    var first = text.Split('\n').FirstOrDefault(l => l.Length > 0);
                    Assert.That(first, Is.EqualTo($"schema: {schema}"), $"{relative}: first line must be 'schema: {schema}'.");
                }
            }
        });
    }

    [Test]
    public void Every_source_reference_resolves_to_the_repository()
    {
        var examined = 0;
        var failures = new List<string>();

        foreach (var relative in ListAgentFiles())
        {
            var text = ReadAgentText(relative);
            var references = relative.EndsWith(".json", StringComparison.Ordinal)
                ? JsonStrings(relative, text)
                : YamlStrings(relative, text, failures);

            foreach (var value in references)
            {
                if (!RootedPathStart.IsMatch(value) && !RootFileStart.IsMatch(value))
                {
                    continue;
                }

                examined++;
                var failure = CheckReference(value);
                if (failure is not null)
                {
                    failures.Add($"{relative}: '{value}' {failure}");
                }
            }
        }

        Assert.That(examined, Is.GreaterThan(0), "No source references were examined, so the gate proved nothing.");
        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    [Test]
    public void Every_uncertainty_marker_names_its_reason_and_required_source()
    {
        var failures = new List<string>();

        foreach (var relative in ListAgentFiles())
        {
            var text = ReadAgentText(relative);
            if (relative.EndsWith(".json", StringComparison.Ordinal))
            {
                using var document = ParseJson(relative, text);
                CheckJsonUncertainty(relative, document.RootElement, "$", failures);
                continue;
            }

            var lines = text.Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                var trimmed = lines[i].TrimStart(' ', '-');
                if (!Regex.IsMatch(trimmed, @"^status:\s*""?uncertain""?\s*$"))
                {
                    continue;
                }

                var indent = lines[i].IndexOf("status:", StringComparison.Ordinal);
                var siblings = new List<string>();
                for (var j = i + 1; j < lines.Length; j++)
                {
                    var line = lines[j];
                    var lineIndent = line.Length - line.TrimStart(' ').Length;
                    if (line.Trim().Length == 0 || lineIndent != indent)
                    {
                        break;
                    }

                    siblings.Add(line.Trim());
                }

                if (!siblings.Any(s => s.StartsWith("reason:", StringComparison.Ordinal)) ||
                    !siblings.Any(s => s.StartsWith("required_source:", StringComparison.Ordinal)))
                {
                    failures.Add($"{relative}:{i + 1}: uncertainty marker lacks reason or required_source.");
                }
            }
        }

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    private static void CheckJsonUncertainty(string relative, JsonElement element, string path, List<string> failures)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                if (element.TryGetProperty("status", out var status) &&
                    status.ValueKind == JsonValueKind.String &&
                    status.GetString() == "uncertain" &&
                    (!element.TryGetProperty("reason", out _) || !element.TryGetProperty("required_source", out _)))
                {
                    failures.Add($"{relative} {path}: uncertainty marker lacks reason or required_source.");
                }

                foreach (var property in element.EnumerateObject())
                {
                    CheckJsonUncertainty(relative, property.Value, $"{path}.{property.Name}", failures);
                }

                break;
            case JsonValueKind.Array:
                var index = 0;
                foreach (var item in element.EnumerateArray())
                {
                    CheckJsonUncertainty(relative, item, $"{path}[{index++}]", failures);
                }

                break;
        }
    }

    private static string? CheckReference(string value)
    {
        var match = StrictReference.Match(value);
        if (!match.Success)
        {
            return "is not of the form 'path' or 'path#Symbol'.";
        }

        var relativePath = match.Groups["path"].Value;
        var path = Path.Combine(RepoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        var symbol = match.Groups["symbol"];
        if (!ExistsWithExactCase(relativePath))
        {
            return File.Exists(path) || Directory.Exists(path)
                ? "differs in letter case from the path on disk; CI runs on a case-sensitive file system."
                : "names a path that does not exist.";
        }

        if (File.Exists(path))
        {
            if (!symbol.Success)
            {
                return null;
            }

            if (!FileTextCache.TryGetValue(path, out var text))
            {
                text = File.ReadAllText(path);
                FileTextCache[path] = text;
            }

            var missing = symbol.Value.Split('.').FirstOrDefault(segment => !Regex.IsMatch(text, $@"(?<![A-Za-z0-9_]){Regex.Escape(segment)}(?![A-Za-z0-9_])"));
            return missing is null ? null : $"names symbol segment '{missing}', which the file does not contain.";
        }

        if (Directory.Exists(path))
        {
            return symbol.Success ? "names a symbol in a directory." : null;
        }

        return "names a path that does not exist.";
    }

    // Windows and macOS resolve paths case-insensitively and Linux does not, so
    // each segment is matched ordinally against the directory's real entries.
    private static bool ExistsWithExactCase(string relativePath)
    {
        var current = RepoRoot;
        foreach (var segment in relativePath.Split('/', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!Directory.Exists(current) ||
                !Directory.EnumerateFileSystemEntries(current).Any(e => string.Equals(Path.GetFileName(e), segment, StringComparison.Ordinal)))
            {
                return false;
            }

            current = Path.Combine(current, segment);
        }

        return true;
    }

    private static IEnumerable<string> JsonStrings(string relative, string text)
    {
        using var document = ParseJson(relative, text);
        var values = new List<string>();
        Collect(document.RootElement, null);
        return values;

        void Collect(JsonElement element, string? propertyName)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var property in element.EnumerateObject())
                    {
                        Collect(property.Value, property.Name);
                    }

                    break;
                case JsonValueKind.Array:
                    foreach (var item in element.EnumerateArray())
                    {
                        Collect(item, propertyName);
                    }

                    break;
                case JsonValueKind.String when propertyName != "required_source":
                    values.Add(element.GetString()!);
                    break;
            }
        }
    }

    private static IEnumerable<string> YamlStrings(string relative, string text, List<string> failures)
    {
        var values = new List<string>();
        var lines = text.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.TrimStart(' ', '-').StartsWith("required_source:", StringComparison.Ordinal))
            {
                continue;
            }

            foreach (Match quoted in QuotedString.Matches(line))
            {
                values.Add(Regex.Unescape(quoted.Groups[1].Value));
            }

            var unquoted = QuotedString.Replace(line, string.Empty);
            var colon = unquoted.IndexOf(':');
            var valuePart = colon >= 0 ? unquoted[(colon + 1)..] : unquoted.TrimStart(' ', '-');
            if (UnquotedPath.IsMatch(valuePart))
            {
                failures.Add($"{relative}:{i + 1}: path value is not double-quoted.");
            }
        }

        return values;
    }

    private static JsonDocument ParseJson(string relative, string text)
    {
        try
        {
            return JsonDocument.Parse(text);
        }
        catch (JsonException ex)
        {
            Assert.Fail($"{relative}: invalid JSON: {ex.Message}");
            throw;
        }
    }

    // A Windows checkout with core.autocrlf materialises CRLF; the repository
    // stores LF (.gitattributes text=auto), so line endings are compared as LF.
    private static string ReadAgentText(string relative) =>
        File.ReadAllText(Path.Combine(AgentsRoot, relative), Encoding.UTF8).Replace("\r\n", "\n", StringComparison.Ordinal);

    private static List<string> ListAgentFiles()
    {
        Assert.That(Directory.Exists(AgentsRoot), Is.True, "docs/agents does not exist.");
        return Directory.EnumerateFiles(AgentsRoot, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(AgentsRoot, f).Replace(Path.DirectorySeparatorChar, '/'))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();
    }

    private static List<ManifestArtifact> ReadManifest()
    {
        var manifestPath = Path.Combine(AgentsRoot, "index.json");
        Assert.That(File.Exists(manifestPath), Is.True, "docs/agents/index.json does not exist.");
        using var document = ParseJson("index.json", ReadAgentText("index.json"));
        var root = document.RootElement;
        Assert.That(root.GetProperty("schema").GetString(), Is.EqualTo(ManifestSchema));

        var artifacts = root.GetProperty("artifacts").EnumerateArray().Select(a => new ManifestArtifact(
            a.GetProperty("path").GetString() ?? string.Empty,
            a.GetProperty("kind").GetString() ?? string.Empty,
            a.GetProperty("format").GetString() ?? string.Empty,
            a.GetProperty("schema").GetString() ?? string.Empty,
            a.GetProperty("summary").GetString() ?? string.Empty)).ToList();
        Assert.That(artifacts, Is.Not.Empty, "index.json lists no artifacts.");
        return artifacts;
    }

    private sealed record ManifestArtifact(string Path, string Kind, string Format, string Schema, string Summary);
}
