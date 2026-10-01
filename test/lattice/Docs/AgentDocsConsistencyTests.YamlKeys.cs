using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Docs;

public sealed partial class AgentDocsConsistencyTests
{
    private static readonly Regex YamlKey = new(@"^(?<key>[A-Za-z_][A-Za-z0-9_]*):(?: |$)", RegexOptions.Compiled);

    [Test]
    public void Yaml_mappings_declare_each_key_once()
    {
        var files = ListAgentFiles().Where(f => f.EndsWith(".yaml", StringComparison.Ordinal)).ToList();
        Assert.That(files, Is.Not.Empty, "docs/agents holds no YAML files, so nothing was examined.");

        var failures = new List<string>();
        foreach (var relative in files)
        {
            failures.AddRange(DuplicateYamlKeys(relative, ReadAgentText(relative)));
        }

        Assert.That(failures, Is.Empty,
            "A YAML mapping repeats a key; a strict parser rejects it and a lenient one keeps only the last value, silently dropping a claim or its source.");
    }

    [Test]
    public void Yaml_duplicate_key_scan_finds_a_repeated_source()
    {
        const string text = "schema: x\nitems:\n  - step: \"a\"\n    source: \"one\"\n    source: \"two\"\n  - step: \"b\"\n    source: \"three\"\n";

        Assert.That(DuplicateYamlKeys("probe.yaml", text).ToList(), Has.Count.EqualTo(1));
    }

    private static IEnumerable<string> DuplicateYamlKeys(string relative, string text)
    {
        var scopes = new Stack<(int Indent, HashSet<string> Keys)>();
        var lines = text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        for (var number = 1; number <= lines.Length; number++)
        {
            var line = lines[number - 1];
            var content = line.TrimStart(' ');
            if (content.Length == 0 || content.StartsWith('#'))
            {
                continue;
            }

            var indent = line.Length - content.Length;
            while (scopes.Count > 0 && scopes.Peek().Indent > indent)
            {
                scopes.Pop();
            }

            if (content.StartsWith("- ", StringComparison.Ordinal))
            {
                indent += 2;
                content = content[2..];
                var item = YamlKey.Match(content);
                if (item.Success)
                {
                    scopes.Push((indent, new HashSet<string>(StringComparer.Ordinal) { item.Groups["key"].Value }));
                }

                continue;
            }

            var match = YamlKey.Match(content);
            if (!match.Success)
            {
                continue;
            }

            var key = match.Groups["key"].Value;
            if (scopes.Count == 0 || scopes.Peek().Indent < indent)
            {
                scopes.Push((indent, new HashSet<string>(StringComparer.Ordinal) { key }));
            }
            else if (!scopes.Peek().Keys.Add(key))
            {
                yield return $"{relative}:{number}: key '{key}' repeats within one mapping.";
            }
        }
    }
}
