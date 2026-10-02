using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// The deprecation half of <see cref="AgentDocsConsistencyTests"/>: every public
/// facade interface member marked <c>[Obsolete]</c> with diagnostic
/// <c>LATTICE0002</c> must carry a <c>deprecated</c> marker on each agent spec
/// operation that names it, a marker must not outlive its <c>[Obsolete]</c>, and an
/// MCP tool that delegates to a deprecated facade operation is deprecated with it.
/// </summary>
public sealed partial class AgentDocsConsistencyTests
{
    private const string DeprecationDiagnostic = "LATTICE0002";

    private static readonly Regex ObsoleteAttribute = new(
        @"\[Obsolete\((?<args>(?:[^()""]|""(?:[^""\\]|\\.)*"")*)\)\]",
        RegexOptions.Compiled);

    private static readonly Regex InterfaceDeclaration = new(
        @"\bpublic\s+(?:partial\s+)?interface\s+(?<name>I[A-Za-z0-9_]*)",
        RegexOptions.Compiled);

    private static readonly Regex AttributedMemberName = new(
        @"^\s*(?:\[[^\]]*\]\s*)*[^;{(=]*?\b(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^>]*>)?\s*\(",
        RegexOptions.Compiled);

    private static readonly Regex InProcessMember = new(
        @"^(?<type>[A-Za-z_][A-Za-z0-9_]*)(?:<[^>]*>)?\.(?<member>[A-Za-z_][A-Za-z0-9_]*)$",
        RegexOptions.Compiled);

    [Test]
    public void Every_lattice0002_facade_member_has_a_deprecated_spec_operation()
    {
        var obsolete = ReadObsoleteFacadeMembers();
        var failures = new List<string>();
        var matched = new HashSet<string>(StringComparer.Ordinal);
        var specs = ApiFiles().Where(f => f != "api/mcp.json").ToList();

        foreach (var relative in specs)
        {
            using var document = ParseJson(relative, ReadAgentText(relative));
            var operations = SpecOperations(document.RootElement);
            var ids = operations.Select(o => o.GetProperty("id").GetString()!).ToHashSet(StringComparer.Ordinal);
            foreach (var operation in operations)
            {
                var id = operation.GetProperty("id").GetString()!;
                var members = InProcessMembers(operation).Where(obsolete.Contains).ToList();
                var hasMarker = operation.TryGetProperty("deprecated", out var marker) && marker.ValueKind == JsonValueKind.Object;
                var diagnostic = hasMarker && marker.TryGetProperty("diagnostic", out var d) && d.ValueKind == JsonValueKind.String ? d.GetString() : null;

                if (members.Count > 0)
                {
                    matched.UnionWith(members);
                    if (!hasMarker || diagnostic != DeprecationDiagnostic)
                    {
                        failures.Add($"{relative}#{id}: {string.Join(", ", members)} is [Obsolete] {DeprecationDiagnostic}, but the operation has no deprecated marker with diagnostic {DeprecationDiagnostic}.");
                        continue;
                    }

                    CheckDeprecatedMarker(relative, id, marker, ids, failures);
                }
                else if (diagnostic == DeprecationDiagnostic && InProcessMembers(operation).Any())
                {
                    failures.Add($"{relative}#{id}: carries a {DeprecationDiagnostic} deprecated marker, but none of its in_process members is [Obsolete] {DeprecationDiagnostic}.");
                }
            }
        }

        var unmatched = obsolete.Except(matched, StringComparer.Ordinal).OrderBy(m => m, StringComparer.Ordinal).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(obsolete, Is.Not.Empty, $"No [Obsolete] {DeprecationDiagnostic} facade members were read from src/, so the gate proved nothing.");
            Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
            Assert.That(
                unmatched,
                Is.Empty,
                $"These [Obsolete] {DeprecationDiagnostic} facade members have no agent spec operation naming them in in_process:"
                + Environment.NewLine + string.Join(Environment.NewLine, unmatched));
        });
    }

    [Test]
    public void Every_mcp_tool_over_a_deprecated_facade_operation_is_deprecated()
    {
        var failures = new List<string>();
        var cache = new Dictionary<string, JsonDocument>(StringComparer.Ordinal);
        var checkedTools = 0;
        try
        {
            using var mcp = ParseJson("api/mcp.json", ReadAgentText("api/mcp.json"));
            var operations = SpecOperations(mcp.RootElement);
            var tools = operations.Select(o => o.GetProperty("id").GetString()!).ToHashSet(StringComparer.Ordinal);
            foreach (var operation in operations)
            {
                var id = operation.GetProperty("id").GetString()!;
                if (!operation.TryGetProperty("facade_operation", out var facade) || facade.ValueKind != JsonValueKind.String)
                {
                    continue;
                }

                var target = ResolveFacade(facade.GetString()!, cache);
                if (target is not { } op || !op.TryGetProperty("deprecated", out _))
                {
                    continue;
                }

                checkedTools++;
                if (!operation.TryGetProperty("deprecated", out var marker) || marker.ValueKind != JsonValueKind.Object)
                {
                    failures.Add($"api/mcp.json#{id}: delegates to the deprecated facade operation {facade.GetString()}, but has no deprecated marker.");
                    continue;
                }

                CheckDeprecatedMarker("api/mcp.json", id, marker, tools, failures);
            }
        }
        finally
        {
            foreach (var document in cache.Values)
            {
                document.Dispose();
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(checkedTools, Is.GreaterThan(0), "No MCP tool delegates to a deprecated facade operation, so the gate proved nothing.");
            Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
        });
    }

    private static void CheckDeprecatedMarker(string relative, string id, JsonElement marker, HashSet<string> sameFileIds, List<string> failures)
    {
        if (!marker.TryGetProperty("removal", out var removal) || removal.ValueKind != JsonValueKind.String || removal.GetString()!.Length == 0)
        {
            failures.Add($"{relative}#{id}: deprecated marker has no removal.");
        }

        if (!marker.TryGetProperty("replacement", out var replacement) || replacement.ValueKind != JsonValueKind.String)
        {
            failures.Add($"{relative}#{id}: deprecated marker has no replacement.");
            return;
        }

        var value = replacement.GetString()!;
        if (FacadeReference.IsMatch(value))
        {
            var cache = new Dictionary<string, JsonDocument>(StringComparer.Ordinal);
            try
            {
                if (ResolveFacade(value, cache) is null)
                {
                    failures.Add($"{relative}#{id}: deprecated replacement '{value}' names no operation.");
                }
            }
            finally
            {
                foreach (var document in cache.Values)
                {
                    document.Dispose();
                }
            }
        }
        else if (!sameFileIds.Contains(value))
        {
            failures.Add($"{relative}#{id}: deprecated replacement '{value}' is not an operation id in {relative}.");
        }
    }

    private static List<JsonElement> SpecOperations(JsonElement root) =>
        root.TryGetProperty("operations", out var operations) && operations.ValueKind == JsonValueKind.Array
            ? operations.EnumerateArray().ToList()
            : new List<JsonElement>();

    private static IEnumerable<string> InProcessMembers(JsonElement operation)
    {
        if (!operation.TryGetProperty("in_process", out var value))
        {
            yield break;
        }

        var candidates = new List<string>();
        switch (value.ValueKind)
        {
            case JsonValueKind.String:
                candidates.Add(value.GetString()!);
                break;
            case JsonValueKind.Array:
                candidates.AddRange(value.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!));
                break;
            case JsonValueKind.Object:
                if (value.TryGetProperty("interface", out var type) && value.TryGetProperty("method", out var method) &&
                    type.ValueKind == JsonValueKind.String && method.ValueKind == JsonValueKind.String)
                {
                    candidates.Add(type.GetString() + "." + method.GetString());
                }

                break;
        }

        foreach (var candidate in candidates)
        {
            var match = InProcessMember.Match(candidate.Trim());
            if (match.Success)
            {
                yield return match.Groups["type"].Value + "." + match.Groups["member"].Value;
            }
        }
    }

    private static HashSet<string> ReadObsoleteFacadeMembers()
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        var src = Path.Combine(RepoRoot, "src");
        foreach (var file in Directory.EnumerateFiles(src, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(src, file).Replace(Path.DirectorySeparatorChar, '/');
            if (relative.Contains("/bin/", StringComparison.Ordinal) || relative.Contains("/obj/", StringComparison.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (!text.Contains(DeprecationDiagnostic, StringComparison.Ordinal))
            {
                continue;
            }

            var interfaces = InterfaceDeclaration.Matches(text);
            foreach (Match attribute in ObsoleteAttribute.Matches(text))
            {
                if (!attribute.Groups["args"].Value.Contains("\"" + DeprecationDiagnostic + "\"", StringComparison.Ordinal))
                {
                    continue;
                }

                var owner = interfaces.LastOrDefault(i => i.Index < attribute.Index);
                if (owner is null)
                {
                    continue;
                }

                var member = AttributedMemberName.Match(text[(attribute.Index + attribute.Length)..]);
                if (member.Success)
                {
                    result.Add(owner.Groups["name"].Value + "." + member.Groups["name"].Value);
                }
            }
        }

        return result;
    }
}
