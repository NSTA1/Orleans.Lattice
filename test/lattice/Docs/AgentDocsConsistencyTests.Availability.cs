using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Availability checks over <c>docs/agents</c>. The specifications describe every
/// surface Lattice ships, but a host exposes only the packages it registers, so
/// every API surface names the registrations that expose it, every MCP tool names
/// the module (and any opt-in) that contributes it and the code that declares it,
/// and every procedure opens by checking that its surfaces are available. Each
/// named registration and tool is checked against the cited source, so a renamed
/// registration or a moved tool fails here.
/// </summary>
public sealed partial class AgentDocsConsistencyTests
{
    private const string SurfaceAvailablePrecondition = "surface-available";

    private const string SurfaceNotAvailableFailureMode = "surface-not-available";

    private const string McpServerRegistration = "AddLatticeMcp";

    [Test]
    public void Every_api_surface_states_the_registrations_that_expose_it()
    {
        var failures = new List<string>();
        var examined = 0;

        foreach (var relative in ApiFiles())
        {
            examined++;
            using var document = ParseJson(relative, ReadAgentText(relative));
            if (!document.RootElement.TryGetProperty("availability", out var availability))
            {
                failures.Add($"{relative}: no availability.");
                continue;
            }

            foreach (var field in new[] { "in_process", "grpc_server", "grpc_client" })
            {
                foreach (var registration in availability.GetProperty(field).EnumerateArray())
                {
                    CheckNamesOccur(relative, $"availability.{field}", new[] { registration.GetProperty("registration").GetString()! }, registration.GetProperty("source").GetString()!, failures);
                }
            }

            if (availability.GetProperty("in_process").GetArrayLength() == 0)
            {
                failures.Add($"{relative}: availability.in_process names no registration.");
            }

            foreach (var requirement in availability.GetProperty("requires").EnumerateArray())
            {
                CheckNamesOccur(relative, "availability.requires", new[] { requirement.GetProperty("name").GetString()! }, requirement.GetProperty("source").GetString()!, failures);
            }

            if (availability.GetProperty("grpc_server").GetArrayLength() == 0 &&
                document.RootElement.GetProperty("operations").EnumerateArray().Any(o => o.GetProperty("grpc_method").ValueKind == JsonValueKind.String))
            {
                failures.Add($"{relative}: operations carry gRPC methods but availability.grpc_server names no registration.");
            }
        }

        Assert.That(examined, Is.GreaterThan(0), "No API files were examined.");
        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    [Test]
    public void Every_mcp_tool_states_the_module_that_contributes_it()
    {
        using var mcp = ParseJson("api/mcp.json", ReadAgentText("api/mcp.json"));
        var modules = mcp.RootElement.GetProperty("in_process").GetProperty("registration").GetProperty("tool_modules")
            .EnumerateArray().Select(m => m.GetString()!).Append(McpServerRegistration).ToHashSet(StringComparer.Ordinal);
        var failures = new List<string>();
        var examined = 0;

        foreach (var operation in mcp.RootElement.GetProperty("operations").EnumerateArray())
        {
            examined++;
            var id = operation.GetProperty("id").GetString()!;
            if (!operation.TryGetProperty("availability", out var availability))
            {
                failures.Add($"api/mcp.json#{id}: no availability.");
                continue;
            }

            var module = availability.GetProperty("module").GetString()!;
            if (!modules.Contains(module))
            {
                failures.Add($"api/mcp.json#{id}: module '{module}' is neither {McpServerRegistration} nor listed in in_process.registration.tool_modules.");
            }

            var source = availability.GetProperty("source").GetString()!;
            var text = ReadSourceText(source);
            if (text is null || !text.Contains($"\"{DeclaredToolName(id)}\"", StringComparison.Ordinal))
            {
                failures.Add($"api/mcp.json#{id}: availability.source '{source}' does not declare the tool name literal \"{DeclaredToolName(id)}\".");
            }

            if (availability.GetProperty("opt_in") is { ValueKind: JsonValueKind.Object } optIn)
            {
                var names = new[] { optIn.GetProperty("parameter"), optIn.GetProperty("option") }
                    .Where(n => n.ValueKind == JsonValueKind.String)
                    .Select(n => n.GetString()!.Split('.')[^1])
                    .ToList();
                if (names.Count == 0)
                {
                    failures.Add($"api/mcp.json#{id}: opt_in names neither a parameter nor an option.");
                }

                CheckNamesOccur($"api/mcp.json#{id}", "availability.opt_in", names, optIn.GetProperty("source").GetString()!, failures);
            }
        }

        Assert.That(examined, Is.GreaterThan(0), "No MCP operations were examined.");
        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    [Test]
    public void Every_procedure_opens_by_checking_its_surfaces_are_available()
    {
        var procedures = ListAgentFiles().Where(p => p.StartsWith("procedures/", StringComparison.Ordinal)).ToList();
        Assert.That(procedures, Is.Not.Empty, "No procedures were examined.");

        Assert.Multiple(() =>
        {
            foreach (var relative in procedures)
            {
                var procedure = ReadArtifactNode(relative)!;
                var firstPrecondition = procedure["preconditions"]?.AsArray().FirstOrDefault()?["id"]?.GetValue<string>();
                Assert.That(firstPrecondition, Is.EqualTo(SurfaceAvailablePrecondition), $"{relative}: the first precondition must be '{SurfaceAvailablePrecondition}'.");
                var failureModes = procedure["failure_modes"]?.AsArray().Select(m => m?["id"]?.GetValue<string>()).ToList() ?? new List<string?>();
                Assert.That(failureModes, Does.Contain(SurfaceNotAvailableFailureMode), $"{relative}: failure_modes must include '{SurfaceNotAvailableFailureMode}'.");
            }
        });
    }

    // An app-contributed tool is advertised as {slug}_{tool}; app slugs are
    // kebab-case while built-in group prefixes are not, and the source declares
    // only the app-local tool name.
    private static string DeclaredToolName(string id)
    {
        var separator = id.IndexOf('_', StringComparison.Ordinal);
        return separator > 0 && id[..separator].Contains('-', StringComparison.Ordinal) ? id[(separator + 1)..] : id;
    }

    private static void CheckNamesOccur(string relative, string field, IEnumerable<string> names, string source, List<string> failures)
    {
        var text = ReadSourceText(source);
        if (text is null)
        {
            failures.Add($"{relative}: {field} source '{source}' does not resolve to a file.");
            return;
        }

        foreach (var name in names)
        {
            if (!Regex.IsMatch(text, $@"(?<![A-Za-z0-9_]){Regex.Escape(name)}(?![A-Za-z0-9_])"))
            {
                failures.Add($"{relative}: {field} names '{name}', which '{source}' does not contain.");
            }
        }
    }

    private static string? ReadSourceText(string reference)
    {
        var relativePath = reference.Split('#')[0];
        var path = Path.Combine(RepoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        if (!File.Exists(path))
        {
            return null;
        }

        if (!FileTextCache.TryGetValue(path, out var text))
        {
            text = File.ReadAllText(path);
            FileTextCache[path] = text;
        }

        return text;
    }
}
