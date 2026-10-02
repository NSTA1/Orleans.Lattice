using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// The gRPC coverage half of <see cref="AgentDocsConsistencyTests"/>: every RPC a
/// binding package declares in the methods catalogue its agent spec names must
/// have an operation in that spec, so a new RPC cannot ship undocumented.
/// </summary>
public sealed partial class AgentDocsConsistencyTests
{
    private static readonly Regex MethodNameConstant = new(
        @"const\s+string\s+\w+MethodName\s*=\s*""(?<name>[^""]+)""",
        RegexOptions.Compiled);

    // RPCs known to be on a gRPC service but missing from its agent spec, as
    // "<spec path> <rpc name>". The list only shrinks: an entry that is now
    // documented fails the ratchet below, so it must be removed here too. Prefer
    // documenting the RPC to adding an entry.
    private static readonly string[] KnownUndocumentedRpcs = Array.Empty<string>();

    [Test]
    public void Every_grpc_rpc_in_the_methods_catalogue_has_an_agent_spec_operation()
    {
        var coverage = ReadGrpcCoverage();
        var missing = coverage
            .SelectMany(c => c.Rpcs.Where(rpc => !c.Documented.Contains(rpc)).Select(rpc => $"{c.Spec} {rpc}"))
            .Where(entry => !KnownUndocumentedRpcs.Contains(entry, StringComparer.Ordinal))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(coverage.Sum(c => c.Rpcs.Count), Is.GreaterThan(0), "No RPCs were read from any methods catalogue, so the gate proved nothing.");
            Assert.That(coverage.Select(c => c.Spec), Does.Contain("api/schema.json"), "The schema agent spec was not examined.");
            Assert.That(
                missing,
                Is.Empty,
                "These RPCs are declared on a gRPC service but have no operation (matching grpc_method) in the agent spec:"
                + Environment.NewLine + string.Join(Environment.NewLine, missing));
        });
    }

    [Test]
    public void Known_undocumented_rpcs_are_still_undocumented()
    {
        var coverage = ReadGrpcCoverage().ToDictionary(c => c.Spec, StringComparer.Ordinal);
        var stale = new List<string>();
        foreach (var entry in KnownUndocumentedRpcs)
        {
            var parts = entry.Split(' ');
            if (!coverage.TryGetValue(parts[0], out var spec) || !spec.Rpcs.Contains(parts[1]) || spec.Documented.Contains(parts[1]))
            {
                stale.Add(entry);
            }
        }

        Assert.That(stale, Is.Empty, "Remove these entries from KnownUndocumentedRpcs; each is now documented or no longer an RPC:" + Environment.NewLine + string.Join(Environment.NewLine, stale));
    }

    private static List<GrpcCoverage> ReadGrpcCoverage()
    {
        var result = new List<GrpcCoverage>();
        foreach (var relative in ListAgentFiles().Where(p => p.StartsWith("api/", StringComparison.Ordinal) && p.EndsWith(".json", StringComparison.Ordinal)))
        {
            using var document = ParseJson(relative, ReadAgentText(relative));
            var root = document.RootElement;
            if (!root.TryGetProperty("grpc", out var grpc) ||
                grpc.ValueKind != JsonValueKind.Object ||
                !grpc.TryGetProperty("methods_source", out var source) ||
                source.ValueKind != JsonValueKind.String ||
                !grpc.TryGetProperty("service", out var service) ||
                service.ValueKind != JsonValueKind.String)
            {
                continue;
            }

            var sourcePath = Path.Combine(RepoRoot, source.GetString()!.Split('#')[0].Replace('/', Path.DirectorySeparatorChar));
            Assert.That(File.Exists(sourcePath), Is.True, $"{relative}: grpc.methods_source '{source.GetString()}' does not exist.");

            var rpcs = MethodNameConstant.Matches(File.ReadAllText(sourcePath))
                .Select(m => m.Groups["name"].Value)
                .ToHashSet(StringComparer.Ordinal);
            var services = service.GetString()!
                .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var documented = new HashSet<string>(StringComparer.Ordinal);
            if (root.TryGetProperty("operations", out var operations) && operations.ValueKind == JsonValueKind.Array)
            {
                foreach (var operation in operations.EnumerateArray())
                {
                    if (operation.TryGetProperty("grpc_method", out var method) && method.ValueKind == JsonValueKind.String)
                    {
                        var value = method.GetString()!;
                        foreach (var name in services)
                        {
                            var prefix = "/" + name + "/";
                            if (value.StartsWith(prefix, StringComparison.Ordinal))
                            {
                                documented.Add(value[prefix.Length..]);
                            }
                        }
                    }
                }
            }

            result.Add(new GrpcCoverage(relative, rpcs, documented));
        }

        return result;
    }

    private sealed record GrpcCoverage(string Spec, HashSet<string> Rpcs, HashSet<string> Documented);
}
