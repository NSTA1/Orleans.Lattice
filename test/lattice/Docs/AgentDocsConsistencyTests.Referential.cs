using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Referential-integrity checks over <c>docs/agents</c>: every identifier one entry
/// uses to point at another (an error id, a decision-tree branch, a facade
/// operation) must resolve, request parameters must be well-formed identifiers, the
/// MCP tool annotations must agree with the retry and idempotency contract, every
/// package reference must be a published NuGet id, and no XML-documentation debris
/// may survive into a specification.
/// </summary>
public sealed partial class AgentDocsConsistencyTests
{
    private static readonly Regex Identifier = new(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    private static readonly Regex FacadeReference = new(@"^api/(?<file>[a-z]+)\.json#(?<id>[a-z0-9\-]+)$", RegexOptions.Compiled);

    private static readonly Regex GeneratorDebris = new(@"///|</?(see|paramref|typeparamref|para|c|code)\b|""\)\]", RegexOptions.Compiled);

    private static readonly Regex NuGetPackageId = new(@"^Orleans\.Lattice(\.[A-Za-z0-9]+)*$", RegexOptions.Compiled);

    private static readonly Regex YamlPackagesHeader = new(@"^(\s*)(?:- )?packages:\s*$", RegexOptions.Compiled);

    private static readonly Regex YamlListItem = new(@"^(\s*)- ""([^""]*)""\s*$", RegexOptions.Compiled);

    private static readonly Regex YamlPackageScalar = new(@"^\s*(?:- )?package:\s*""([^""]*)""", RegexOptions.Compiled);

    [Test]
    public void Every_api_error_reference_resolves_to_its_error_taxonomy()
    {
        var examined = 0;
        Assert.Multiple(() =>
        {
            foreach (var relative in ApiFiles())
            {
                using var document = ParseJson(relative, ReadAgentText(relative));
                var root = document.RootElement;
                var taxonomy = root.TryGetProperty("error_taxonomy", out var tax)
                    ? tax.EnumerateArray().Select(e => e.GetProperty("id").GetString()).ToHashSet(StringComparer.Ordinal)
                    : new HashSet<string?>(StringComparer.Ordinal);
                foreach (var operation in root.GetProperty("operations").EnumerateArray())
                {
                    var id = operation.GetProperty("id").GetString();
                    if (!operation.TryGetProperty("errors", out var errors) || errors.ValueKind != JsonValueKind.Array)
                    {
                        continue;
                    }

                    foreach (var error in errors.EnumerateArray())
                    {
                        examined++;
                        var errorId = error.TryGetProperty("error_id", out var e) ? e.GetString() : null;
                        Assert.That(taxonomy, Does.Contain(errorId), $"{relative}#{id}: error_id '{errorId}' is not in error_taxonomy.");
                    }
                }
            }
        });
        Assert.That(examined, Is.GreaterThan(0), "No api error references were examined.");
    }

    [Test]
    public void Every_api_request_parameter_is_a_well_formed_identifier()
    {
        var examined = 0;
        Assert.Multiple(() =>
        {
            foreach (var relative in ApiFiles())
            {
                using var document = ParseJson(relative, ReadAgentText(relative));
                foreach (var operation in document.RootElement.GetProperty("operations").EnumerateArray())
                {
                    if (!operation.TryGetProperty("request", out var request) || request.ValueKind != JsonValueKind.Array)
                    {
                        continue;
                    }

                    var id = operation.GetProperty("id").GetString();
                    foreach (var parameter in request.EnumerateArray())
                    {
                        examined++;
                        var name = parameter.GetProperty("name").GetString() ?? string.Empty;
                        var type = parameter.GetProperty("type").GetString() ?? string.Empty;
                        Assert.That(Identifier.IsMatch(name), Is.True, $"{relative}#{id}: parameter name '{name}' is not an identifier.");
                        Assert.That(type.Contains("\")]", StringComparison.Ordinal) || type.EndsWith('=') || type.Contains(';'), Is.False,
                            $"{relative}#{id}.{name}: parameter type '{type}' carries attribute or default-value debris.");
                    }
                }
            }
        });
        Assert.That(examined, Is.GreaterThan(0), "No api request parameters were examined.");
    }

    [Test]
    public void Every_decision_tree_branch_resolves_to_a_node_or_a_declared_terminal()
    {
        var examined = 0;
        Assert.Multiple(() =>
        {
            foreach (var relative in ListAgentFiles().Where(p => p.StartsWith("governance/", StringComparison.Ordinal) && p.EndsWith(".json", StringComparison.Ordinal)))
            {
                using var document = ParseJson(relative, ReadAgentText(relative));
                if (!document.RootElement.TryGetProperty("decision_trees", out var trees))
                {
                    continue;
                }

                foreach (var tree in trees.EnumerateArray())
                {
                    examined++;
                    var treeId = tree.GetProperty("id").GetString();
                    var nodes = tree.GetProperty("nodes").EnumerateArray().Select(n => n.GetProperty("id").GetString()!).ToList();
                    Assert.That(tree.TryGetProperty("terminals", out var terminalsElement), Is.True, $"{relative}#{treeId}: declares no terminals.");
                    var terminals = tree.TryGetProperty("terminals", out terminalsElement)
                        ? terminalsElement.EnumerateArray().Select(t => t.GetProperty("id").GetString()!).ToList()
                        : new List<string>();
                    Assert.That(nodes.Concat(terminals), Is.Unique, $"{relative}#{treeId}: node and terminal ids must be unique.");
                    Assert.That(terminals, Is.Ordered.Using((IComparer<string>)StringComparer.Ordinal), $"{relative}#{treeId}: terminals must be sorted by id.");

                    var targets = new HashSet<string>(StringComparer.Ordinal);
                    foreach (var node in tree.GetProperty("nodes").EnumerateArray())
                    {
                        foreach (var branch in new[] { "on_true", "on_false" })
                        {
                            var target = node.GetProperty(branch).GetString()!;
                            targets.Add(target);
                            Assert.That(nodes.Contains(target) || terminals.Contains(target), Is.True,
                                $"{relative}#{treeId}.{node.GetProperty("id").GetString()}: {branch} '{target}' is neither a node nor a declared terminal.");
                        }
                    }

                    Assert.That(terminals.Except(targets), Is.Empty, $"{relative}#{treeId}: terminals no branch reaches.");
                }
            }
        });
        Assert.That(examined, Is.GreaterThan(0), "No decision trees were examined.");
    }

    [Test]
    public void Every_mcp_tool_annotation_agrees_with_its_contract_and_facade()
    {
        using var mcp = ParseJson("api/mcp.json", ReadAgentText("api/mcp.json"));
        var facades = new Dictionary<string, JsonDocument>(StringComparer.Ordinal);
        var examined = 0;
        try
        {
            Assert.Multiple(() =>
            {
                foreach (var operation in mcp.RootElement.GetProperty("operations").EnumerateArray())
                {
                    examined++;
                    var id = operation.GetProperty("id").GetString();
                    Assert.That(operation.TryGetProperty("tool_annotations", out var annotations), Is.True, $"api/mcp.json#{id}: no tool_annotations.");
                    if (!operation.TryGetProperty("tool_annotations", out annotations))
                    {
                        continue;
                    }

                    var readOnly = annotations.GetProperty("read_only").GetBoolean();
                    _ = annotations.GetProperty("destructive").GetBoolean();
                    Assert.That(annotations.GetProperty("source").GetString(), Does.StartWith("src/"), $"api/mcp.json#{id}: tool_annotations.source.");

                    if (readOnly)
                    {
                        var idempotency = operation.GetProperty("idempotency");
                        Assert.That(idempotency.TryGetProperty("idempotent", out var idem) && idem.ValueKind == JsonValueKind.True, Is.True,
                            $"api/mcp.json#{id}: a read-only tool must be idempotent.");
                        var retry = operation.GetProperty("retry");
                        Assert.That(retry.TryGetProperty("safe", out var safe) && safe.ValueKind == JsonValueKind.True, Is.True,
                            $"api/mcp.json#{id}: a read-only tool must be safe to retry.");
                    }

                    Assert.That(operation.TryGetProperty("facade_operation", out var facade), Is.True, $"api/mcp.json#{id}: no facade_operation.");
                    var references = facade.ValueKind switch
                    {
                        JsonValueKind.String => new[] { facade.GetString()! },
                        JsonValueKind.Array => facade.EnumerateArray().Select(r => r.GetString()!).ToArray(),
                        _ => Array.Empty<string>(),
                    };
                    foreach (var reference in references)
                    {
                        var target = ResolveFacade(reference, facades);
                        Assert.That(target.HasValue, Is.True, $"api/mcp.json#{id}: facade_operation '{reference}' does not resolve.");
                        if (target is { } resolved && facade.ValueKind == JsonValueKind.String)
                        {
                            foreach (var field in new[] { "retry", "idempotency", "concurrency" })
                            {
                                Assert.That(Canonical(operation.GetProperty(field)), Is.EqualTo(Canonical(resolved.GetProperty(field))),
                                    $"api/mcp.json#{id}: {field} must equal {reference}.{field}.");
                            }
                        }
                    }
                }
            });
        }
        finally
        {
            foreach (var document in facades.Values)
            {
                document.Dispose();
            }
        }

        Assert.That(examined, Is.GreaterThan(0), "No MCP operations were examined.");
    }

    [Test]
    public void Every_package_reference_is_a_published_nuget_id()
    {
        var failures = new List<string>();
        var examined = 0;
        foreach (var relative in ListAgentFiles())
        {
            var text = ReadAgentText(relative);
            if (relative.EndsWith(".json", StringComparison.Ordinal))
            {
                using var document = ParseJson(relative, text);
                Collect(document.RootElement, null);

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
                        case JsonValueKind.String when propertyName is "package" or "packages":
                            Check(element.GetString()!, relative);
                            break;
                    }
                }
            }
            else
            {
                int? listIndent = null;
                foreach (var line in text.Split('\n'))
                {
                    var header = YamlPackagesHeader.Match(line);
                    if (header.Success)
                    {
                        listIndent = header.Groups[1].Length;
                        continue;
                    }

                    if (listIndent is { } indent)
                    {
                        var item = YamlListItem.Match(line);
                        if (item.Success && item.Groups[1].Length >= indent)
                        {
                            Check(item.Groups[2].Value, relative);
                            continue;
                        }

                        listIndent = null;
                    }

                    var single = YamlPackageScalar.Match(line);
                    if (single.Success)
                    {
                        Check(single.Groups[1].Value, relative);
                    }
                }
            }
        }

        Assert.That(examined, Is.GreaterThan(0), "No package references were examined.");
        Assert.That(failures, Is.Empty, string.Join('\n', failures));

        void Check(string value, string relative)
        {
            examined++;
            if (!NuGetPackageId.IsMatch(value))
            {
                failures.Add($"{relative}: package '{value}' is not a published NuGet id (use 'Orleans.Lattice...' casing, never a directory name).");
            }
        }
    }

    [Test]
    public void No_xml_documentation_debris_survives_into_agent_docs()
    {
        var failures = new List<string>();
        var examined = 0;
        foreach (var relative in ListAgentFiles())
        {
            var text = ReadAgentText(relative);
            var strings = relative.EndsWith(".json", StringComparison.Ordinal)
                ? JsonStrings(relative, text)
                : YamlStrings(relative, text, new List<string>());
            foreach (var value in strings)
            {
                examined++;
                if (GeneratorDebris.IsMatch(value))
                {
                    failures.Add($"{relative}: '{value}' carries XML documentation or attribute debris.");
                }
            }
        }

        Assert.That(examined, Is.GreaterThan(0), "No strings were examined.");
        Assert.That(failures, Is.Empty, string.Join('\n', failures));
    }

    private static IEnumerable<string> ApiFiles() =>
        ListAgentFiles().Where(p => p.StartsWith("api/", StringComparison.Ordinal) && p.EndsWith(".json", StringComparison.Ordinal));

    private static JsonElement? ResolveFacade(string reference, Dictionary<string, JsonDocument> cache)
    {
        var match = FacadeReference.Match(reference);
        if (!match.Success || match.Groups["file"].Value == "mcp")
        {
            return null;
        }

        var relative = $"api/{match.Groups["file"].Value}.json";
        if (!File.Exists(Path.Combine(AgentsRoot, relative)))
        {
            return null;
        }

        if (!cache.TryGetValue(relative, out var document))
        {
            document = ParseJson(relative, ReadAgentText(relative));
            cache[relative] = document;
        }

        foreach (var operation in document.RootElement.GetProperty("operations").EnumerateArray())
        {
            if (operation.GetProperty("id").GetString() == match.Groups["id"].Value)
            {
                return operation;
            }
        }

        return null;
    }

    private static string Canonical(JsonElement element)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            element.WriteTo(writer);
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
