using System.Text;
using System.Text.Json.Nodes;
using Json.Schema;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    [TestCase("*")]
    [TestCase(" records")]
    [TestCase("records ")]
    [TestCase("rec\nords")]
    [TestCase("rec\u0000ords")]
    public void AdoptedTreeId_rejects_the_cluster_wide_sentinel_and_untrimmed_or_control_characters(string adopted)
    {
        var root = JsonNode.Parse(Json)!;
        root["trees"]![0]!["adoptedTreeId"] = adopted;
        var result = AppManifestParser.Parse(root.ToJsonString());
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Any(e => e.Code == "adoption" && e.Path == "$.trees[0].adoptedTreeId"), Is.True);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False);
    }

    [Test]
    public void AdoptedTreeId_rejects_an_overlong_id()
    {
        var root = JsonNode.Parse(Json)!;
        root["trees"]![0]!["adoptedTreeId"] = new string('x', AppManifestLimits.MaxTextLength + 1);
        var result = AppManifestParser.Parse(root.ToJsonString());
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Any(e => e.Path == "$.trees[0].adoptedTreeId"), Is.True);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False);
    }

    [Test]
    public void Parse_refuses_manifest_text_over_the_size_bound_before_deserializing()
    {
        var oversized = Json + new string(' ', AppManifestLimits.MaxManifestChars);
        var result = AppManifestParser.Parse(oversized);
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("too-large"));
    }

    [Test]
    public void Parse_stream_refuses_content_over_the_size_bound()
    {
        var bytes = Encoding.UTF8.GetBytes(Json + new string(' ', AppManifestLimits.MaxManifestChars));
        using var stream = new MemoryStream(bytes);
        var result = AppManifestParser.Parse(stream);
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("too-large"));
        Assert.That(stream.CanRead, Is.True);
    }

    [TestCase("trees")]
    [TestCase("roles")]
    [TestCase("subscriptions")]
    [TestCase("mcpTools")]
    public void Validate_bounds_every_section_length(string section)
    {
        var manifest = Manifest;
        var count = AppManifestLimits.MaxSectionItems + 1;
        manifest = section switch
        {
            "trees" => manifest with { Trees = Enumerable.Range(0, count).Select(i => new AppTreeDeclaration { Name = $"t{i}" }).ToArray() },
            "roles" => manifest with { Roles = Enumerable.Range(0, count).Select(i => manifest.Roles[0] with { Name = $"r{i}" }).ToArray() },
            "subscriptions" => manifest with { Subscriptions = Enumerable.Range(0, count).Select(i => manifest.Subscriptions[0] with { Name = $"s{i}" }).ToArray() },
            _ => manifest with { McpTools = Enumerable.Range(0, count).Select(i => manifest.McpTools[0] with { Name = $"m{i}" }).ToArray() },
        };

        var result = AppManifestValidator.Validate(manifest);

        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Any(e => e.Code == "limit" && e.Path == "$." + section), Is.True);
    }

    [Test]
    public void Validate_bounds_the_scopes_of_one_role()
    {
        var manifest = Manifest;
        var scope = manifest.Roles[0].Scopes[0];
        manifest = manifest with
        {
            Roles = [manifest.Roles[0] with { Scopes = Enumerable.Repeat(scope, AppManifestLimits.MaxSectionItems + 1).ToArray() }],
        };

        var result = AppManifestValidator.Validate(manifest);

        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Any(e => e.Code == "limit" && e.Path == "$.roles[0].scopes"), Is.True);
    }

    [Test]
    public void Validate_bounds_free_text_and_key_lengths()
    {
        var manifest = Manifest;
        var longText = new string('x', AppManifestLimits.MaxDescriptionLength + 1);
        var longKey = new string('k', AppManifestLimits.MaxTextLength + 1);
        manifest = manifest with
        {
            McpTools = [manifest.McpTools[0] with { Description = longText }],
            Roles = [manifest.Roles[0] with { Scopes = [manifest.Roles[0].Scopes[1] with { KeyOrPrefix = longKey }] }],
            Subscriptions = [manifest.Subscriptions[0] with { KeyPrefix = longKey }],
            Identity = manifest.Identity with { Provenance = manifest.Identity.Provenance with { Reference = longKey } },
        };

        var paths = AppManifestValidator.Validate(manifest).Errors.Where(e => e.Code == "limit").Select(e => e.Path).ToArray();

        Assert.That(paths, Is.SupersetOf(new[]
        {
            "$.mcpTools[0].description",
            "$.roles[0].scopes[0].keyOrPrefix",
            "$.subscriptions[0].keyPrefix",
            "$.identity.provenance.reference",
        }));
    }

    [Test]
    public void Validate_accepts_a_manifest_at_every_bound()
    {
        var manifest = Manifest;
        var trees = Enumerable.Range(0, AppManifestLimits.MaxSectionItems)
            .Select(i => new AppTreeDeclaration { Name = i == 0 ? "records" : $"t{i}" }).ToArray();
        manifest = manifest with { Trees = trees, Replication = null, Schema = null };

        Assert.That(AppManifestValidator.Validate(manifest).IsValid, Is.True,
            () => string.Join("; ", AppManifestValidator.Validate(manifest).Errors));
    }
}
