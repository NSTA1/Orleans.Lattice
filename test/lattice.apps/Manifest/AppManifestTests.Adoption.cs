using System.Text.Json.Nodes;
using Json.Schema;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    [TestCase("")]
    [TestCase(" \t")]
    [TestCase("a/")]
    [TestCase("a/other-app/records")]
    [TestCase("_lattice_")]
    [TestCase("_lattice_state")]
    [TestCase("sys-")]
    [TestCase("sys-state")]
    [TestCase("t/")]
    [TestCase("t/tenant/records")]
    public void AdoptedTreeId_rejects_empty_structural_and_reserved_ids(string adopted)
    {
        var root = JsonNode.Parse(Json)!;
        root["trees"]![0]!["adoptedTreeId"] = adopted;
        var result = AppManifestParser.Parse(root.ToJsonString());
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Any(e => e.Code == "adoption" && e.Path == "$.trees[0].adoptedTreeId"), Is.True);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False);
    }

    [TestCase(null)]
    [TestCase("repo-context-memory")]
    [TestCase("archive/records")]
    [TestCase("a")]
    [TestCase("t")]
    [TestCase("sys")]
    [TestCase("_lattice")]
    public void AdoptedTreeId_accepts_legacy_ids_and_keeps_local_scope_references(string? adopted)
    {
        var root = JsonNode.Parse(Json)!;
        root["trees"]![0]!["adoptedTreeId"] = adopted;
        var result = AppManifestParser.Parse(root.ToJsonString());
        Assert.That(result.IsValid, Is.True);
        Assert.That(result.Manifest!.Trees[0].AdoptedTreeId, Is.EqualTo(adopted));
        Assert.That(result.Manifest.Roles[0].Scopes[0].Tree, Is.EqualTo("records"));
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.True);
    }

    [TestCase(true)]
    [TestCase(false)]
    public void AdoptedTreeId_must_be_unique_across_declarations(bool duplicate)
    {
        var manifest = Manifest;
        manifest = manifest with
        {
            Trees =
            [
                manifest.Trees[0] with { AdoptedTreeId = "repo-context-memory" },
                new() { Name = "second", AdoptedTreeId = duplicate ? "repo-context-memory" : "repo-context-structural" },
            ],
        };
        var result = AppManifestValidator.Validate(manifest);
        Assert.That(result.IsValid, Is.EqualTo(!duplicate));
        if (duplicate)
            Assert.That(result.Errors.Single(), Is.EqualTo(new AppManifestError(
                "duplicate", "$.trees[1].adoptedTreeId", "A physical tree may be adopted only once per manifest.")));
    }

    [Test]
    public void AdoptedTreeId_Orleans_roundtrip_preserves_physical_mapping()
    {
        using var services = new ServiceCollection()
            .AddSerializer(builder => builder.AddAssembly(typeof(AppManifest).Assembly))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var source = Manifest.Trees[0] with { AdoptedTreeId = "repo-context-memory" };
        var copy = serializer.Deserialize<AppTreeDeclaration>(serializer.SerializeToArray(source));
        Assert.That(copy, Is.EqualTo(source));
        Assert.That(Manifest.Trees[0].AdoptedTreeId, Is.Null);
    }
}
