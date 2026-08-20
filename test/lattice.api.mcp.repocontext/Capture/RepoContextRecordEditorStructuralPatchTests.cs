using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Unit tests for <see cref="RepoContextRecordEditor"/> patching the structural
/// record families - repo, package, and symbol - not covered by
/// <see cref="RepoContextRecordEditorTests"/>. Each asserts that a scalar patch is
/// folded through the record model's CRDT merge and that an unknown field for the
/// family is rejected with an <see cref="McpException"/>.
/// </summary>
[TestFixture]
public sealed class RepoContextRecordEditorStructuralPatchTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static RepoContextKey Parse(string key)
    {
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True, key);
        return parsed;
    }

    private static RepoContextRecordEditor.PatchResult Patch(
        RepoContextKey key, byte[] existing, IReadOnlyDictionary<string, string> fields, long ticks)
        => RepoContextRecordEditor.Patch(
            key, existing, fields, addTags: null, removeTags: null,
            addLinks: null, removeLinks: null, Clock(ticks), Serializer);

    [Test]
    public void Patch_sets_repo_scalar_fields_through_merge()
    {
        var key = Parse(RepoContextKeys.Repo("acme"));
        var existing = Serializer.SerializeToArray(new RepoNode { RepoId = "acme" });

        var result = Patch(
            key, existing,
            new Dictionary<string, string>
            {
                ["displayName"] = "Acme",
                ["defaultBranch"] = "main",
                ["lastIngested"] = "2024-01-01",
            },
            ticks: 100);

        var merged = Serializer.Deserialize<RepoNode>(result.Merged);
        Assert.Multiple(() =>
        {
            Assert.That(result.FieldsUpdated, Is.EqualTo(3));
            Assert.That(RepoContextValues.ReadString(merged.DisplayName), Is.EqualTo("Acme"));
            Assert.That(RepoContextValues.ReadString(merged.DefaultBranch), Is.EqualTo("main"));
            Assert.That(RepoContextValues.ReadString(merged.LastIngested), Is.EqualTo("2024-01-01"));
        });
    }

    [Test]
    public void Patch_rejects_an_unknown_repo_field()
    {
        var key = Parse(RepoContextKeys.Repo("acme"));
        var existing = Serializer.SerializeToArray(new RepoNode { RepoId = "acme" });

        Assert.That(
            () => Patch(key, existing, new Dictionary<string, string> { ["nonsense"] = "x" }, ticks: 100),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Patch_sets_package_scalar_fields_through_merge()
    {
        var key = Parse(RepoContextKeys.Package("acme", "src/pkg"));
        var existing = Serializer.SerializeToArray(new PackageNode { RepoId = "acme", Path = "src/pkg" });

        var result = Patch(
            key, existing,
            new Dictionary<string, string>
            {
                ["language"] = "csharp",
                ["version"] = "1.2.3",
                ["lastIngested"] = "2024-02-02",
            },
            ticks: 100);

        var merged = Serializer.Deserialize<PackageNode>(result.Merged);
        Assert.Multiple(() =>
        {
            Assert.That(result.FieldsUpdated, Is.EqualTo(3));
            Assert.That(RepoContextValues.ReadString(merged.Language), Is.EqualTo("csharp"));
            Assert.That(RepoContextValues.ReadString(merged.Version), Is.EqualTo("1.2.3"));
            Assert.That(RepoContextValues.ReadString(merged.LastIngested), Is.EqualTo("2024-02-02"));
        });
    }

    [Test]
    public void Patch_rejects_an_unknown_package_field()
    {
        var key = Parse(RepoContextKeys.Package("acme", "src/pkg"));
        var existing = Serializer.SerializeToArray(new PackageNode { RepoId = "acme", Path = "src/pkg" });

        Assert.That(
            () => Patch(key, existing, new Dictionary<string, string> { ["nonsense"] = "x" }, ticks: 100),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Patch_sets_symbol_scalar_and_integer_fields_through_merge()
    {
        var key = Parse(RepoContextKeys.Symbol("acme", "N.C.M()"));
        var existing = Serializer.SerializeToArray(
            new SymbolRecord { RepoId = "acme", FullyQualifiedName = "N.C.M()" });

        var result = Patch(
            key, existing,
            new Dictionary<string, string>
            {
                ["filePath"] = "src/C.cs",
                ["startLine"] = "10",
                ["endLine"] = "20",
                ["signature"] = "public void M()",
                ["digest"] = "abc",
            },
            ticks: 100);

        var merged = Serializer.Deserialize<SymbolRecord>(result.Merged);
        Assert.Multiple(() =>
        {
            Assert.That(result.FieldsUpdated, Is.EqualTo(5));
            Assert.That(RepoContextValues.ReadString(merged.FilePath), Is.EqualTo("src/C.cs"));
            Assert.That(RepoContextValues.ReadInt64(merged.StartLine), Is.EqualTo(10));
            Assert.That(RepoContextValues.ReadInt64(merged.EndLine), Is.EqualTo(20));
            Assert.That(RepoContextValues.ReadString(merged.Signature), Is.EqualTo("public void M()"));
            Assert.That(RepoContextValues.ReadString(merged.Digest), Is.EqualTo("abc"));
        });
    }

    [Test]
    public void Patch_rejects_a_non_integer_symbol_line_value()
    {
        var key = Parse(RepoContextKeys.Symbol("acme", "N.C.M()"));
        var existing = Serializer.SerializeToArray(
            new SymbolRecord { RepoId = "acme", FullyQualifiedName = "N.C.M()" });

        Assert.That(
            () => Patch(key, existing, new Dictionary<string, string> { ["startLine"] = "ten" }, ticks: 100),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Patch_sets_the_file_last_ingested_field()
    {
        var key = Parse(RepoContextKeys.File("acme", "a.cs"));
        var existing = Serializer.SerializeToArray(new FileNode { RepoId = "acme", Path = "a.cs" });

        var result = Patch(
            key, existing, new Dictionary<string, string> { ["lastIngested"] = "2024-03-03" }, ticks: 100);

        var merged = Serializer.Deserialize<FileNode>(result.Merged);
        Assert.That(RepoContextValues.ReadString(merged.LastIngested), Is.EqualTo("2024-03-03"));
    }
}
