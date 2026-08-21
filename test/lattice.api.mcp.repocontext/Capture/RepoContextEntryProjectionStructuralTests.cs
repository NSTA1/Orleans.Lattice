using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Unit tests for <see cref="RepoContextEntryProjection"/> projecting the
/// structural record families - repo, package, and symbol - not covered by
/// <see cref="RepoContextEntryProjectionTests"/>, plus the key-rebuild path for a
/// vector-family key whose value carries no projected fields.
/// </summary>
[TestFixture]
public sealed class RepoContextEntryProjectionStructuralTests
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

    [Test]
    public void Project_flattens_a_repo_record_scalars_and_tags()
    {
        var key = Parse(RepoContextKeys.Repo("acme"));
        var record = new RepoNode
        {
            RepoId = "acme",
            DisplayName = RepoContextValues.Lww("Acme", Clock(1)),
            DefaultBranch = RepoContextValues.Lww("main", Clock(1)),
            LastIngested = RepoContextValues.Lww("2024-01-01", Clock(1)),
        };
        record.Tags.Add(Encoding.UTF8.GetBytes("primary"), "a", 0);

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.Multiple(() =>
        {
            Assert.That(view.Kind, Is.EqualTo("Repo"));
            Assert.That(view.Key, Is.EqualTo(RepoContextKeys.Repo("acme")));
            Assert.That(view.Fields["displayName"], Is.EqualTo("Acme"));
            Assert.That(view.Fields["defaultBranch"], Is.EqualTo("main"));
            Assert.That(view.Fields["lastIngested"], Is.EqualTo("2024-01-01"));
            Assert.That(view.Tags, Is.EqualTo(new[] { "primary" }));
        });
    }

    [Test]
    public void Project_flattens_a_package_record_scalars()
    {
        var key = Parse(RepoContextKeys.Package("acme", "src/pkg"));
        var record = new PackageNode
        {
            RepoId = "acme",
            Path = "src/pkg",
            Language = RepoContextValues.Lww("csharp", Clock(1)),
            Version = RepoContextValues.Lww("1.2.3", Clock(1)),
        };

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.Multiple(() =>
        {
            Assert.That(view.Kind, Is.EqualTo("Package"));
            Assert.That(view.Key, Is.EqualTo(RepoContextKeys.Package("acme", "src/pkg")));
            Assert.That(view.Fields["language"], Is.EqualTo("csharp"));
            Assert.That(view.Fields["version"], Is.EqualTo("1.2.3"));
        });
    }

    [Test]
    public void Project_flattens_a_symbol_record_with_references()
    {
        var key = Parse(RepoContextKeys.Symbol("acme", "N.C.M()"));
        var record = new SymbolRecord
        {
            RepoId = "acme",
            FullyQualifiedName = "N.C.M()",
            Kind = SymbolKind.Method,
            FilePath = RepoContextValues.Lww("src/C.cs", Clock(1)),
            StartLine = RepoContextValues.Lww(10, Clock(1)),
            EndLine = RepoContextValues.Lww(20, Clock(1)),
            Signature = RepoContextValues.Lww("public void M()", Clock(1)),
            Digest = RepoContextValues.Lww("abc", Clock(1)),
        };
        record.References.Add(Encoding.UTF8.GetBytes("Widget"), "a", 0);
        record.References.Add(Encoding.UTF8.GetBytes("Gadget"), "b", 0);

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.Multiple(() =>
        {
            Assert.That(view.Kind, Is.EqualTo("Symbol"));
            Assert.That(view.FullyQualifiedName, Is.EqualTo("N.C.M()"));
            Assert.That(view.Fields["kind"], Is.EqualTo("Method"));
            Assert.That(view.Fields["filePath"], Is.EqualTo("src/C.cs"));
            Assert.That(view.Fields["startLine"], Is.EqualTo("10"));
            Assert.That(view.Fields["endLine"], Is.EqualTo("20"));
            Assert.That(view.Fields["signature"], Is.EqualTo("public void M()"));
            Assert.That(view.Fields["digest"], Is.EqualTo("abc"));
            Assert.That(view.Fields["references"], Is.EqualTo("Gadget,Widget"),
                "references are ordinal-sorted and comma-joined");
        });
    }

    [Test]
    public void Project_of_a_vector_key_rebuilds_the_key_without_projected_fields()
    {
        // A vector-family key carries no projected scalars: the switch takes its
        // default arm and only the identity is rebuilt from the parsed key.
        var key = Parse(RepoContextKeys.Vector("acme", "vec-1"));

        var view = RepoContextEntryProjection.Project(
            key, Array.Empty<byte>(), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.Multiple(() =>
        {
            Assert.That(view.Exists, Is.True);
            Assert.That(view.Fields, Is.Empty);
            Assert.That(view.Key, Is.EqualTo(RepoContextKeys.Vector("acme", "vec-1")));
        });
    }
}
