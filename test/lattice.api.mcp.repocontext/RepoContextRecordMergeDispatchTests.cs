using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for the record-family dispatch of the default
/// <see cref="RepoContextRecordMerge"/> strategy across the structural families -
/// repo, package, symbol, and content - and the verbatim fall-through for a key
/// whose family the strategy does not fold. The file and memory families are
/// covered by <see cref="RepoContextRecordMergeTests_Portability"/>.
/// </summary>
[TestFixture]
public sealed class RepoContextRecordMergeDispatchTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public void Default_merge_folds_a_repo_record_last_writer_wins()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var key = RepoContextKeys.Repo("acme");

        var existing = new RepoNode { RepoId = "acme", DisplayName = RepoContextValues.Lww("Old", Clock(100)) };
        var incoming = new RepoNode { RepoId = "acme", DisplayName = RepoContextValues.Lww("New", Clock(200)) };

        var merged = _serializer.Deserialize<RepoNode>(
            merge(key, _serializer.SerializeToArray(existing), _serializer.SerializeToArray(incoming)));

        Assert.That(RepoContextValues.ReadString(merged.DisplayName), Is.EqualTo("New"));
    }

    [Test]
    public void Default_merge_folds_a_package_record_last_writer_wins()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var key = RepoContextKeys.Package("acme", "src/pkg");

        var existing = new PackageNode { RepoId = "acme", Path = "src/pkg", Version = RepoContextValues.Lww("1.0", Clock(100)) };
        var incoming = new PackageNode { RepoId = "acme", Path = "src/pkg", Version = RepoContextValues.Lww("2.0", Clock(200)) };

        var merged = _serializer.Deserialize<PackageNode>(
            merge(key, _serializer.SerializeToArray(existing), _serializer.SerializeToArray(incoming)));

        Assert.That(RepoContextValues.ReadString(merged.Version), Is.EqualTo("2.0"));
    }

    [Test]
    public void Default_merge_folds_a_symbol_record_last_writer_wins()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var key = RepoContextKeys.Symbol("acme", "N.C.M()");

        var existing = new SymbolRecord
        {
            RepoId = "acme",
            FullyQualifiedName = "N.C.M()",
            Signature = RepoContextValues.Lww("void M()", Clock(100)),
        };
        var incoming = new SymbolRecord
        {
            RepoId = "acme",
            FullyQualifiedName = "N.C.M()",
            Signature = RepoContextValues.Lww("int M()", Clock(200)),
        };

        var merged = _serializer.Deserialize<SymbolRecord>(
            merge(key, _serializer.SerializeToArray(existing), _serializer.SerializeToArray(incoming)));

        Assert.That(RepoContextValues.ReadString(merged.Signature), Is.EqualTo("int M()"));
    }

    [Test]
    public void Default_merge_folds_a_content_record_last_writer_wins()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var key = RepoContextKeys.Content("acme", "src/a.cs");

        var existing = new ContentRecord { RepoId = "acme", Path = "src/a.cs", Text = RepoContextValues.Lww("old body", Clock(100)) };
        var incoming = new ContentRecord { RepoId = "acme", Path = "src/a.cs", Text = RepoContextValues.Lww("new body", Clock(200)) };

        var merged = _serializer.Deserialize<ContentRecord>(
            merge(key, _serializer.SerializeToArray(existing), _serializer.SerializeToArray(incoming)));

        Assert.That(RepoContextValues.ReadString(merged.Text), Is.EqualTo("new body"));
    }

    [Test]
    public void Default_merge_returns_incoming_for_a_non_folded_family()
    {
        // A vector-family key parses but is not one of the folded record families,
        // so the strategy stores the incoming bytes verbatim rather than folding.
        var merge = RepoContextRecordMerge.Default(_serializer);
        var incoming = new byte[] { 9, 8, 7 };

        var result = merge(RepoContextKeys.Vector("acme", "vec-1"), new byte[] { 1 }, incoming);

        Assert.That(result, Is.EqualTo(incoming));
    }
}
