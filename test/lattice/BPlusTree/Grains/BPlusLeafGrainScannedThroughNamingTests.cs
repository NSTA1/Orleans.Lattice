using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Keeps activation replay's scanned-through frontier named for the offsets
/// the leaf reads, rather than the entries it applies.
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainScannedThroughNamingTests
{
    [Test]
    public void Replay_scanned_through_progress_uses_names_that_describe_read_offsets()
    {
        const string sourcePath = "src/lattice/BPlusTree/Grains/BPlusLeafGrain.Activation.cs";
        var fullPath = Path.Combine(HygieneRepository.FindRepoRoot(), sourcePath);
        Assert.That(File.Exists(fullPath), Is.True, $"Missing production source: {sourcePath}");

        var syntax = CSharpSyntaxTree.ParseText(File.ReadAllText(fullPath), path: fullPath).GetRoot();
        var identifiers = syntax.DescendantTokens()
            .Where(token => token.IsKind(SyntaxKind.IdentifierToken))
            .Select(token => token.ValueText)
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(identifiers, Does.Contain("ReplayPartitionAsync"));
        Assert.Multiple(() =>
        {
            foreach (var misleadingName in new[] { "maxApplied", "MaxApplied", "perPartitionMaxApplied" })
            {
                Assert.That(
                    identifiers,
                    Does.Not.Contain(misleadingName),
                    $"Production code still names scanned-through replay progress '{misleadingName}'.");
            }

            Assert.That(identifiers, Does.Contain("maxScanned"));
            Assert.That(identifiers, Does.Contain("MaxScanned"));
            Assert.That(identifiers, Does.Contain("perPartitionMaxScanned"));
        });
    }
}
