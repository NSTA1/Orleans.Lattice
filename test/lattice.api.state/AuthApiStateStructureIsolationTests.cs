using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Tests;

[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateStructureIsolationTests
{
    private const string TreeId = "structure-prefix-isolation";
    private AuthApiStateClusterFixture _fixture = null!;
    private NodeStateSummary _root = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new AuthApiStateClusterFixture();
        await _fixture.InitializeAsync();
        await _fixture.CreatePopulatedTreeAsync(TreeId,
            Enumerable.Range(0, 80).Select(i => $"{(i < 40 ? "private" : "public")}/{i:D3}").ToArray());
        await _fixture.GrantAsync(
            new LatticeAuthorizationRule("partial", LatticeSubjectSelector.User("partial"),
                LatticeScope.Prefix(TreeId, "public/"),
                LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Allow),
            new LatticeAuthorizationRule("whole", LatticeSubjectSelector.User("whole"),
                LatticeScope.Tree(TreeId),
                LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Allow));

        using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
        {
            var structure = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
            {
                TreeId = TreeId,
                DepthLimit = StructureRequest.MaxDepthLimit,
                MaxNodes = StructureRequest.MaxNodeBudget,
            });
            _root = structure.Roots.First(r => r.Kind == NodeKind.Internal);
            Assert.That(Boundaries(structure.Roots).Any(k => k.StartsWith("private/", StringComparison.Ordinal)),
                Is.True, "the populated topology must actually contain protected split keys");
        }
    }

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [TestCase("whole")]
    [TestCase("shard")]
    [TestCase("subpath")]
    public async Task Structure_partial_reader_cannot_recover_protected_split_keys(string target)
    {
        var request = new StructureRequest
        {
            TreeId = TreeId,
            ShardIndex = target == "whole" ? null : _root.ShardIndex,
            SubPathNodeId = target == "subpath" ? _root.NodeId : null,
        };
        using (AuthApiStateClusterFixture.AsSubject("partial"))
        {
            var scan = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = TreeId });
            Assert.That(scan.Entries, Is.Not.Empty);
            Assert.That(scan.Entries.All(e => e.Key.StartsWith("public/", StringComparison.Ordinal)), Is.True);

            var structure = await _fixture.Query.GetTreeStructureAsync(request);
            Assert.That(structure.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(structure.Roots, Is.Empty);
        }

        foreach (var subject in new[] { "whole", AuthApiStateClusterFixture.BootstrapAdmin })
        {
            using (AuthApiStateClusterFixture.AsSubject(subject))
            {
                var structure = await _fixture.Query.GetTreeStructureAsync(request);
                Assert.That(structure.Status, Is.EqualTo(StateQueryStatus.Found));
                Assert.That(structure.Roots, Is.Not.Empty);
            }
        }
    }

    private static IEnumerable<string> Boundaries(IEnumerable<NodeStateSummary> nodes)
    {
        foreach (var node in nodes)
        {
            if (node.KeyRangeLow is { } low)
            {
                yield return low;
            }

            if (node.KeyRangeHigh is { } high)
            {
                yield return high;
            }

            foreach (var boundary in Boundaries(node.Children))
            {
                yield return boundary;
            }
        }
    }
}
