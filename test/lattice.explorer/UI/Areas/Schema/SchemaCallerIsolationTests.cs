using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>Same-route schema directory reads and editors belong to the current caller.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaCallerIsolationTests : SchemaTestContext
{
    [Test]
    public async Task An_identity_change_removes_previous_policy_summaries_from_the_directory()
    {
        UseEstate();
        var cut = RenderAt<SchemaDirectoryPage>("schema?show=all");
        cut.WaitUntil(() => Assert.That(cut.Find("tbody").TextContent, Does.Contain("3 rules")));
        Schema.DefaultCapabilities = tree => FakeSchemaControl.All(tree) with
        {
            CanViewPolicy = false,
            CanManagePolicy = false,
            CanViewVersionConfig = false,
            CanManageVersion = false,
            CanViewRemediationStatus = false,
            CanRemediate = false,
            CanScanCompliance = false,
            CanViewDeadLetters = false,
        };

        await cut.InvokeAsync(() => Auth.SignIn("restricted"));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("tbody").TextContent, Does.Not.Contain("3 rules"));
            Assert.That(cut.Find("tbody").TextContent, Does.Contain("Not permitted"));
            Assert.That(cut.Instance.ScanTargets, Is.Empty);
        });
    }
}
