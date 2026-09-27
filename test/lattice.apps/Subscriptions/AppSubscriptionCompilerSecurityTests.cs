using Orleans.Lattice.Auth;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The subscription compiler applies the same grantable-tree guard as the role compiler, so an
/// app can never observe a control-plane, reserved, tenant-qualified or sentinel tree whatever its
/// manifest or ceiling says. The manifests bypass the validator on purpose.
/// </summary>
[TestFixture]
public sealed class AppSubscriptionCompilerSecurityTests
{
    [TestCase("*")]
    [TestCase("sys-app-registry")]
    [TestCase("sys-tenant-registry")]
    [TestCase("_lattice_trees")]
    [TestCase("t/other/contacts")]
    public void Compile_denies_a_subscription_to_an_adopted_non_data_tree_even_when_approved(string adoptedTreeId)
    {
        var manifest = new AppManifest
        {
            Identity = new() { Slug = Notes, Version = V1 },
            Trees = [Tree("feed", adoptedTreeId)],
            Roles = [],
            Subscriptions = [Subscription("feed", "feed")],
            McpTools = [],
        };

        var result = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(TreeException(adoptedTreeId)));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Subscriptions, Is.Empty);
        Assert.That(result.Denials.Single().SubscriptionName, Is.EqualTo("feed"));
    }
}
