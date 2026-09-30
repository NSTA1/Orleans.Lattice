using System.Text.RegularExpressions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.AppKit;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.UI.Framing.Broker;
using F1 = Orleans.Lattice.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// Pins the host's protocol constants to their owners: the bridge vocabulary and protocol
/// version to F1, and every message name and limit to AppKit's <c>AppKitProtocol</c> (F5).
/// </summary>
[TestFixture]
public sealed class AppFrameProtocolTests
{
    [Test]
    public void Operations_are_exactly_F1s_bridge_vocabulary()
    {
        Assert.That(AppFrameProtocol.Operations, Is.EquivalentTo(F1.AppUiBridgeOperations.All));
    }

    [Test]
    public void Version_is_F1s_protocol_version()
    {
        Assert.That(AppFrameProtocol.Version, Is.EqualTo(F1.AppUiProtocol.Current));
    }

    [Test]
    public void IsDataOperation_matches_F1_for_every_operation_and_a_stranger()
    {
        Assert.Multiple(() =>
        {
            foreach (var op in F1.AppUiBridgeOperations.All.Append("data.readx").Append(string.Empty))
            {
                Assert.That(AppFrameProtocol.IsDataOperation(op), Is.EqualTo(F1.AppUiBridgeOperations.IsDataOperation(op)), op);
            }

            Assert.That(AppFrameProtocol.IsDataOperation(null), Is.False);
        });
    }

    [TestCase("orders", true)]
    [TestCase("a", true)]
    [TestCase("log_v-2", true)]
    [TestCase("0log", false)]
    [TestCase("a.b", false)]
    [TestCase("", false)]
    [TestCase("Orders", false)]
    [TestCase("-orders", false)]
    [TestCase(".orders", false)]
    [TestCase("a/taskboard/orders", false)]
    [TestCase("t/default/a/taskboard/orders", false)]
    [TestCase("_lattice_registry", false)]
    [TestCase("app:taskboard", false)]
    [TestCase("orders ", false)]
    [TestCase("\u043erders", false)]
    public void IsLogicalTreeName_accepts_only_logical_names(string tree, bool expected)
    {
        Assert.That(AppFrameProtocol.IsLogicalTreeName(tree), Is.EqualTo(expected));
    }

    [Test]
    public void IsLogicalTreeName_bounds_the_length()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppFrameProtocol.IsLogicalTreeName(new string('a', AppFrameProtocol.MaxTreeNameLength)), Is.True);
            Assert.That(AppFrameProtocol.IsLogicalTreeName(new string('a', AppFrameProtocol.MaxTreeNameLength + 1)), Is.False);
        });
    }

    [TestCase(AppBridgeFailure.Denied, "denied")]
    [TestCase(AppBridgeFailure.NotFound, "not_found")]
    [TestCase(AppBridgeFailure.Invalid, "invalid")]
    [TestCase(AppBridgeFailure.TooLarge, "too_large")]
    [TestCase(AppBridgeFailure.Conflict, "conflict")]
    [TestCase(AppBridgeFailure.Unavailable, "unavailable")]
    [TestCase((AppBridgeFailure)99, "denied")]
    [TestCase((AppBridgeFailure)(-1), "denied")]
    public void MapFailure_maps_the_closed_set_and_denies_anything_else(AppBridgeFailure failure, string code)
    {
        Assert.That(AppBridgeBroker.MapFailure(failure), Is.EqualTo(code));
    }

    [Test]
    public void Every_error_code_has_a_fixed_message_and_an_unknown_code_reads_as_denied()
    {
        Assert.Multiple(() =>
        {
            foreach (var code in AppFrameProtocol.ErrorCodes)
            {
                Assert.That(AppBridgeBroker.MessageFor(code), Is.Not.Empty, code);
            }

            Assert.That(AppBridgeBroker.MessageFor("anything"), Is.EqualTo(AppBridgeBroker.MessageFor(AppFrameProtocol.ErrorDenied)));
            Assert.That(AppFrameProtocol.ErrorCodes, Is.EquivalentTo(new[] { "denied", "not_found", "invalid", "too_large", "rate_limited", "unavailable", "conflict" }));
        });
    }

    [Test]
    public void The_host_speaks_AppKits_protocol_sets_and_version()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppFrameProtocol.Version, Is.EqualTo(AppKitProtocol.Version));
            Assert.That(AppFrameProtocol.Operations, Is.EquivalentTo(AppKitProtocol.Operations.All));
            Assert.That(AppFrameProtocol.ErrorCodes, Is.EquivalentTo(AppKitProtocol.ErrorCodes.All));
            Assert.That(AppFrameProtocol.FailureCodes, Is.EquivalentTo(AppKitProtocol.FailureCodes.All));
            Assert.That(
                new[] { AppFrameProtocol.RevokedDisabled, AppFrameProtocol.RevokedUninstalled, AppFrameProtocol.RevokedUpgraded, AppFrameProtocol.RevokedRevision, AppFrameProtocol.RevokedClosed },
                Is.EquivalentTo(AppKitProtocol.RevokedReasons.All));
            Assert.That(AppFrameRoute.BootstrapDocument, Is.EqualTo(AppKitProtocol.FrameDocument));
            Assert.That(AppFrameRoute.AppKitContentPath, Does.EndWith("/" + AppKitProtocol.AssetDirectory));
        });
    }

    [Test]
    public void IsLogicalTreeName_agrees_with_AppKits_tree_name_pattern()
    {
        var pattern = new Regex(AppKitProtocol.TreeNamePattern, RegexOptions.CultureInvariant);
        string[] corpus =
        [
            "orders", "a", "a1", "a_b-c", "log_v-2", "", "0log", "_x", "-x", "a.b", "A", "aB", "a/b", "t/default/a/app/orders",
            "a:b", "a b", "a\u0131", "\u0430", new string('a', AppKitProtocol.Limits.MaxTreeNameLength),
        ];

        Assert.Multiple(() =>
        {
            foreach (var tree in corpus)
            {
                var expected = tree.Length <= AppKitProtocol.Limits.MaxTreeNameLength && pattern.IsMatch(tree);
                Assert.That(AppFrameProtocol.IsLogicalTreeName(tree), Is.EqualTo(expected), tree);
            }

            Assert.That(AppFrameProtocol.IsLogicalTreeName(new string('a', AppKitProtocol.Limits.MaxTreeNameLength + 1)), Is.False);
        });
    }
}