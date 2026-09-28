using System.Reflection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Framing;
using Orleans.Lattice.Explorer.Shell.Framing.Broker;
using F1 = Orleans.Lattice.Apps;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing;

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
    [TestCase("0.log_v-2", true)]
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

    /// <summary>
    /// Pins every host constant to AppKit's public <c>AppKitProtocol</c>. F5 (issue #3814)
    /// builds that type in parallel; until it is in the AppKit assembly this test reports
    /// itself ignored, and it activates the moment the type lands, with no edit.
    /// </summary>
    [Test]
    public void Every_constant_matches_AppKitProtocol()
    {
        var appKit = Assembly.Load("Orleans.Lattice.Explorer.AppKit");
        var protocol = appKit.GetType("Orleans.Lattice.Explorer.AppKit.AppKitProtocol");
        if (protocol is null)
        {
            Assert.Ignore("AppKitProtocol (F5, issue #3814) is not in the AppKit assembly yet.");
        }

        var constants = protocol.GetNestedTypes()
            .Prepend(protocol)
            .SelectMany(type => type.GetFields(BindingFlags.Public | BindingFlags.Static))
            .Where(field => field.IsLiteral)
            .Select(field => field.GetRawConstantValue())
            .ToHashSet();

        var hostStrings = new object[]
        {
            AppFrameProtocol.Ready, AppFrameProtocol.Hello, AppFrameProtocol.Bundle, AppFrameProtocol.Loaded, AppFrameProtocol.Failed,
            AppFrameProtocol.ContextChanged, AppFrameProtocol.NavChanged, AppFrameProtocol.Revoked,
            AppFrameProtocol.ActionGet, AppFrameProtocol.ActionScan, AppFrameProtocol.ActionSet, AppFrameProtocol.ActionDelete,
        }
            .Concat(AppFrameProtocol.Operations)
            .Concat(AppFrameProtocol.ErrorCodes)
            .Concat(AppFrameProtocol.FailureCodes);

        var hostNumbers = new object[]
        {
            AppFrameProtocol.MaxValueBytes, AppFrameProtocol.MaxResponseBytes, AppFrameProtocol.MaxRequestBytes,
            AppFrameProtocol.MaxPageSize, AppFrameProtocol.MaxNotifyLength, AppFrameProtocol.MaxKeyLength,
            AppFrameProtocol.MaxTreeNameLength, AppFrameProtocol.MaxPathLength, AppFrameProtocol.MaxContinuationLength,
        };

        Assert.Multiple(() =>
        {
            foreach (var value in hostStrings.Concat(hostNumbers))
            {
                Assert.That(constants, Does.Contain(value), $"AppKitProtocol declares no constant equal to {value}");
            }
        });
    }
}
