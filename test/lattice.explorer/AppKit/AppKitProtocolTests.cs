using System.Reflection;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Explorer.AppKit;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// <see cref="AppKitProtocol"/> is the host's spelling of the frame protocol.
/// Its operation names are pinned to the one bridge vocabulary, F1's
/// <see cref="AppUiBridgeOperations"/>, and every other name and bound is pinned
/// to its literal, so a rename on either side fails here first.
/// </summary>
[TestFixture]
public sealed class AppKitProtocolTests
{
    [Test]
    public void Version_is_the_apps_ui_protocol_version()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppKitProtocol.Version, Is.EqualTo(AppUiProtocol.Current));
            Assert.That(AppKitProtocol.Version, Is.EqualTo(1));
        });
    }

    [Test]
    public void Operations_are_exactly_the_bridge_vocabulary()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppKitProtocol.Operations.All, Is.EquivalentTo(AppUiBridgeOperations.All));
            Assert.That(AppKitProtocol.Operations.All, Is.Unique);
            Assert.That(AppKitProtocol.Operations.ContextRead, Is.EqualTo(AppUiBridgeOperations.ContextRead));
            Assert.That(AppKitProtocol.Operations.ContextUser, Is.EqualTo(AppUiBridgeOperations.ContextUser));
            Assert.That(AppKitProtocol.Operations.DataRead, Is.EqualTo(AppUiBridgeOperations.DataRead));
            Assert.That(AppKitProtocol.Operations.DataWrite, Is.EqualTo(AppUiBridgeOperations.DataWrite));
            Assert.That(AppKitProtocol.Operations.DataDelete, Is.EqualTo(AppUiBridgeOperations.DataDelete));
            Assert.That(AppKitProtocol.Operations.NavSync, Is.EqualTo(AppUiBridgeOperations.NavSync));
            Assert.That(AppKitProtocol.Operations.UiNotify, Is.EqualTo(AppUiBridgeOperations.UiNotify));
        });
    }

    [Test]
    public void Messages_are_pinned()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppKitProtocol.Messages.Ready, Is.EqualTo("lattice.ready"));
            Assert.That(AppKitProtocol.Messages.Hello, Is.EqualTo("lattice.hello"));
            Assert.That(AppKitProtocol.Messages.Bundle, Is.EqualTo("lattice.bundle"));
            Assert.That(AppKitProtocol.Messages.Loaded, Is.EqualTo("lattice.loaded"));
            Assert.That(AppKitProtocol.Messages.Failed, Is.EqualTo("lattice.failed"));
        });
    }

    [Test]
    public void Events_are_pinned()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppKitProtocol.Events.ContextChanged, Is.EqualTo("context.changed"));
            Assert.That(AppKitProtocol.Events.NavChanged, Is.EqualTo("nav.changed"));
            Assert.That(AppKitProtocol.Events.Revoked, Is.EqualTo("lattice.revoked"));
        });
    }

    [Test]
    public void Revoked_reasons_are_pinned()
    {
        Assert.That(
            new[]
            {
                AppKitProtocol.RevokedReasons.Disabled, AppKitProtocol.RevokedReasons.Uninstalled,
                AppKitProtocol.RevokedReasons.Upgraded, AppKitProtocol.RevokedReasons.Revision,
                AppKitProtocol.RevokedReasons.Closed,
            },
            Is.EqualTo(new[] { "disabled", "uninstalled", "upgraded", "revision", "closed" }));
    }

    [Test]
    public void Data_actions_are_pinned()
    {
        Assert.That(
            new[] { AppKitProtocol.DataActions.Get, AppKitProtocol.DataActions.Scan, AppKitProtocol.DataActions.Set, AppKitProtocol.DataActions.Delete },
            Is.EqualTo(new[] { "get", "scan", "set", "delete" }));
    }

    [Test]
    public void Error_codes_are_the_closed_set()
    {
        Assert.That(
            new[]
            {
                AppKitProtocol.ErrorCodes.Denied, AppKitProtocol.ErrorCodes.NotFound, AppKitProtocol.ErrorCodes.Invalid,
                AppKitProtocol.ErrorCodes.TooLarge, AppKitProtocol.ErrorCodes.RateLimited,
                AppKitProtocol.ErrorCodes.Unavailable, AppKitProtocol.ErrorCodes.Conflict,
            },
            Is.EqualTo(new[] { "denied", "not_found", "invalid", "too_large", "rate_limited", "unavailable", "conflict" }));
    }

    [Test]
    public void Failure_codes_are_the_closed_set()
    {
        Assert.That(
            new[]
            {
                AppKitProtocol.FailureCodes.ProtocolUnsupported, AppKitProtocol.FailureCodes.BundleMalformed,
                AppKitProtocol.FailureCodes.BundleTooLarge, AppKitProtocol.FailureCodes.AssetMissing,
                AppKitProtocol.FailureCodes.DigestMismatch, AppKitProtocol.FailureCodes.BundleDigestMismatch,
                AppKitProtocol.FailureCodes.CryptoUnavailable, AppKitProtocol.FailureCodes.LoadFailed,
                AppKitProtocol.FailureCodes.Internal,
            },
            Is.EqualTo(new[]
            {
                "protocol_unsupported", "bundle_malformed", "bundle_too_large", "asset_missing", "digest_mismatch",
                "bundle_digest_mismatch", "crypto_unavailable", "load_failed", "internal",
            }));
    }

    [Test]
    [TestCaseSource(nameof(NameSets))]
    public void Every_All_list_holds_exactly_its_class_constants(Type type)
    {
        var constants = type.GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field.IsLiteral && field.FieldType == typeof(string))
            .Select(field => (string)field.GetRawConstantValue()!)
            .ToArray();
        var all = (IReadOnlyList<string>)type.GetProperty("All", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;

        Assert.Multiple(() =>
        {
            Assert.That(all, Is.EquivalentTo(constants), "All lists every constant");
            Assert.That(all, Is.Unique);
        });
    }

    [Test]
    public void The_name_sets_do_not_overlap_where_the_port_must_tell_them_apart()
    {
        // Messages and events share the port's "type" member, so no name may mean both.
        Assert.That(AppKitProtocol.Messages.All.Intersect(AppKitProtocol.Events.All), Is.Empty);
    }

    [Test]
    public void The_limits_are_pinned_and_consistent_with_the_bundle_rules()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppKitProtocol.Limits.MaxValueBytes, Is.EqualTo(65536));
            Assert.That(AppKitProtocol.Limits.MaxValueBase64Length, Is.EqualTo(Convert.ToBase64String(new byte[65536]).Length));
            Assert.That(AppKitProtocol.Limits.MaxRequestBytes, Is.EqualTo(131072));
            Assert.That(AppKitProtocol.Limits.MaxResponseBytes, Is.EqualTo(1048576));
            Assert.That(AppKitProtocol.Limits.MaxPageSize, Is.EqualTo(200));
            Assert.That(AppKitProtocol.Limits.MaxNotifyLength, Is.EqualTo(200));
            Assert.That(AppKitProtocol.Limits.MaxKeyLength, Is.EqualTo(1024));
            Assert.That(AppKitProtocol.Limits.MaxTreeNameLength, Is.EqualTo(128));
            Assert.That(AppKitProtocol.Limits.MaxPathLength, Is.EqualTo(1024));
            Assert.That(AppKitProtocol.Limits.MaxContinuationLength, Is.EqualTo(4096));
            Assert.That(AppKitProtocol.Limits.DefaultTimeoutMilliseconds, Is.EqualTo(30_000));
            Assert.That(AppKitProtocol.Limits.MaxTimeoutMilliseconds, Is.EqualTo(300_000));
            Assert.That(AppKitProtocol.Limits.DefaultTimeoutMilliseconds, Is.LessThanOrEqualTo(AppKitProtocol.Limits.MaxTimeoutMilliseconds));
            Assert.That(AppKitProtocol.Limits.MaxValueBase64Length, Is.LessThan(AppKitProtocol.Limits.MaxRequestBytes),
                "a maximal set request must fit inside the request envelope bound");
        });
    }

    [Test]
    public void The_asset_location_names_the_shipped_bootstrap()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppKitProtocol.AssetDirectory, Is.EqualTo("appkit/v" + AppKitProtocol.Version));
            Assert.That(AppKitProtocol.FrameDocument, Is.EqualTo("frame.html"));
            var onDisk = AppKitPaths.Project + "/wwwroot/" + AppKitProtocol.AssetDirectory + "/" + AppKitProtocol.FrameDocument;
            Assert.That(File.Exists(AppKitPaths.Absolute(onDisk)), Is.True, onDisk);
        });
    }

    [TestCase("notes", true)]
    [TestCase("my_tree-2", true)]
    [TestCase("a", true)]
    [TestCase("a/app/notes", false)]
    [TestCase("Notes", false)]
    [TestCase("2notes", false)]
    [TestCase("", false)]
    [TestCase("*", false)]
    public void The_tree_name_pattern_admits_only_local_names(string name, bool expected)
    {
        Assert.That(System.Text.RegularExpressions.Regex.IsMatch(name, AppKitProtocol.TreeNamePattern), Is.EqualTo(expected));
    }

    private static IEnumerable<TestCaseData> NameSets() =>
        new[]
        {
            typeof(AppKitProtocol.Messages), typeof(AppKitProtocol.Events), typeof(AppKitProtocol.RevokedReasons),
            typeof(AppKitProtocol.Operations), typeof(AppKitProtocol.DataActions), typeof(AppKitProtocol.ErrorCodes),
            typeof(AppKitProtocol.FailureCodes),
        }.Select(type => new TestCaseData(type).SetArgDisplayNames(type.Name));
}
