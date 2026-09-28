using System.Collections.Immutable;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Framing;
using Orleans.Lattice.Explorer.Shell.Framing.Broker;
using Orleans.Lattice.Explorer.Tests.AppKit;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.AppFrameTestData;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker.BrokerHarness;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker;

/// <summary>
/// The <c>roles</c> member of <c>context.read</c>: the caller's app role names from the
/// workspace, sanitised, snapshotted per launch and refreshed by a re-launch - and every
/// reply the broker sends is valid against AppKit's shipped protocol schema.
/// </summary>
public sealed partial class AppBridgeBrokerTests
{
    private static readonly ProtocolSchema FrameSchema = ProtocolSchema.Load();

    [Test]
    public async Task Context_read_reports_the_callers_roles_from_the_workspace()
    {
        var harness = await CreateAsync();

        var result = AssertOk(await harness.SendAsync(1, "context.read"), 1);

        Assert.That(result.GetProperty("roles").EnumerateArray().Select(role => role.GetString()), Is.EqualTo(new[] { "viewer" }));
    }

    [Test]
    public async Task Context_read_sends_only_well_formed_unique_role_names_up_to_the_bound()
    {
        var hostile = new[] { "editor", "Editor", "", "a/b", "app:taskboard:viewer", "<b>", "editor", "1st", null! }
            .Concat(Enumerable.Range(0, AppFrameProtocol.MaxRoles + 10).Select(i => "r" + i))
            .ToImmutableArray();
        var result = AssertOk(await SendWithRolesAsync(hostile, "{\"id\":1,\"op\":\"context.read\",\"args\":{}}"), 1);

        var roles = result.GetProperty("roles").EnumerateArray().Select(role => role.GetString()!).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(roles, Has.Length.EqualTo(AppFrameProtocol.MaxRoles));
            Assert.That(roles[0], Is.EqualTo("editor"));
            Assert.That(roles[1], Is.EqualTo("r0"));
            Assert.That(roles, Is.Unique);
            Assert.That(roles, Is.All.Matches<string>(role => AppFrameProtocol.IsRoleName(role)));
        });
    }

    [Test]
    public async Task Context_read_with_no_roles_sends_an_empty_list()
    {
        var result = AssertOk(await SendWithRolesAsync(default, "{\"id\":1,\"op\":\"context.read\",\"args\":{}}"), 1);

        Assert.That(result.GetProperty("roles").GetArrayLength(), Is.Zero);
    }

    [Test]
    public async Task Roles_are_a_launch_snapshot_and_a_re_launch_refreshes_them()
    {
        var harness = await CreateAsync();
        SetRoles(harness.Workspace, ["viewer", "editor"]);

        var before = AssertOk(await harness.SendAsync(1, "context.read"), 1);
        var relaunch = (await harness.Loader.AuthorizeAsync(Slug)).Launch!;
        var after = AssertOk(await harness.Broker.HandleAsync(harness.Broker.Open(relaunch), "{\"id\":2,\"op\":\"context.read\",\"args\":{}}"), 2);

        Assert.Multiple(() =>
        {
            Assert.That(before.GetProperty("roles").EnumerateArray().Select(role => role.GetString()), Is.EqualTo(new[] { "viewer" }));
            Assert.That(after.GetProperty("roles").EnumerateArray().Select(role => role.GetString()), Is.EqualTo(new[] { "viewer", "editor" }));
        });
    }

    [Test]
    public void IsRoleName_follows_the_manifest_name_rule()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppFrameProtocol.IsRoleName("viewer"), Is.True);
            Assert.That(AppFrameProtocol.IsRoleName("task_editor-2"), Is.True);
            Assert.That(AppFrameProtocol.IsRoleName(new string('r', AppFrameProtocol.MaxRoleNameLength)), Is.True);
            Assert.That(AppFrameProtocol.IsRoleName(new string('r', AppFrameProtocol.MaxRoleNameLength + 1)), Is.False);
            Assert.That(AppFrameProtocol.IsRoleName(string.Empty), Is.False);
            Assert.That(AppFrameProtocol.IsRoleName("Viewer"), Is.False);
            Assert.That(AppFrameProtocol.IsRoleName("app:x"), Is.False);
        });
    }

    private static IEnumerable<TestCaseData> HostReplies()
    {
        yield return new TestCaseData("{\"id\":1,\"op\":\"context.read\",\"args\":{}}").SetArgDisplayNames("context.read");
        yield return new TestCaseData("{\"id\":1,\"op\":\"context.user\",\"args\":{}}").SetArgDisplayNames("context.user");
        yield return new TestCaseData("{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k1\"}}").SetArgDisplayNames("get found");
        yield return new TestCaseData("{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"none\"}}").SetArgDisplayNames("get missing");
        yield return new TestCaseData("{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"scan\",\"tree\":\"orders\",\"prefix\":\"\"}}").SetArgDisplayNames("scan");
        yield return new TestCaseData("{\"id\":1,\"op\":\"data.write\",\"args\":{\"action\":\"set\",\"tree\":\"orders\",\"key\":\"k\",\"value\":\"AA==\"}}").SetArgDisplayNames("set");
        yield return new TestCaseData("{\"id\":1,\"op\":\"data.delete\",\"args\":{\"action\":\"delete\",\"tree\":\"orders\",\"key\":\"k\"}}").SetArgDisplayNames("delete");
        yield return new TestCaseData("{\"id\":1,\"op\":\"nav.sync\",\"args\":{\"path\":\"/a\"}}").SetArgDisplayNames("nav.sync");
        yield return new TestCaseData("{\"id\":1,\"op\":\"ui.notify\",\"args\":{\"text\":\"hi\"}}").SetArgDisplayNames("ui.notify");
        yield return new TestCaseData("{\"id\":1,\"op\":\"app.uninstall\",\"args\":{}}").SetArgDisplayNames("denied");
        yield return new TestCaseData("{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"get\",\"tree\":\"orders\"}}").SetArgDisplayNames("invalid");
    }

    [TestCaseSource(nameof(HostReplies))]
    public async Task Every_broker_reply_is_valid_against_the_shipped_protocol_schema(string request)
    {
        var harness = await CreateAsync();
        harness.Host.UserDisplayName = "Ada";
        harness.Host.TenantDisplayName = "Contoso";
        harness.Bridge!.Values["k1"] = [1, 2, 3];
        harness.Bridge.Page = new() { Entries = [new() { Key = "a", Value = new byte[] { 1 } }], Continuation = "next" };

        var outcome = await harness.SendAsync(request);

        Assert.That(outcome.Reply, Is.Not.Null);
        Assert.That(FrameSchema.Validate("hostToFrame", outcome.Reply!), Is.Empty, outcome.Reply);
    }

    [Test]
    public void The_host_events_are_valid_against_the_shipped_protocol_schema()
    {
        Assert.Multiple(() =>
        {
            Assert.That(FrameSchema.Validate("hostToFrame", AppFrameMessages.NavChanged("/boards/1")), Is.Empty);
            Assert.That(FrameSchema.Validate("hostToFrame", AppFrameMessages.ContextChanged(new AppFrameAppearance("board", "more", "compact", true))), Is.Empty);
        });
    }

    private static async Task<AppBridgeOutcome> SendWithRolesAsync(ImmutableArray<string> roles, string message)
    {
        var workspace = Workspace();
        SetRoles(workspace, roles);
        var loader = new AppFrameBundleLoader(workspace, new AppFrameBundleCache(), NullLogger<AppFrameBundleLoader>.Instance);
        var broker = new AppBridgeBroker(null, loader, null, new LtToastService(), new ManualTimeProvider(), NullLogger<AppBridgeBroker>.Instance);
        var launch = (await loader.AuthorizeAsync(Slug)).Launch!;
        return await broker.HandleAsync(broker.Open(launch), message);
    }

    private static void SetRoles(FakeAppWorkspace workspace, ImmutableArray<string> roles)
    {
        var summary = workspace.Apps.Single();
        workspace.Apps[0] = summary with { Roles = roles };
    }
}
