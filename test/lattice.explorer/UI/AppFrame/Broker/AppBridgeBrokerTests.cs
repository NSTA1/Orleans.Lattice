using Orleans.Lattice.Apps;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.UI.Framing.Broker;
using static Orleans.Lattice.Explorer.Tests.UI.Framing.Broker.BrokerHarness;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing.Broker;

/// <summary>
/// Every refusal branch of the broker, each asserting that the refusal is the answer and
/// that nothing reached the cluster: deny is the default arm.
/// </summary>
[TestFixture]
public sealed partial class AppBridgeBrokerTests
{
    private static readonly object OrdersGet = new { action = "get", tree = "orders", key = "k1" };

    [Test]
    public async Task A_well_formed_granted_request_is_relayed()
    {
        var harness = await CreateAsync();
        harness.Bridge!.Values["k1"] = [1, 2, 3];

        var result = AssertOk(await harness.SendAsync(1, "data.read", OrdersGet), 1);

        Assert.Multiple(() =>
        {
            Assert.That(result.GetProperty("found").GetBoolean(), Is.True);
            Assert.That(result.GetProperty("value").GetBytesFromBase64(), Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(harness.Bridge.Calls, Has.Count.EqualTo(1));
        });
    }

    [TestCase("")]
    [TestCase("not json")]
    [TestCase("[1,2,3]")]
    [TestCase("\"text\"")]
    [TestCase("42")]
    [TestCase("{\"op\":\"context.read\",\"args\":{}}")]
    [TestCase("{\"id\":0,\"op\":\"context.read\",\"args\":{}}")]
    [TestCase("{\"id\":-1,\"op\":\"context.read\",\"args\":{}}")]
    [TestCase("{\"id\":1.5,\"op\":\"context.read\",\"args\":{}}")]
    [TestCase("{\"id\":\"1\",\"op\":\"context.read\",\"args\":{}}")]
    [TestCase("{\"id\":9007199254740992,\"op\":\"context.read\",\"args\":{}}")]
    [TestCase("{\"id\":1,\"op\":\"context.read\",\"args\":{}/*c*/}")]
    [TestCase("{\"id\":1,\"op\":\"context.read\",\"args\":{},}")]
    [TestCase("{\"id\":1,\"op\":\"context.read\",\"args\":{\"a\":{\"b\":{\"c\":{\"d\":1}}}}}")]
    public async Task A_message_without_a_usable_id_is_dropped(string message)
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync(message);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AppBridgeOutcome.Dropped));
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("Malformed"));
        });
    }

    [Test]
    public async Task A_null_message_is_dropped()
    {
        var harness = await CreateAsync();
        Assert.That(await harness.Broker.HandleAsync(harness.Session, null), Is.EqualTo(AppBridgeOutcome.Dropped));
    }

    [Test]
    public async Task An_oversize_message_is_dropped_without_parsing()
    {
        var harness = await CreateAsync();
        var padding = new string('a', AppFrameProtocol.MaxRequestBytes);

        var outcome = await harness.SendAsync(1, "data.write", new { action = "set", tree = "orders", key = "k", value = padding });

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AppBridgeOutcome.Dropped));
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("Oversize"));
        });
    }

    [Test]
    public async Task A_message_oversize_only_in_utf8_is_dropped()
    {
        var harness = await CreateAsync();
        var text = new string('\u00e9', (AppFrameProtocol.MaxRequestBytes / 2) + 10);
        var message = "{\"id\":1,\"op\":\"ui.notify\",\"args\":{\"text\":\"" + text + "\"}}";
        Assert.That(message.Length, Is.LessThan(AppFrameProtocol.MaxRequestBytes), "only the UTF-8 form is oversize");

        var outcome = await harness.SendAsync(message);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AppBridgeOutcome.Dropped));
            Assert.That(harness.Log.Messages, Has.Some.Contains("Oversize"));
        });
    }

    [Test]
    public async Task A_lone_surrogate_never_reaches_the_toast_region()
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync("{\"id\":1,\"op\":\"ui.notify\",\"args\":{\"text\":\"\\ud800\"}}");

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Reply is null || outcome.Reply.Contains("\"invalid\"", StringComparison.Ordinal), Is.True, outcome.Reply);
            Assert.That(harness.Toasts!.Toasts, Is.Empty);
        });
    }

    [TestCase("{\"id\":1,\"op\":\"context.read\",\"args\":{},\"extra\":1}")]
    [TestCase("{\"id\":1,\"op\":\"context.read\",\"id\":2,\"args\":{}}")]
    [TestCase("{\"id\":1,\"op\":7,\"args\":{}}")]
    [TestCase("{\"id\":1,\"args\":{}}")]
    [TestCase("{\"id\":1,\"op\":\"context.read\",\"args\":[]}")]
    public async Task A_malformed_envelope_with_an_id_is_invalid(string message)
    {
        var harness = await CreateAsync();
        AssertRefused(await harness.SendAsync(message), 1, "invalid");
    }

    [TestCase("app.install")]
    [TestCase("app.uninstall")]
    [TestCase("consent.update")]
    [TestCase("auth.token")]
    [TestCase("fetch")]
    [TestCase("data.readAll")]
    [TestCase("DATA.READ")]
    [TestCase("")]
    public async Task An_operation_outside_the_vocabulary_is_denied(string op)
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync(1, op);

        AssertRefused(outcome, 1, "denied");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("UnknownOperation"));
            Assert.That(harness.Log.Messages, Has.All.Contains("for 'unknown'"), "the frame-supplied name is never logged");
        });
    }

    [TestCase(AppUiBridgeOperations.ContextRead, "{}")]
    [TestCase(AppUiBridgeOperations.ContextUser, "{}")]
    [TestCase(AppUiBridgeOperations.NavSync, "{\"path\":\"/a\"}")]
    [TestCase(AppUiBridgeOperations.UiNotify, "{\"text\":\"hi\"}")]
    [TestCase(AppUiBridgeOperations.DataRead, "{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\"}")]
    [TestCase(AppUiBridgeOperations.DataWrite, "{\"action\":\"set\",\"tree\":\"orders\",\"key\":\"k\",\"value\":\"AA==\"}")]
    [TestCase(AppUiBridgeOperations.DataDelete, "{\"action\":\"delete\",\"tree\":\"orders\",\"key\":\"k\"}")]
    public async Task An_operation_the_install_did_not_consent_to_is_denied(string op, string args)
    {
        var others = AppUiBridgeOperations.All.Where(candidate => candidate != op).ToArray();
        var harness = await CreateAsync(Grants(null, others));
        harness.Host.UserDisplayName = "Ada";

        var outcome = await harness.SendAsync($"{{\"id\":5,\"op\":\"{op}\",\"args\":{args}}}");

        AssertRefused(outcome, 5, "denied");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Toasts!.Toasts, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("Unconsented"));
        });
    }

    [Test]
    public async Task A_data_grant_scoped_to_another_tree_is_denied()
    {
        var harness = await CreateAsync(Grants("notes", AppUiBridgeOperations.DataRead));

        AssertRefused(await harness.SendAsync(1, "data.read", OrdersGet), 1, "denied");
        Assert.That(harness.Bridge!.Calls, Is.Empty);
    }

    [Test]
    public async Task A_data_grant_scoped_to_the_tree_is_relayed()
    {
        var harness = await CreateAsync(Grants("orders", AppUiBridgeOperations.DataRead));

        AssertOk(await harness.SendAsync(1, "data.read", OrdersGet), 1);
        Assert.That(harness.Bridge!.Calls, Has.Count.EqualTo(1));
    }

    [TestCase("t/default/a/taskboard/orders")]
    [TestCase("a/taskboard/orders")]
    [TestCase("_lattice_app_registry")]
    [TestCase("app:taskboard:orders")]
    [TestCase("Orders")]
    [TestCase("")]
    public async Task A_physical_or_malformed_tree_id_is_denied_and_never_forwarded(string tree)
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync(1, "data.read", new { action = "get", tree, key = "k" });

        AssertRefused(outcome, 1, "denied");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("PhysicalTree"));
        });
    }

    [TestCase("treeId")]
    [TestCase("physicalTreeId")]
    [TestCase("tenant")]
    [TestCase("slug")]
    [TestCase("installRevision")]
    public async Task An_argument_the_operation_does_not_define_is_denied(string member)
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync(
            $"{{\"id\":1,\"op\":\"data.read\",\"args\":{{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\",\"{member}\":\"t/x/a/other/secrets\"}}}}");

        AssertRefused(outcome, 1, "denied");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("UnknownArgument"));
        });
    }

    [Test]
    public async Task A_logical_tree_the_app_does_not_declare_is_denied()
    {
        var harness = await CreateAsync();

        AssertRefused(await harness.SendAsync(1, "data.read", new { action = "get", tree = "invoices", key = "k" }), 1, "denied");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("UndeclaredTree"));
        });
    }

    [TestCase("data.read", "{\"action\":\"set\",\"tree\":\"orders\",\"key\":\"k\"}")]
    [TestCase("data.read", "{\"action\":\"get\",\"tree\":\"orders\"}")]
    [TestCase("data.read", "{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\",\"prefix\":\"\"}")]
    [TestCase("data.read", "{\"action\":\"scan\",\"tree\":\"orders\"}")]
    [TestCase("data.read", "{\"action\":\"scan\",\"tree\":\"orders\",\"prefix\":\"\",\"pageSize\":0}")]
    [TestCase("data.read", "{\"action\":\"scan\",\"tree\":\"orders\",\"prefix\":\"\",\"pageSize\":201}")]
    [TestCase("data.read", "{\"action\":\"scan\",\"tree\":\"orders\",\"prefix\":\"\",\"pageSize\":\"10\"}")]
    [TestCase("data.read", "{\"action\":\"get\",\"tree\":\"orders\",\"key\":7}")]
    [TestCase("data.read", "{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\",\"key\":\"j\"}")]
    [TestCase("data.read", "{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"\"}")]
    [TestCase("data.write", "{\"action\":\"set\",\"tree\":\"orders\",\"key\":\"k\"}")]
    [TestCase("data.write", "{\"action\":\"set\",\"tree\":\"orders\",\"key\":\"k\",\"value\":\"***\"}")]
    [TestCase("data.delete", "{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\"}")]
    [TestCase("context.read", "{\"tree\":\"orders\"}")]
    [TestCase("nav.sync", "{}")]
    [TestCase("nav.sync", "{\"path\":\"relative\"}")]
    [TestCase("nav.sync", "{\"path\":\"//evil.example/x\"}")]
    [TestCase("nav.sync", "{\"path\":\"/a\\\\b\"}")]
    [TestCase("nav.sync", "{\"path\":\"/a\\u0000b\"}")]
    [TestCase("ui.notify", "{\"text\":\"\"}")]
    [TestCase("ui.notify", "{\"text\":\"line\\nbreak\"}")]
    [TestCase("ui.notify", "{\"text\":\"\\u202eevil\"}")]
    public async Task Malformed_arguments_are_invalid(string op, string args)
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync($"{{\"id\":3,\"op\":\"{op}\",\"args\":{args}}}");

        AssertRefused(outcome, 3, "invalid");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Toasts!.Toasts, Is.Empty);
        });
    }

    [Test]
    public async Task A_notification_over_the_length_limit_is_invalid()
    {
        var harness = await CreateAsync();
        AssertRefused(await harness.SendAsync(1, "ui.notify", new { text = new string('x', AppFrameProtocol.MaxNotifyLength + 1) }), 1, "invalid");
    }

    [Test]
    public async Task A_path_over_the_length_limit_is_invalid()
    {
        var harness = await CreateAsync();
        AssertRefused(await harness.SendAsync(1, "nav.sync", new { path = "/" + new string('x', AppFrameProtocol.MaxPathLength) }), 1, "invalid");
    }

    [Test]
    public async Task A_key_over_the_length_limit_is_too_large()
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync(1, "data.read", new { action = "get", tree = "orders", key = new string('k', AppFrameProtocol.MaxKeyLength + 1) });

        AssertRefused(outcome, 1, "too_large");
        Assert.That(harness.Bridge!.Calls, Is.Empty);
    }

    [Test]
    public async Task A_value_over_the_size_limit_is_too_large()
    {
        var harness = await CreateAsync();
        var value = Convert.ToBase64String(new byte[AppFrameProtocol.MaxValueBytes + 1]);

        var outcome = await harness.SendAsync(1, "data.write", new { action = "set", tree = "orders", key = "k", value });

        AssertRefused(outcome, 1, "too_large");
        Assert.That(harness.Bridge!.Calls, Is.Empty);
    }

    [Test]
    public async Task A_value_at_the_size_limit_is_relayed()
    {
        var harness = await CreateAsync();
        var bytes = new byte[AppFrameProtocol.MaxValueBytes];
        bytes[^1] = 7;

        AssertOk(await harness.SendAsync(1, "data.write", new { action = "set", tree = "orders", key = "k", value = Convert.ToBase64String(bytes) }), 1);
        Assert.That(harness.Bridge!.LastWrite, Is.EqualTo(bytes));
    }

    [Test]
    public async Task A_continuation_over_the_length_limit_is_too_large()
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync(1, "data.read", new
        {
            action = "scan",
            tree = "orders",
            prefix = string.Empty,
            continuation = new string('c', AppFrameProtocol.MaxContinuationLength + 1),
        });

        AssertRefused(outcome, 1, "too_large");
    }

    [Test]
    public async Task A_null_bridge_collaborator_refuses_every_data_request()
    {
        var harness = await CreateAsync(withBridge: false);

        AssertRefused(await harness.SendAsync(1, "data.read", OrdersGet), 1, "unavailable");
        Assert.That(harness.Log.Messages, Has.Some.Contains("CollaboratorMissing"));
    }

    [Test]
    public async Task A_closed_session_drops_everything()
    {
        var harness = await CreateAsync();
        harness.Session.Close();

        var outcome = await harness.SendAsync(1, "data.read", OrdersGet);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AppBridgeOutcome.Dropped));
            Assert.That(harness.Bridge!.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task A_session_from_another_broker_is_dropped()
    {
        var harness = await CreateAsync();
        var other = await CreateAsync();

        var outcome = await harness.Broker.HandleAsync(other.Session, "{\"id\":1,\"op\":\"context.read\",\"args\":{}}");

        Assert.That(outcome, Is.EqualTo(AppBridgeOutcome.Dropped));
    }

    [Test]
    public async Task Open_refuses_a_launch_from_another_circuit()
    {
        var harness = await CreateAsync();
        var other = await CreateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(() => harness.Broker.Open(other.Session.Launch), Throws.InvalidOperationException);
            Assert.That(() => harness.Broker.Open(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task HandleAsync_rejects_a_null_session()
    {
        var harness = await CreateAsync();
        Assert.That(async () => await harness.Broker.HandleAsync(null!, "{}"), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Denials_are_logged_without_keys_or_values()
    {
        var harness = await CreateAsync(Grants("notes", AppUiBridgeOperations.DataRead));

        await harness.SendAsync(1, "data.read", new { action = "get", tree = "orders", key = "customer-4711-secret" });
        await harness.SendAsync(2, "data.write", new { action = "set", tree = "orders", key = "customer-4711-secret", value = "c2VjcmV0LXZhbHVl" });

        Assert.Multiple(() =>
        {
            Assert.That(harness.Log.Messages, Has.Count.EqualTo(2));
            Assert.That(harness.Log.Messages, Has.None.Contains("customer-4711-secret"));
            Assert.That(harness.Log.Messages, Has.None.Contains("c2VjcmV0LXZhbHVl"));
        });
    }
}
