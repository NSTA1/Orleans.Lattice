using System.Collections.Immutable;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.UI.Framing.Broker;
using static Orleans.Lattice.Explorer.Tests.UI.Framing.AppFrameTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing.Broker;

/// <summary>
/// One broker, one launch, one session, over fakes: the unit under test for every
/// <see cref="AppBridgeBroker"/> branch. Time is a <see cref="ManualTimeProvider"/>.
/// </summary>
internal sealed class BrokerHarness
{
    private BrokerHarness(
        FakeAppWorkspace workspace,
        FakeAppBridge? bridge,
        HostContext host,
        LtToastService? toasts,
        ManualTimeProvider time,
        CapturingLogger log,
        AppFrameBundleLoader loader,
        AppBridgeBroker broker,
        AppBridgeSession session)
    {
        Workspace = workspace;
        Bridge = bridge;
        Host = host;
        Toasts = toasts;
        Time = time;
        Log = log;
        Loader = loader;
        Broker = broker;
        Session = session;
    }

    public FakeAppWorkspace Workspace { get; }

    public FakeAppBridge? Bridge { get; }

    public HostContext Host { get; }

    public LtToastService? Toasts { get; }

    public ManualTimeProvider Time { get; }

    public CapturingLogger Log { get; }

    public AppFrameBundleLoader Loader { get; }

    public AppBridgeBroker Broker { get; }

    public AppBridgeSession Session { get; }

    public static async Task<BrokerHarness> CreateAsync(
        ImmutableArray<AppUiBridgeGrantDescriptor>? grants = null,
        bool withBridge = true,
        bool withToasts = true,
        bool withHostContext = true,
        ILatticeActiveTenantProvider? tenant = null)
    {
        var workspace = Workspace(Describe(Ui(grants)));
        var bridge = withBridge ? new FakeAppBridge() : null;
        var host = new HostContext();
        var toasts = withToasts ? new LtToastService() : null;
        var time = new ManualTimeProvider();
        var log = new CapturingLogger();
        var loader = new AppFrameBundleLoader(workspace, new AppFrameBundleCache(), NullLogger<AppFrameBundleLoader>.Instance, tenant);
        var broker = new AppBridgeBroker(bridge, loader, withHostContext ? host : null, toasts, time, log, tenant);
        var launch = (await loader.AuthorizeAsync(Slug)).Launch!;
        return new BrokerHarness(workspace, bridge, host, toasts, time, log, loader, broker, broker.Open(launch));
    }

    /// <summary>Only the named operations are granted; data operations over every tree unless <paramref name="tree"/> is given.</summary>
    public static ImmutableArray<AppUiBridgeGrantDescriptor> Grants(string? tree, params string[] operations) =>
        operations.Select(op => new AppUiBridgeGrantDescriptor { Operation = op, Tree = tree }).ToImmutableArray();

    public Task<AppBridgeOutcome> SendAsync(string message) => Broker.HandleAsync(Session, message);

    public Task<AppBridgeOutcome> SendAsync(long id, string op, object? args = null) =>
        SendAsync(JsonSerializer.Serialize(new { id, op, args = args ?? new { } }));

    /// <summary>Parses a reply, asserting it is a response to <paramref name="id"/>.</summary>
    public static JsonElement Reply(AppBridgeOutcome outcome, long id)
    {
        Assert.That(outcome.Reply, Is.Not.Null, "the broker should have answered");
        var root = JsonDocument.Parse(outcome.Reply!).RootElement;
        Assert.That(root.GetProperty("id").GetInt64(), Is.EqualTo(id));
        return root;
    }

    /// <summary>Asserts a refusal with <paramref name="code"/> and a fixed message.</summary>
    public static void AssertRefused(AppBridgeOutcome outcome, long id, string code)
    {
        var root = Reply(outcome, id);
        Assert.Multiple(() =>
        {
            Assert.That(root.GetProperty("ok").GetBoolean(), Is.False);
            Assert.That(root.GetProperty("error").GetProperty("code").GetString(), Is.EqualTo(code));
            Assert.That(root.GetProperty("error").GetProperty("message").GetString(), Is.EqualTo(AppBridgeBroker.MessageFor(code)));
            Assert.That(root.TryGetProperty("result", out _), Is.False);
            Assert.That(outcome.Effect, Is.EqualTo(AppBridgeEffect.None));
        });
    }

    /// <summary>Asserts success and returns the result.</summary>
    public static JsonElement AssertOk(AppBridgeOutcome outcome, long id)
    {
        var root = Reply(outcome, id);
        Assert.That(root.GetProperty("ok").GetBoolean(), Is.True, outcome.Reply);
        return root.GetProperty("result");
    }

    internal sealed class HostContext : IAppFrameHostContext
    {
        public AppFrameAppearance Appearance { get; set; } = AppFrameAppearance.Default;

        public string? TenantDisplayName { get; set; }

        public string? UserDisplayName { get; set; }
    }

    internal sealed class CapturingLogger : ILogger<AppBridgeBroker>
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
    }
}
