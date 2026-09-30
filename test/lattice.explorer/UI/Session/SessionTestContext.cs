using Microsoft.AspNetCore.Components.Forms;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Session;
using Orleans.Lattice.Explorer.Tests.UI.Design;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The bUnit context the session chrome is tested under: Core's session services
/// replaced by directly driven fakes, the chrome's own per-circuit state real,
/// and a fixed antiforgery token so form-post controls render.
/// </summary>
/// <remarks>
/// A pure component context over fakes - no cluster, host or channel - so the
/// fixtures that use it carry no slow category. Every interaction is dispatched
/// by the test and nothing waits on a timer.
/// </remarks>
public abstract class SessionTestContext : ShellDesignTestContext
{
    private SessionSignInOptions _options = new();

    /// <summary>Registers the fakes and the chrome's own services.</summary>
    protected SessionTestContext()
    {
        StateConnection = new FakeStateConnection();
        Explorer = new FakeExplorerSession(StateConnection);
        Auth = new FakeAuthSession();
        Tester = new FakeConnectionTester();

        Services.AddSingleton<IExplorerSession>(Explorer);
        Services.AddSingleton<IExplorerAuthSession>(Auth);
        Services.AddSingleton<IConnectionTester>(Tester);
        Services.AddSingleton(_ => _options);
        Services.AddScoped<SessionChromeState>();
        Services.AddScoped<ShellHeaderPanels>();
        Services.AddSingleton<AntiforgeryStateProvider, FakeAntiforgeryStateProvider>();
        Services.AddSingleton<IExplorerAuthMethod, BasicExplorerAuthMethod>();
    }

    internal FakeStateConnection StateConnection { get; }

    internal FakeExplorerSession Explorer { get; }

    internal FakeAuthSession Auth { get; }

    internal FakeConnectionTester Tester { get; }

    /// <summary>The circuit's chrome state, resolved as a component would.</summary>
    internal SessionChromeState State => Services.GetRequiredService<SessionChromeState>();

    /// <summary>Replaces the sign-in options before the first render.</summary>
    /// <param name="options">The options the head would register.</param>
    internal void UseOptions(SessionSignInOptions options) => _options = options;

    /// <summary>A secure configuration for a remote endpoint.</summary>
    internal static ExplorerConfiguration RemoteConfiguration(string endpoint = "https://cluster.example:443") =>
        new() { Endpoint = endpoint };
}
