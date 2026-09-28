using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// The connection settings: the endpoint every Lattice API facade is reached at,
/// its transport posture, a connection test, and save-and-connect.
/// </summary>
/// <remarks>
/// <para>
/// Validation is Core's <see cref="TransportSecurityPolicy.TryValidateEndpoint"/>,
/// applied before a test and before a save, so the dialog can never persist an
/// endpoint the session would refuse. The save itself is
/// <see cref="IExplorerSession.ApplyAsync"/>, which persists, reconnects and
/// raises <see cref="IExplorerSession.ConfigurationChanged"/>.
/// </para>
/// <para>
/// Core's configuration model carries one endpoint, and every facade the host
/// registers - the state API and each administration facade - is served from
/// it, so one address configures them all.
/// </para>
/// <para>
/// On first run it is mandatory: <see cref="AllowCancel"/> is
/// <see langword="false"/>, so it has no Cancel or Close and Escape does not
/// dismiss it.
/// </para>
/// </remarks>
public partial class ConnectionDialog
{
    private readonly EventCallback<string> _endpointChanged;
    private readonly EventCallback<bool> _insecureChanged;
    private readonly EventCallback<bool> _h2cChanged;
    private LtTextInput? _endpointInput;
    private string _endpoint = string.Empty;
    private bool _allowUnencryptedHttp2;
    private bool _insecureLoopbackDev;
    private string? _error;
    private bool _saving;
    private bool _testing;
    private ConnectionTestResult? _testResult;
    private ExplorerConfiguration? _appliedInitial;

    /// <summary>Creates the dialog, binding its field callbacks once rather than per render.</summary>
    public ConnectionDialog()
    {
        _endpointChanged = EventCallback.Factory.Create<string>(this, OnEndpointChanged);
        _insecureChanged = EventCallback.Factory.Create<bool>(this, value => { _insecureLoopbackDev = value; _testResult = null; });
        _h2cChanged = EventCallback.Factory.Create<bool>(this, value => { _allowUnencryptedHttp2 = value; _testResult = null; });
    }

    /// <summary>The configuration to pre-populate the form with, or <see langword="null"/> for an empty form.</summary>
    [Parameter]
    public ExplorerConfiguration? Initial { get; set; }

    /// <summary>Whether the dialog can be dismissed. <see langword="false"/> on the mandatory first-run flow.</summary>
    [Parameter]
    public bool AllowCancel { get; set; }

    /// <summary>Raised after the configuration is saved and applied.</summary>
    [Parameter]
    public EventCallback OnSaved { get; set; }

    /// <summary>Raised when the dialog is dismissed without saving.</summary>
    [Parameter]
    public EventCallback OnCancelled { get; set; }

    [Inject]
    private IExplorerSession Explorer { get; set; } = default!;

    [Inject]
    private IConnectionTester Tester { get; set; } = default!;

    private bool IsBusy => _saving || _testing;

    private ExplorerTransportMode TransportMode =>
        _insecureLoopbackDev ? ExplorerTransportMode.InsecureLoopbackDev : ExplorerTransportMode.Secure;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Initial is not null && !ReferenceEquals(Initial, _appliedInitial))
        {
            _appliedInitial = Initial;
            _endpoint = Initial.Endpoint;
            _allowUnencryptedHttp2 = Initial.AllowUnencryptedHttp2;
            _insecureLoopbackDev = Initial.TransportMode == ExplorerTransportMode.InsecureLoopbackDev;
        }
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (firstRender && _endpointInput is not null)
        {
            await _endpointInput.FocusAsync();
        }
    }

    private void OnEndpointChanged(string value)
    {
        _endpoint = value;
        _error = null;
        _testResult = null;
    }

    private bool TryBuild(out ExplorerConfiguration configuration)
    {
        if (!TransportSecurityPolicy.TryValidateEndpoint(_endpoint, TransportMode, out var error))
        {
            _error = error;
            configuration = null!;
            return false;
        }

        _error = null;
        configuration = new ExplorerConfiguration
        {
            Endpoint = _endpoint.Trim(),
            TransportMode = TransportMode,
            AllowUnencryptedHttp2 = _allowUnencryptedHttp2,
            Headers = Initial?.Headers,
            TransportHeaders = Initial?.TransportHeaders,
        };
        return true;
    }

    private async Task TestAsync()
    {
        if (IsBusy || !TryBuild(out var configuration))
        {
            return;
        }

        _testing = true;
        _testResult = null;
        try
        {
            _testResult = await Tester.TestAsync(configuration);
        }
        catch (Exception ex)
        {
            _testResult = new ConnectionTestResult(ConnectionTestOutcome.Unreachable, ex.Message);
        }
        finally
        {
            _testing = false;
        }
    }

    private async Task SaveAsync()
    {
        if (IsBusy || !TryBuild(out var configuration))
        {
            return;
        }

        _saving = true;
        try
        {
            await Explorer.ApplyAsync(configuration);
            await OnSaved.InvokeAsync();
        }
        catch (Exception ex)
        {
            _error = ex.Message;
        }
        finally
        {
            _saving = false;
        }
    }

    private Task CancelAsync() => OnCancelled.InvokeAsync();

    private Task HandleOpenChangedAsync(bool open) =>
        !open && AllowCancel ? OnCancelled.InvokeAsync() : Task.CompletedTask;
}
