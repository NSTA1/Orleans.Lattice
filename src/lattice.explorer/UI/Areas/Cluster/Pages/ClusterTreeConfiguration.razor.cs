using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's configuration tab: its sizing and runtime overrides, and its
/// durable-history retention, each changeable by a caller with whole-tree admin
/// authority. Both changes are configuration only and are absorbed forward, so
/// neither asks for a typed confirmation.
/// </summary>
public partial class ClusterTreeConfiguration : IDisposable
{
    private const string Inherit = "";
    private const string On = "on";
    private const string Off = "off";

    private static readonly IReadOnlyList<LtSelectOption> SwitchOptions =
    [
        new(Inherit, "Cluster default"),
        new(On, "On"),
        new(Off, "Off"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> ModeOptions =
    [
        new(Inherit, "Default (metadata only)"),
        new(nameof(TreeHistoryRetentionMode.MetadataOnly), "Metadata only"),
        new(nameof(TreeHistoryRetentionMode.FullValue), "Full value"),
        new(nameof(TreeHistoryRetentionMode.Hybrid), "Hybrid"),
    ];

    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<TreeConfigurationReport> _config = ClusterLoad<TreeConfigurationReport>.Loading;
    private ClusterLoad<TreeHistoryRetention> _retention = ClusterLoad<TreeHistoryRetention>.Loading;
    private string? _publishEvents;
    private string? _maintainDigest;
    private string? _walCeiling;
    private string? _walCeilingError;
    private string? _mode;
    private string? _window;
    private string? _windowError;
    private bool _saving;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>What the caller may do to the tree.</summary>
    [Parameter, EditorRequired]
    public LatticeTreeAdminCapabilities Capabilities { get; set; } = default!;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        if (!Capabilities.CanViewDiagnostics && !Capabilities.CanAdministerTree)
        {
            return;
        }

        var admin = Facades.RequireTreeAdmin();
        var config = ClusterLoad<TreeConfigurationReport>.RunAsync(ct => admin.GetTreeConfigAsync(TreeId, ct), _lifetime.Token);
        var retention = ClusterLoad<TreeHistoryRetention>.RunAsync(ct => admin.GetHistoryRetentionAsync(TreeId, ct), _lifetime.Token);
        Show(await config);
        ShowRetention(await retention);
    }

    private void Show(ClusterLoad<TreeConfigurationReport> config)
    {
        _config = config;
        if (config.Value is { } value)
        {
            _publishEvents = SwitchValue(value.PublishEvents);
            _maintainDigest = SwitchValue(value.MaintainProjectionDigest);
            _walCeiling = value.WalMaxRetainedBytes?.ToString(CultureInfo.InvariantCulture);
        }
    }

    private void ShowRetention(ClusterLoad<TreeHistoryRetention> retention)
    {
        _retention = retention;
        if (retention.Value is { } value)
        {
            _mode = value.Mode.ToString();
            _window = value.Window > TimeSpan.Zero ? ((long)value.Window.TotalSeconds).ToString(CultureInfo.InvariantCulture) : null;
        }
    }

    private async Task SaveConfigurationAsync()
    {
        if (_config.Value is not { } current)
        {
            return;
        }

        _walCeilingError = null;
        long? ceiling = null;
        if (!string.IsNullOrWhiteSpace(_walCeiling))
        {
            if (!long.TryParse(_walCeiling.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
            {
                _walCeilingError = "Enter a whole number of bytes greater than zero.";
                return;
            }

            ceiling = parsed;
        }

        var publish = SwitchState(_publishEvents);
        var digest = SwitchState(_maintainDigest);
        var update = new TreeConfigurationUpdate
        {
            ApplyPublishEvents = publish != current.PublishEvents,
            PublishEvents = publish,
            ApplyMaintainProjectionDigest = digest != current.MaintainProjectionDigest && !current.ProjectionDigestPermanentlyDisabled,
            MaintainProjectionDigest = digest,
            ApplyWalMaxRetainedBytes = ceiling != current.WalMaxRetainedBytes,
            WalMaxRetainedBytes = ceiling,
        };

        if (!update.ApplyPublishEvents && !update.ApplyMaintainProjectionDigest && !update.ApplyWalMaxRetainedBytes)
        {
            Toasts.Show("Nothing to change.", LtToastTone.Info);
            return;
        }

        _saving = true;
        var result = await ClusterLoad<TreeConfigurationReport>.RunAsync(
            ct => Facades.RequireTreeAdmin().SetTreeConfigAsync(TreeId, update, ct),
            _lifetime.Token);
        _saving = false;

        if (result.Value is not null)
        {
            Show(result);
            Toasts.Show("Configuration saved.", LtToastTone.Success);
        }
        else
        {
            Toasts.Show(result.Error!, LtToastTone.Danger);
        }
    }

    private async Task SaveRetentionAsync()
    {
        _windowError = null;
        TimeSpan? window = null;
        if (!string.IsNullOrWhiteSpace(_window))
        {
            if (!long.TryParse(_window.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var seconds) || seconds <= 0 || seconds > TimeSpan.MaxValue.TotalSeconds)
            {
                _windowError = "Enter a whole number of seconds greater than zero.";
                return;
            }

            window = TimeSpan.FromSeconds(seconds);
        }

        TreeHistoryRetentionMode? mode = Enum.TryParse<TreeHistoryRetentionMode>(_mode, out var parsedMode) ? parsedMode : null;

        _saving = true;
        var result = await ClusterLoad<TreeHistoryRetention>.RunAsync(
            ct => Facades.RequireTreeAdmin().SetHistoryRetentionAsync(TreeId, mode, window, ct),
            _lifetime.Token);
        _saving = false;

        if (result.Value is not null)
        {
            ShowRetention(result);
            Toasts.Show("History retention saved.", LtToastTone.Success);
        }
        else
        {
            Toasts.Show(result.Error!, LtToastTone.Danger);
        }
    }

    private static string Optional(int? value) => value is { } number ? ClusterFormat.Count(number) : "Library default";

    private static string Switch(bool? value) => value switch
    {
        true => "On",
        false => "Off",
        null => "Cluster default",
    };

    private static string SwitchValue(bool? value) => value switch
    {
        true => On,
        false => Off,
        null => Inherit,
    };

    private static bool? SwitchState(string? value) => value switch
    {
        On => true,
        Off => false,
        _ => null,
    };

    private static string ModeText(TreeHistoryRetentionMode mode) => mode switch
    {
        TreeHistoryRetentionMode.FullValue => "Full value: each revision keeps its bytes",
        TreeHistoryRetentionMode.Hybrid => "Hybrid: recent revisions keep their bytes, older keep metadata",
        _ => "Metadata only: each revision keeps its hash and length",
    };
}
