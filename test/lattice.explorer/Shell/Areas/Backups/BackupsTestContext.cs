using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.Shell.Areas.Backups;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Backups;

/// <summary>
/// The bUnit context the Backups area is tested under: the Shell registered as
/// a head registers it, with the backup facade and both apps facades replaced by
/// fakes (so no test ever dials a cluster), and the Backups area as the only
/// area, so sibling areas' probes cannot reach a real transport.
/// </summary>
public abstract class BackupsTestContext : ShellChromeTestContext
{
    /// <summary>Replaces every facade the area can reach with a fake.</summary>
    protected BackupsTestContext()
    {
        Backups = new FakeBackupControl();
        AppsControl = Substitute.For<ILatticeAppsControl>();
        AppsControl.DescribeAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<AppDescriptor?>(null));
        Workspace = Substitute.For<ILatticeAppWorkspace>();
        Workspace.DescribeMyAppAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<WorkspaceAppDescriptor?>(null));

        Services.RemoveAll<ILatticeBackupControl>();
        Services.RemoveAll<ILatticeAppsControl>();
        Services.RemoveAll<ILatticeAppWorkspace>();
        Services.AddSingleton<ILatticeBackupControl>(Backups);
        Services.AddSingleton(AppsControl);
        Services.AddSingleton(Workspace);

        Services.RemoveAll<IExplorerArea>();
        Services.AddExplorerArea<BackupsArea>();
    }

    /// <summary>The scripted backup facade.</summary>
    internal FakeBackupControl Backups { get; }

    /// <summary>The apps control facade, answering "no such app" unless a test scripts it.</summary>
    internal ILatticeAppsControl AppsControl { get; }

    /// <summary>The caller's app workspace, answering "no such app" unless a test scripts it.</summary>
    internal ILatticeAppWorkspace Workspace { get; }

    /// <summary>The circuit's staged operations.</summary>
    internal BackupOperations Operations => Services.GetRequiredService<BackupOperations>();

    /// <summary>The circuit's toasts.</summary>
    internal LtToastService Toasts => Services.GetRequiredService<LtToastService>();

    /// <summary>The browser's address, relative to the base, with a leading slash.</summary>
    internal string CurrentPath => "/" + Navigation.ToBaseRelativePath(Navigation.Uri);

    /// <summary>Navigates to <paramref name="relative"/> and renders <typeparamref name="TPage"/> there.</summary>
    /// <typeparam name="TPage">The page.</typeparam>
    /// <param name="relative">The base-relative address, such as <c>backups/abc</c>.</param>
    /// <param name="band">The measured width band, or <see langword="null"/> for none.</param>
    internal IRenderedComponent<TPage> RenderAt<TPage>(string relative, LtBreakpoint? band = null)
        where TPage : IComponent
    {
        Navigation.NavigateTo(relative);
        return Render<TPage>(parameters =>
        {
            if (band is { } value)
            {
                parameters.AddCascadingValue(LtBreakpointCascade.Name, value);
            }
        });
    }

    /// <summary>Adds <paramref name="manifests"/> to the catalogue.</summary>
    /// <param name="manifests">The manifests.</param>
    internal void Seed(params Orleans.Lattice.Backup.BackupManifest[] manifests) => Backups.Catalogue.AddRange(manifests);
}
