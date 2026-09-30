using Bunit;
using Microsoft.JSInterop;
using NSubstitute;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The chrome's JavaScript seam: it imports the module from the one asset helper,
/// calls it with the stored names, and carries on without script rather than
/// faulting.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellChromeInteropTests : BunitContext
{
    [Test]
    public void The_assets_derive_from_the_one_package_path()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShellChromeAssets.BasePath, Is.EqualTo(Orleans.Lattice.Explorer.UI.Design.ShellDesignAssets.ContentBasePath + "shell/"));
            Assert.That(ShellChromeAssets.Stylesheet, Does.EndWith("shell/lattice-chrome.css"));
            Assert.That(ShellChromeAssets.Module, Does.EndWith("shell/lattice-chrome.js"));
            Assert.That(ShellChromeAssets.FirstPaintScript, Does.EndWith("shell/lattice-appearance.js"));
            Assert.That(ShellChromeAssets.ModuleSpecifier, Is.EqualTo("./" + ShellChromeAssets.Module));
        });
    }

    [Test]
    public async Task The_applier_passes_the_stored_names_to_the_module()
    {
        var module = JSInterop.SetupModule(ShellChromeAssets.ModuleSpecifier);
        module.SetupVoid("applyAppearance", _ => true).SetVoidResult();
        var applier = new JsShellAppearanceApplier(new ShellChromeInterop(JSInterop.JSRuntime));

        await applier.ApplyAsync(ShellTheme.Board, ShellContrast.More, LtDensity.Compact);

        Assert.That(module.Invocations["applyAppearance"].Single().Arguments, Is.EqualTo(new object?[] { "dark", "more", "compact" }));
    }

    [Test]
    public async Task Focus_goes_through_the_module_and_a_refused_focus_is_swallowed()
    {
        var module = JSInterop.SetupModule(ShellChromeAssets.ModuleSpecifier);
        var focus = module.SetupVoid("focusElement", _ => true);
        await using var interop = new ShellChromeInterop(JSInterop.JSRuntime);

        focus.SetVoidResult();
        await interop.FocusAsync(default);
        Assert.That(module.Invocations["focusElement"], Has.Count.EqualTo(1));

        // A focus the browser refuses - its element already gone - costs only the focus.
        focus.SetException(new JSException("Unable to focus an invalid element."));
        Assert.That(async () => await interop.FocusAsync(default), Throws.Nothing);
    }
    [Test]
    public async Task Every_call_is_best_effort_when_the_module_cannot_be_reached()
    {
        var js = Substitute.For<IJSRuntime>();
        js.InvokeAsync<IJSObjectReference>("import", Arg.Any<object?[]>())
            .Returns<ValueTask<IJSObjectReference>>(_ => throw new JSDisconnectedException("gone"));
        await using var interop = new ShellChromeInterop(js);
        using var target = DotNetObjectReference.Create(new object());

        Assert.Multiple(async () =>
        {
            Assert.That(await interop.RegisterShortcutsAsync(target), Is.Null);
            Assert.That(await interop.ObserveViewportAsync(default, target, [1, 2]), Is.Null);
            Assert.That(async () => await interop.ApplyAppearanceAsync("light", "system", "comfortable"), Throws.Nothing);
            Assert.That(async () => await interop.FocusAndSelectAsync(default), Throws.Nothing);
            Assert.That(async () => await interop.FocusAsync(default), Throws.Nothing);
        });
    }

    [Test]
    public async Task A_failed_import_is_retried_on_the_next_call()
    {
        var js = Substitute.For<IJSRuntime>();
        js.InvokeAsync<IJSObjectReference>("import", Arg.Any<object?[]>())
            .Returns(
                _ => ValueTask.FromException<IJSObjectReference>(new InvalidOperationException("prerendering")),
                _ => new ValueTask<IJSObjectReference>(Substitute.For<IJSObjectReference>()));
        await using var interop = new ShellChromeInterop(js);

        await interop.ApplyAppearanceAsync("light", "system", "comfortable");
        await interop.ApplyAppearanceAsync("light", "system", "comfortable");

        await js.Received(2).InvokeAsync<IJSObjectReference>("import", Arg.Any<object?[]>());
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        var interop = new ShellChromeInterop(JSInterop.JSRuntime);

        Assert.Multiple(() =>
        {
            Assert.That(() => new ShellChromeInterop(null!), Throws.ArgumentNullException);
            Assert.That(() => new JsShellAppearanceApplier(null!), Throws.ArgumentNullException);
            Assert.That(async () => await interop.RegisterShortcutsAsync<object>(null!), Throws.ArgumentNullException);
            Assert.That(async () => await interop.ObserveViewportAsync<object>(default, null!, [1]), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_callbacks_forward_to_the_layout()
    {
        var opened = 0;
        var bands = new List<int>();
        var callbacks = new ShellLayoutCallbacks(
            () =>
            {
                opened++;
                return Task.CompletedTask;
            },
            band =>
            {
                bands.Add(band);
                return Task.CompletedTask;
            });

        await callbacks.OpenAddressLine();
        await callbacks.OnViewportBand(1);

        Assert.Multiple(() =>
        {
            Assert.That(opened, Is.EqualTo(1));
            Assert.That(bands, Is.EqualTo(new[] { 1 }));
            Assert.That(() => new ShellLayoutCallbacks(null!, _ => Task.CompletedTask), Throws.ArgumentNullException);
            Assert.That(() => new ShellLayoutCallbacks(() => Task.CompletedTask, null!), Throws.ArgumentNullException);
        });
    }
}
