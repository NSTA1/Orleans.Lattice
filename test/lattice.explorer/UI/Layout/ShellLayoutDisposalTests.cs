using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The layout left while the chrome module is still answering its first render (issue
/// #4011): the answer resumes <c>OnAfterRenderAsync</c> after the layout is disposed, and
/// the layout must stop quietly. It used to go on to load the appearance with its disposed
/// cancellation source's token, which threw out of <c>OnAfterRenderAsync</c> and ended the
/// circuit (CI: the apps shard, a fresh document per page).
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutDisposalTests : ShellLayoutTestContext
{
    [Test]
    public async Task A_layout_left_while_the_module_observes_its_width_ends_quietly_and_releases_its_listeners()
    {
        var module = new HeldModule();
        Services.AddScoped(_ => new ShellChromeInterop(new ModuleRuntime(module)));
        var cut = RenderLayout();
        cut.WaitForAssertion(() => Assert.That(module.Observed, Is.True));

        await DisposeComponentsAsync();
        var listener = new Handle();
        await cut.InvokeAsync(() => module.Observing.SetResult(listener));

        Assert.Multiple(() =>
        {
            Assert.That(LeftPage.Fault(Renderer), Is.Null, "the circuit would end");
            Assert.That(listener.Disposed, Is.True, "a listener registered after the layout is gone is released");
        });
    }

    // A runtime whose only answer is the chrome module.
    private sealed class ModuleRuntime(IJSObjectReference module) : IJSRuntime
    {
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) =>
            InvokeAsync<TValue>(identifier, CancellationToken.None, args);

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args) =>
            ValueTask.FromResult(identifier == "import" ? (TValue)module : default!);
    }

    // The chrome module, holding its width observation open until a test releases it.
    private sealed class HeldModule : IJSObjectReference
    {
        public TaskCompletionSource<IJSObjectReference?> Observing { get; } = new();

        public bool Observed { get; private set; }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) =>
            InvokeAsync<TValue>(identifier, CancellationToken.None, args);

        public async ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args)
        {
            if (identifier == "observeViewport")
            {
                Observed = true;
                return (TValue)(object)(await Observing.Task)!;
            }

            return default!;
        }
    }

    private sealed class Handle : IJSObjectReference
    {
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) => ValueTask.FromResult(default(TValue)!);

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args) => ValueTask.FromResult(default(TValue)!);
    }
}
