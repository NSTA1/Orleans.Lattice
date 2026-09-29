using Bunit;
using Microsoft.JSInterop;
using NSubstitute;
using Orleans.Lattice.Explorer.UI.Areas.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// The script-backed visibility observer: it imports the area's module once, reports
/// the document's visibility through its callback, and without script reads as
/// visible so a cadence never silently stops.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class JsReplicationPageVisibilityTests : BunitContext
{
    [Test]
    public async Task It_imports_the_module_once_and_follows_the_callback()
    {
        var module = JSInterop.SetupModule(ReplicationAssets.ModuleSpecifier);
        var observation = module.SetupModule("observeVisibility", _ => true);
        observation.SetupVoid("dispose").SetVoidResult();
        var visibility = new JsReplicationPageVisibility(JSInterop.JSRuntime);
        var changes = 0;
        visibility.Changed += () => changes++;

        await visibility.StartAsync();
        await visibility.StartAsync();
        visibility.OnVisibilityChanged(true);
        visibility.OnVisibilityChanged(false);
        var hidden = visibility.IsVisible;
        visibility.OnVisibilityChanged(true);
        await visibility.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(module.Invocations["observeVisibility"], Has.Count.EqualTo(1));
            Assert.That(module.Invocations["observeVisibility"].Single().Arguments[0], Is.InstanceOf<DotNetObjectReference<JsReplicationPageVisibility>>());
            Assert.That(observation.Invocations["dispose"], Has.Count.EqualTo(1), "disposing stops listening");
            Assert.That(hidden, Is.False);
            Assert.That(visibility.IsVisible, Is.True);
            Assert.That(changes, Is.EqualTo(2), "a report that changes nothing raises nothing");
        });
    }

    [Test]
    public async Task Without_script_the_page_reads_as_visible()
    {
        var js = NSubstitute.Substitute.For<IJSRuntime>();
        js.InvokeAsync<IJSObjectReference>(Arg.Any<string>(), Arg.Any<object?[]?>())
            .Returns<ValueTask<IJSObjectReference>>(_ => throw new JSException("no document"));
        await using var visibility = new JsReplicationPageVisibility(js);

        await visibility.StartAsync();

        Assert.That(visibility.IsVisible, Is.True);
    }

    [Test]
    public void It_rejects_a_null_runtime()
    {
        Assert.That(() => new JsReplicationPageVisibility(null!), Throws.ArgumentNullException);
    }
}
