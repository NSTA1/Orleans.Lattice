using Microsoft.JSInterop;
using NSubstitute;
using Orleans.Lattice.Explorer.UI.Areas.Backups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>Artifact download ownership, lazy import recovery and circuit teardown.</summary>
[TestFixture]
public sealed class BackupsInteropTests
{
    [Test]
    public async Task SaveAsync_failed_import_disposes_the_stream_and_the_next_export_retries()
    {
        var js = Substitute.For<IJSRuntime>();
        var module = Substitute.For<IJSObjectReference>();
        var fault = new JSException("import failed");
        var imports = 0;
        js.InvokeAsync<IJSObjectReference>("import", Arg.Any<object?[]>())
            .Returns(_ => ++imports == 1
                ? ValueTask.FromException<IJSObjectReference>(fault)
                : ValueTask.FromResult(module));
        var interop = new BackupsInterop(js);
        using var failed = new MemoryStream([1, 2]);

        Assert.That(async () => await interop.SaveAsync("failed.bin", failed), Throws.Exception.SameAs(fault));
        Assert.That(failed.CanRead, Is.False);
        await interop.DisposeAsync();
        await module.DidNotReceive().DisposeAsync();

        using var first = new MemoryStream([3]);
        using var second = new MemoryStream([4]);
        using var lifetime = new CancellationTokenSource();
        await interop.SaveAsync("first.bin", first, lifetime.Token);
        await interop.SaveAsync("second.bin", second);
        await interop.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(imports, Is.EqualTo(2), "the failed import is retried, the successful one is reused");
            Assert.That(first.CanRead || second.CanRead, Is.False);
            var saves = module.ReceivedCalls().Where(call => call.GetMethodInfo().Name == "InvokeAsync").ToArray();
            Assert.That(saves, Has.Length.EqualTo(2));
            Assert.That(saves.Select(call => call.GetArguments()[0]), Is.All.EqualTo("saveArtifact"));
            Assert.That(saves.Select(call => ((object?[])call.GetArguments()[2]!)[0]),
                Is.EqualTo(new[] { "first.bin", "second.bin" }));
            Assert.That(saves[0].GetArguments()[1], Is.EqualTo(lifetime.Token));
            Assert.That(saves.Select(call => ((DotNetStreamReference)((object?[])call.GetArguments()[2]!)[1]!).Stream),
                Is.EqualTo(new Stream[] { first, second }));
        });
        await module.Received(1).DisposeAsync();
    }

    [Test]
    public async Task DisposeAsync_disconnected_circuit_is_ignored_after_releasing_the_module()
    {
        var module = Substitute.For<IJSObjectReference>();
        module.DisposeAsync().Returns(ValueTask.FromException(new JSDisconnectedException("circuit left")));
        var js = Runtime(module);
        var interop = new BackupsInterop(js);
        using var stream = new MemoryStream([1]);
        await interop.SaveAsync("artifact.bin", stream);

        await interop.DisposeAsync();

        await module.Received(1).DisposeAsync();
        Assert.That(stream.CanRead, Is.False);
    }

    [Test]
    public async Task DisposeAsync_other_module_faults_are_not_hidden()
    {
        var fault = new JSException("module disposal failed");
        var module = Substitute.For<IJSObjectReference>();
        module.DisposeAsync().Returns(ValueTask.FromException(fault));
        var interop = new BackupsInterop(Runtime(module));
        using var stream = new MemoryStream();
        await interop.SaveAsync("artifact.bin", stream);

        Assert.That(async () => await interop.DisposeAsync(), Throws.Exception.SameAs(fault));
    }

    [Test]
    public async Task DisposeAsync_unused_interop_never_imports_a_module()
    {
        var js = Substitute.For<IJSRuntime>();

        await new BackupsInterop(js).DisposeAsync();

        Assert.That(js.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void FileNameFor_preserves_ascii_letters_digits_and_each_allowed_punctuation_character()
    {
        Assert.That(BackupsInterop.FileNameFor("BACK-up_01.2", "part-_.3"),
            Is.EqualTo("BACK-up_01.2-part-_.3.bin"));
    }

    private static IJSRuntime Runtime(IJSObjectReference module)
    {
        var js = Substitute.For<IJSRuntime>();
        js.InvokeAsync<IJSObjectReference>("import", Arg.Any<object?[]>()).Returns(ValueTask.FromResult(module));
        return js;
    }
}
