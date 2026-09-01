using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// The browser applier: what it asks the bootstrap script for, and the fact that
/// no way of failing to reach a document is allowed to reach the shell.
/// </summary>
[TestFixture]
public sealed class ExplorerAppearanceApplierTests
{
    [Test]
    public void Construction_rejects_a_missing_runtime()
    {
        Assert.That(() => new ExplorerAppearanceApplier(null!), Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task Applying_calls_the_bootstrap_scripts_own_function()
    {
        var js = new FakeJsRuntime();
        var applier = new ExplorerAppearanceApplier(js);

        await applier.ApplyAsync(new ExplorerAppearanceState(
            ExplorerThemeChoice.Light,
            ExplorerContrastChoice.More,
            ExplorerDensityChoice.Compact));

        Assert.Multiple(() =>
        {
            Assert.That(js.Calls, Has.Count.EqualTo(1));
            Assert.That(js.Calls[0].Identifier, Is.EqualTo(ExplorerAppearanceApplier.ApplyFunction));
            Assert.That(js.Calls[0].Arguments, Is.EqualTo(new object?[] { "light", "more", "compact" }));
        });
    }

    [Test]
    public async Task Following_the_environment_is_passed_as_an_absent_value()
    {
        // The script removes the attribute for a null, which is what hands the
        // answer back to prefers-color-scheme, to prefers-contrast, and to each
        // adaptive root's own breakpoint.
        var js = new FakeJsRuntime();
        var applier = new ExplorerAppearanceApplier(js);

        await applier.ApplyAsync(ExplorerAppearanceState.Default);

        Assert.That(js.Calls[0].Arguments, Is.EqualTo(new object?[] { null, null, null }));
    }

    [Test]
    public async Task Every_axis_is_passed_independently()
    {
        var js = new FakeJsRuntime();
        var applier = new ExplorerAppearanceApplier(js);

        await applier.ApplyAsync(ExplorerAppearanceState.Default with { Contrast = ExplorerContrastChoice.Standard });

        Assert.That(js.Calls[0].Arguments, Is.EqualTo(new object?[] { null, "standard", null }));
    }

    [Test]
    public void An_undeclared_choice_is_rejected_rather_than_written_to_the_document()
    {
        var applier = new ExplorerAppearanceApplier(new FakeJsRuntime());

        Assert.That(
            async () => await applier.ApplyAsync(
                ExplorerAppearanceState.Default with { Theme = (ExplorerThemeChoice)99 }),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [TestCaseSource(nameof(UnreachableDocumentFailures))]
    public void An_unreachable_document_never_faults_the_shell(Exception fault)
    {
        // Interop is unavailable during a prerender pass, during a static render,
        // and after a circuit has gone. In all three the shell must keep working,
        // and in the first two the document already carries what the bootstrap
        // script applied before the application existed.
        var applier = new ExplorerAppearanceApplier(new FakeJsRuntime { Fault = fault });

        Assert.That(async () => await applier.ApplyAsync(ExplorerAppearanceState.Default), Throws.Nothing);
    }

    private static IEnumerable<Exception> UnreachableDocumentFailures()
    {
        yield return new JSDisconnectedException("circuit gone");
        yield return new JSException("script failed");
        yield return new InvalidOperationException("interop outside an interactive circuit");
        yield return new OperationCanceledException("tearing down");
    }
}
