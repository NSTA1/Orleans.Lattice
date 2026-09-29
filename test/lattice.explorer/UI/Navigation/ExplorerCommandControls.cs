using Bunit;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The shared assertion behind "nothing is reachable only through the palette":
/// every contributed command has a visible, enabled control carrying its
/// <c>data-lt-command</c>. Every area item calls it for each command it
/// contributes, on the page rendered at the command's target.
/// </summary>
internal static class ExplorerCommandControls
{
    /// <summary>
    /// Asserts that <paramref name="rendered"/> holds exactly one enabled control
    /// for <paramref name="command"/> that is not hidden from anyone.
    /// </summary>
    /// <typeparam name="TComponent">The rendered component's type.</typeparam>
    /// <param name="rendered">The rendered page or chrome.</param>
    /// <param name="command">The command.</param>
    public static void AssertVisibleControl<TComponent>(IRenderedComponent<TComponent> rendered, ExplorerCommand command)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        AssertVisibleControl(rendered.FindAll($"[{ExplorerCommand.ControlAttribute}=\"{command.Id}\"]").ToArray(), command);

    /// <summary>Asserts on controls already found, for a component of any type.</summary>
    /// <param name="controls">The elements carrying the command's attribute.</param>
    /// <param name="command">The command.</param>
    public static void AssertVisibleControl(IReadOnlyList<AngleSharp.Dom.IElement> controls, ExplorerCommand command)
    {
        Assert.That(controls, Has.Count.EqualTo(1), $"the command '{command.Id}' ({command.Title}) has no single visible control; nothing may be reachable only through the palette");

        var control = controls[0];
        Assert.Multiple(() =>
        {
            Assert.That(control.LocalName, Is.AnyOf("a", "button", "input", "select"), $"'{command.Id}' must be an interactive control");
            Assert.That(control.HasAttribute("disabled"), Is.False, $"'{command.Id}' must be enabled");
            Assert.That(control.GetAttribute("aria-hidden"), Is.Not.EqualTo("true"), $"'{command.Id}' must not be hidden");
            Assert.That(control.GetAttribute("tabindex"), Is.Not.EqualTo("-1"), $"'{command.Id}' must be reachable by keyboard");
            Assert.That(control.TextContent.Trim().Length + (control.GetAttribute("aria-label")?.Length ?? 0), Is.GreaterThan(0),
                $"'{command.Id}' must have a visible or accessible name");
        });
    }
}
