using AngleSharp.Dom;
using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Tenancy;

/// <summary>Finds form controls the way a user does, by their visible label, and reads the toasts a page posted.</summary>
internal static class TenancyForms
{
    /// <summary>The control labelled <paramref name="label"/>.</summary>
    public static IElement Field<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : IComponent
    {
        var matches = cut.FindAll("label").Where(candidate => candidate.TextContent.Trim() == label).ToArray();
        Assert.That(matches, Has.Length.EqualTo(1), $"expected one control labelled '{label}'");
        var id = matches[0].GetAttribute("for");
        Assert.That(id, Is.Not.Null.And.Not.Empty, $"the label '{label}' names no control");
        return cut.Find("#" + id);
    }

    /// <summary>Types <paramref name="value"/> into the text field labelled <paramref name="label"/>.</summary>
    public static void Type<TComponent>(IRenderedComponent<TComponent> cut, string label, string value)
        where TComponent : IComponent => Field(cut, label).Input(value);

    /// <summary>Chooses <paramref name="value"/> in the select labelled <paramref name="label"/>.</summary>
    public static void Choose<TComponent>(IRenderedComponent<TComponent> cut, string label, string value)
        where TComponent : IComponent => Field(cut, label).Change(value);

    /// <summary>The error sentence shown for the field labelled <paramref name="label"/>, or <see langword="null"/>.</summary>
    public static string? ErrorOf<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : IComponent
    {
        var field = Field(cut, label).ParentElement!;
        return field.QuerySelector(".lt-field__error")?.TextContent.Replace("!", string.Empty, StringComparison.Ordinal).Trim();
    }

    /// <summary>The one button whose text is <paramref name="text"/>.</summary>
    public static IElement Button<TComponent>(IRenderedComponent<TComponent> cut, string text)
        where TComponent : IComponent
    {
        var matches = cut.FindAll("button").Where(candidate => candidate.TextContent.Trim() == text).ToArray();
        Assert.That(matches, Has.Length.EqualTo(1), $"expected one button '{text}'");
        return matches[0];
    }

    /// <summary>Whether any enabled button reads <paramref name="text"/>.</summary>
    public static bool HasButton<TComponent>(IRenderedComponent<TComponent> cut, string text)
        where TComponent : IComponent =>
        cut.FindAll("button").Any(candidate => candidate.TextContent.Trim() == text);

    /// <summary>The toasts the circuit has posted.</summary>
    public static IReadOnlyList<LtToast> GetToasts(this IServiceProvider services) =>
        services.GetRequiredService<LtToastService>().Toasts;
}
