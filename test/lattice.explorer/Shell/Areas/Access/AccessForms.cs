using AngleSharp.Dom;
using Bunit;
using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>Finds form controls the way a user does: by their visible label.</summary>
internal static class AccessForms
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

    /// <summary>Whether a control labelled <paramref name="label"/> is rendered.</summary>
    public static bool HasField<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : IComponent =>
        cut.FindAll("label").Any(candidate => candidate.TextContent.Trim() == label);

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

    /// <summary>The button whose text is <paramref name="text"/>.</summary>
    public static IElement Button<TComponent>(IRenderedComponent<TComponent> cut, string text)
        where TComponent : IComponent
    {
        var matches = cut.FindAll("button").Where(candidate => candidate.TextContent.Trim() == text).ToArray();
        Assert.That(matches, Has.Length.EqualTo(1), $"expected one button '{text}'");
        return matches[0];
    }
}
