using AngleSharp.Dom;
using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Data;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Suggestions;

/// <summary>
/// Helpers for the per-field picker tests: seed the circuit's tree catalogue, type
/// into a labelled combobox, and read what it offers and whether it refused.
/// </summary>
internal static class SuggestionFields
{
    /// <summary>Registers an in-memory state API whose catalogue holds <paramref name="trees"/>.</summary>
    /// <param name="services">The test's services.</param>
    /// <param name="trees">The tree ids the catalogue lists.</param>
    /// <returns>The state API, for further seeding.</returns>
    public static FakeStateClient UseTreeCatalogue(this IServiceCollection services, params string[] trees)
    {
        var client = new FakeStateClient();
        foreach (var tree in trees)
        {
            client.WithTree(tree);
        }

        services.AddSingleton<ILatticeStateClient>(client);
        return client;
    }

    /// <summary>The input of the combobox labelled <paramref name="label"/>.</summary>
    public static IElement Box<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : IComponent
    {
        var labels = cut.FindAll("label").Where(candidate => candidate.TextContent.Trim() == label).ToArray();
        Assert.That(labels, Has.Length.EqualTo(1), $"expected one control labelled '{label}'");
        var input = cut.Find("#" + labels[0].GetAttribute("for"));
        Assert.That(input.GetAttribute("role"), Is.EqualTo("combobox"), $"'{label}' is a type-ahead picker");
        return input;
    }

    /// <summary>Types <paramref name="text"/> into the picker labelled <paramref name="label"/> and waits for what it offers.</summary>
    /// <returns>The offered values, best first.</returns>
    public static IReadOnlyList<string> Offers<TComponent>(IRenderedComponent<TComponent> cut, string label, string text, int atLeast = 1)
        where TComponent : IComponent
    {
        Box(cut, label).Input(text);
        IReadOnlyList<string> values = [];
        cut.WaitUntil(() =>
        {
            values = OfferedValues(cut, label);
            Assert.That(values, Has.Count.GreaterThanOrEqualTo(atLeast), $"'{label}' offers existing values for '{text}'");
        });
        return values;
    }

    /// <summary>The values the picker labelled <paramref name="label"/> offers now.</summary>
    public static IReadOnlyList<string> OfferedValues<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : IComponent
    {
        var input = Box(cut, label);
        var list = input.GetAttribute("aria-controls");
        return [.. cut.FindAll($"#{list} [role=option] .lt-combobox__value").Select(value => value.TextContent)];
    }

    /// <summary>The error the field labelled <paramref name="label"/> shows, or <see langword="null"/>.</summary>
    public static string? ErrorOf<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : IComponent =>
        Box(cut, label).Closest(".lt-field")!.QuerySelector(".lt-field__error")?.TextContent.Replace("!", string.Empty, StringComparison.Ordinal).Trim();

    /// <summary>The flag the field labelled <paramref name="label"/> shows, or <see langword="null"/>.</summary>
    public static string? FlagOf<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : IComponent =>
        Box(cut, label).Closest(".lt-field")!.QuerySelector(".lt-combobox__flag")?.TextContent.Trim();
}
