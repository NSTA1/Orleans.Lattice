using System.Collections.Concurrent;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The route templates a page type declares, and whether an address is one of
/// them: the test <see cref="ExplorerPage"/> applies before it accepts the
/// location the layout cascades.
/// </summary>
/// <remarks>
/// <para>
/// Templates follow <see cref="ExplorerPage"/>'s grammar: literal segments and
/// <c>{name}</c> parameters, optional ones marked <c>{name?}</c>. A catch-all
/// (<c>{*name}</c>) and a constraint (<c>{name:int}</c>) are read as a
/// parameter, the catch-all taking every remaining segment, so an unexpected
/// template only ever widens what a page accepts.
/// </para>
/// <para>
/// Each page type is read once and cached; answering allocates nothing.
/// </para>
/// </remarks>
internal sealed class ExplorerPageRoutes
{
    private static readonly ConcurrentDictionary<Type, ExplorerPageRoutes> Cache = new();

    private readonly Template[] _templates;

    private ExplorerPageRoutes(Type pageType)
    {
        var routes = (RouteAttribute[])pageType.GetCustomAttributes(typeof(RouteAttribute), inherit: false);
        _templates = new Template[routes.Length];
        for (var i = 0; i < routes.Length; i++)
        {
            _templates[i] = Template.Parse(routes[i].Template);
        }
    }

    /// <summary>Whether the page type declares any route; a page without one is not routed and answers every address.</summary>
    public bool IsRouted => _templates.Length > 0;

    /// <summary>The routes <paramref name="pageType"/> declares.</summary>
    /// <param name="pageType">The page type.</param>
    public static ExplorerPageRoutes For(Type pageType)
    {
        ArgumentNullException.ThrowIfNull(pageType);
        return Cache.GetOrAdd(pageType, static type => new ExplorerPageRoutes(type));
    }

    /// <summary>
    /// Whether one of the page's routes answers <paramref name="address"/>, its
    /// query aside. A page that declares no route answers every address.
    /// </summary>
    /// <param name="address">The address.</param>
    public bool Answers(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);

        if (_templates.Length == 0)
        {
            return true;
        }

        foreach (var template in _templates)
        {
            if (template.Matches(address))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>One route template: its literals (a null entry is a parameter) and how many segments it requires.</summary>
    private readonly record struct Template(string?[] Segments, int Required, bool CatchAll)
    {
        public static Template Parse(string template)
        {
            var parts = template.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var segments = new string?[parts.Length];
            var required = 0;
            var catchAll = false;
            for (var i = 0; i < parts.Length; i++)
            {
                var part = parts[i];
                if (part.StartsWith('{') && part.EndsWith('}'))
                {
                    segments[i] = null;
                    if (part.StartsWith("{*", StringComparison.Ordinal))
                    {
                        catchAll = true;
                    }
                    else if (!part.EndsWith("?}", StringComparison.Ordinal))
                    {
                        required = i + 1;
                    }
                }
                else
                {
                    segments[i] = part;
                    required = i + 1;
                }
            }

            return new Template(segments, required, catchAll);
        }

        public bool Matches(ExplorerAddress address)
        {
            var count = address.RouteSegmentCount;
            if (count < Required || (!CatchAll && count > Segments.Length))
            {
                return false;
            }

            var compared = Math.Min(count, Segments.Length);
            for (var i = 0; i < compared; i++)
            {
                if (Segments[i] is { } literal
                    && !string.Equals(literal, address.RouteSegmentAt(i), StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
