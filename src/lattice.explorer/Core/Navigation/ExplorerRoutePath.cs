using System.Buffers;

namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Translates between an <see cref="ExplorerRoute"/> and its URL form. The one
/// place the shell's route grammar is written down, so the address bar, a
/// bookmark, a shared link and a deep link all agree.
/// </summary>
/// <remarks>
/// <para>
/// The grammar is <c>/{area}/{kind}/{id}/{surface}</c> with tenant scope and any
/// extra surface state in the query string:
/// </para>
/// <list type="bullet">
/// <item><c>/</c> - the bare address; restore the remembered view.</item>
/// <item><c>/explore</c> - the home area, nothing selected.</item>
/// <item><c>/explore/trees</c> - the home area browsing trees.</item>
/// <item><c>/explore/trees/orders</c> - the <c>orders</c> tree selected.</item>
/// <item><c>/explore/trees/orders/data</c> - open on its data surface.</item>
/// <item><c>/tenants?tenant=acme</c> - a plugin area, scoped to one tenant.</item>
/// </list>
/// <para>
/// <b>Emission is strict, parsing is forgiving.</b> <see cref="Format"/> only
/// ever produces canonical lower-case segments, because the route type refuses
/// to hold anything else. <see cref="Parse"/> accepts an upper-case segment, a
/// trailing slash and a stray query spelling, reports
/// <see cref="ExplorerRouteStatus.Normalized"/>, and lets the shell rewrite the
/// address bar - so a hand-typed link still lands somewhere sensible instead of
/// erroring.
/// </para>
/// <para>
/// <b>The id is escaped, not slugged.</b> A tree id is cluster-owned and may
/// contain a slash (a tenant-owned tree is <c>t/acme/orders</c>) or mixed case,
/// so it round-trips through <see cref="Uri.EscapeDataString"/> rather than
/// being folded to a slug. That is also why the shell parses the address itself
/// rather than leaning on Blazor route-parameter binding, which would split such
/// an id across segments.
/// </para>
/// </remarks>
public static class ExplorerRoutePath
{
    /// <summary>The bare address, <c>/</c>.</summary>
    public const string RootPath = "/";

    private const int MaxPathSegments = 4;

    /// <summary>
    /// Renders <paramref name="route"/> as a root-relative URL. Deterministic:
    /// the same route always produces the same string, which is what lets a
    /// bookmark round-trip and lets the router compare an inbound address against
    /// what it last emitted.
    /// </summary>
    /// <param name="route">The route to render. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="route"/> is <see langword="null"/>.</exception>
    public static string Format(ExplorerRoute route)
    {
        ArgumentNullException.ThrowIfNull(route);

        if (route.IsBare && route.Tenant.Length == 0 && !route.AllTenants && route.Parameters.Count == 0)
        {
            return RootPath;
        }

        // Stack-first so the common route - four short segments and at most a
        // couple of query pairs - is built without touching the heap beyond the
        // single result string.
        var writer = new RouteWriter(stackalloc char[256]);
        try
        {
            if (route.Area.Length != 0)
            {
                writer.AppendSegment(route.Area);

                if (route.Kind.Length != 0)
                {
                    writer.AppendSegment(route.Kind);

                    if (route.Id.Length != 0)
                    {
                        writer.AppendSegment(Uri.EscapeDataString(route.Id));

                        if (route.Surface.Length != 0)
                        {
                            writer.AppendSegment(route.Surface);
                        }
                    }
                }
            }
            else
            {
                writer.Append('/');
            }

            // Shell keys first, then the extension set in its own key order, so
            // the whole query string is a pure function of the route.
            if (route.AllTenants)
            {
                writer.AppendQuery(ExplorerRouteSegments.AllTenantsQueryKey, ExplorerRouteSegments.TrueValue);
            }

            if (route.Tenant.Length != 0)
            {
                writer.AppendQuery(ExplorerRouteSegments.TenantQueryKey, Uri.EscapeDataString(route.Tenant));
            }

            foreach (var parameter in route.Parameters)
            {
                writer.AppendQuery(parameter.Key, Uri.EscapeDataString(parameter.Value));
            }

            return writer.ToString();
        }
        finally
        {
            writer.Dispose();
        }
    }

    /// <summary>
    /// Parses <paramref name="address"/> into a route. Never throws and always
    /// yields a usable route, so an address the shell cannot fully understand
    /// degrades to the nearest safe view instead of wedging it.
    /// </summary>
    /// <param name="address">
    /// A root-relative address such as <c>/explore/trees/orders?tenant=acme</c>,
    /// or the base-relative form Blazor hands back (no leading slash). An
    /// absolute URI is accepted and reduced to its path and query. A fragment is
    /// ignored.
    /// </param>
    public static ExplorerRouteParseResult Parse(string? address)
    {
        var span = address.AsSpan().Trim();

        // An absolute address (what NavigationManager.Uri hands back) is reduced
        // to its path and query, so both callers can pass what they have.
        var schemeEnd = span.IndexOf("://", StringComparison.Ordinal);
        if (schemeEnd >= 0)
        {
            var afterScheme = span[(schemeEnd + 3)..];
            var pathStart = afterScheme.IndexOf('/');
            span = pathStart < 0 ? default : afterScheme[pathStart..];
        }

        var fragment = span.IndexOf('#');
        if (fragment >= 0)
        {
            span = span[..fragment];
        }

        var query = ReadOnlySpan<char>.Empty;
        var pathAndQuery = span;
        var queryStart = span.IndexOf('?');
        if (queryStart >= 0)
        {
            query = span[(queryStart + 1)..];
            span = span[..queryStart];
        }

        var malformed = false;

        // Leading and trailing slashes are noise, not state: '/explore/' and
        // 'explore' address the same view as '/explore'.
        var trimmed = span.Trim('/');

        Span<Range> segments = stackalloc Range[MaxPathSegments + 1];
        var segmentCount = trimmed.IsEmpty
            ? 0
            : trimmed.Split(segments, '/', StringSplitOptions.RemoveEmptyEntries);

        if (segmentCount > MaxPathSegments)
        {
            // More depth than the grammar has. Keep what is addressable and say
            // the address was not fully understood.
            segmentCount = MaxPathSegments;
            malformed = true;
        }

        var area = string.Empty;
        var kind = string.Empty;
        var id = string.Empty;
        var surface = string.Empty;

        for (var i = 0; i < segmentCount; i++)
        {
            var raw = trimmed[segments[i]];
            if (i == 2)
            {
                // The id is a value, not a slug: unescape it and keep its case.
                if (!TryUnescape(raw, out id))
                {
                    malformed = true;
                    id = string.Empty;
                    break;
                }

                continue;
            }

            var slug = ExplorerRouteSlug.Normalize(raw.ToString());
            if (slug.Length == 0)
            {
                malformed = true;
                break;
            }

            switch (i)
            {
                case 0:
                    area = slug;
                    break;
                case 1:
                    kind = slug;
                    break;
                default:
                    surface = slug;
                    break;
            }
        }

        var tenant = string.Empty;
        var allTenants = false;
        ExplorerRouteParameters parameters = ExplorerRouteParameters.Empty;

        if (!query.IsEmpty)
        {
            parameters = ParseQuery(query, ref tenant, ref allTenants, ref malformed);
        }

        var route = ExplorerRoute.FromParts(area, kind, id, surface, tenant, allTenants, parameters);

        // A surface without an id, or an id without a kind, is state the grammar
        // cannot express; FromParts already dropped it, and the address is
        // therefore not reproducible as written.
        if ((surface.Length != 0 && route.Surface.Length == 0) ||
            (id.Length != 0 && route.Id.Length == 0) ||
            (kind.Length != 0 && route.Kind.Length == 0))
        {
            malformed = true;
        }

        if (malformed)
        {
            return new ExplorerRouteParseResult(route, ExplorerRouteStatus.Malformed);
        }

        if (route.IsBare && route.Tenant.Length == 0 && !route.AllTenants && route.Parameters.Count == 0)
        {
            return new ExplorerRouteParseResult(route, ExplorerRouteStatus.Bare);
        }

        // Canonical is defined as "the formatter would reproduce this address
        // exactly". Deriving it from the formatter rather than tracking every
        // tolerated spelling separately means the two can never disagree.
        var status = pathAndQuery.Equals(Format(route), StringComparison.Ordinal)
            ? ExplorerRouteStatus.Canonical
            : ExplorerRouteStatus.Normalized;

        return new ExplorerRouteParseResult(route, status);
    }

    private static ExplorerRouteParameters ParseQuery(
        ReadOnlySpan<char> query,
        ref string tenant,
        ref bool allTenants,
        ref bool malformed)
    {
        List<ExplorerRouteParameter>? extras = null;

        foreach (var pairRange in query.Split('&'))
        {
            var pair = query[pairRange];
            if (pair.IsEmpty)
            {
                continue;
            }

            var separator = pair.IndexOf('=');
            var rawKey = separator < 0 ? pair : pair[..separator];
            var rawValue = separator < 0 ? ReadOnlySpan<char>.Empty : pair[(separator + 1)..];

            var key = ExplorerRouteSlug.Normalize(rawKey.ToString());
            if (key.Length == 0)
            {
                malformed = true;
                continue;
            }

            if (!TryUnescape(rawValue, out var value))
            {
                malformed = true;
                continue;
            }

            if (string.Equals(key, ExplorerRouteSegments.TenantQueryKey, StringComparison.Ordinal))
            {
                tenant = value;
                continue;
            }

            if (string.Equals(key, ExplorerRouteSegments.AllTenantsQueryKey, StringComparison.Ordinal))
            {
                // '1' is tolerated so a hand-written link works, but it is not
                // what the shell emits, so such an address is not canonical.
                allTenants =
                    string.Equals(value, ExplorerRouteSegments.TrueValue, StringComparison.OrdinalIgnoreCase) ||
                    value is "1";
                continue;
            }

            if (value.Length == 0)
            {
                // An empty value carries nothing and the formatter would drop it.
                continue;
            }

            extras ??= [];
            extras.Add(new ExplorerRouteParameter(key, value));
        }

        return extras is null ? ExplorerRouteParameters.Empty : ExplorerRouteParameters.Create(extras);
    }

    private static bool TryUnescape(ReadOnlySpan<char> value, out string result)
    {
        if (value.IsEmpty)
        {
            result = string.Empty;
            return true;
        }

        // Uri.UnescapeDataString does not reject a broken escape - it hands back
        // '%zz' verbatim - so an address carrying one would silently resolve to a
        // different id than the link intended. Validate first and report it, so
        // the shell can say the link was not understood rather than quietly
        // showing the wrong thing.
        for (var i = 0; i < value.Length; i++)
        {
            if (value[i] != '%')
            {
                continue;
            }

            if (i + 2 >= value.Length ||
                !Uri.IsHexDigit(value[i + 1]) ||
                !Uri.IsHexDigit(value[i + 2]))
            {
                result = string.Empty;
                return false;
            }

            i += 2;
        }

        result = Uri.UnescapeDataString(value.ToString());
        return true;
    }

    /// <summary>
    /// A stack-first URL writer. The route grammar is short and bounded, so the
    /// whole address is composed in a stack buffer and materialised once, rather
    /// than concatenating a string per segment on every navigation.
    /// </summary>
    private ref struct RouteWriter(Span<char> initial)
    {
        private Span<char> _buffer = initial;
        private char[]? _rented;
        private int _length = 0;
        private bool _hasQuery = false;

        public void Append(char value)
        {
            Grow(1);
            _buffer[_length++] = value;
        }

        public void AppendSegment(ReadOnlySpan<char> segment)
        {
            Grow(segment.Length + 1);
            _buffer[_length++] = '/';
            segment.CopyTo(_buffer[_length..]);
            _length += segment.Length;
        }

        public void AppendQuery(ReadOnlySpan<char> key, ReadOnlySpan<char> value)
        {
            Grow(key.Length + value.Length + 2);
            _buffer[_length++] = _hasQuery ? '&' : '?';
            _hasQuery = true;
            key.CopyTo(_buffer[_length..]);
            _length += key.Length;
            _buffer[_length++] = '=';
            value.CopyTo(_buffer[_length..]);
            _length += value.Length;
        }

        public override readonly string ToString() => new(_buffer[.._length]);

        public readonly void Dispose()
        {
            if (_rented is not null)
            {
                ArrayPool<char>.Shared.Return(_rented);
            }
        }

        private void Grow(int additional)
        {
            if (_length + additional <= _buffer.Length)
            {
                return;
            }

            var next = ArrayPool<char>.Shared.Rent(Math.Max(_buffer.Length * 2, _length + additional));
            _buffer[.._length].CopyTo(next);

            if (_rented is not null)
            {
                ArrayPool<char>.Shared.Return(_rented);
            }

            _rented = next;
            _buffer = next;
        }
    }
}
