using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Orleans.Lattice.Explorer.Shell.Navigation.Address;

/// <summary>
/// One Explorer address: where an object lives, in the grammar the address line
/// shows and accepts, and the URL the browser carries (epic decision E11).
/// </summary>
/// <remarks>
/// <para>
/// The canonical grammar (the ABNF is in the package README) is
/// <c>[/t/{tenant}]/{area}[/{segment}...][?{key}={value}&amp;...]</c>, with the
/// bare <c>/</c> (or <c>/t/{tenant}</c>) as Home. For the Data area the segments
/// are the logical tree id's parts, so <c>a/crm/orders</c> is addressed as
/// <c>/data/a/crm/orders</c> (<see cref="ForTree"/>).
/// </para>
/// <para>
/// An address is immutable and compares by value. <see cref="Format"/> is the
/// canonical text, and <see cref="Parse"/> of it returns an equal address;
/// <see cref="ToHref"/> is the same text relative to the application's base path,
/// which is what every link and navigation must use so the Explorer works when it
/// is mounted under a base path.
/// </para>
/// </remarks>
internal sealed class ExplorerAddress : IEquatable<ExplorerAddress>
{
    /// <summary>The literal segment that introduces a tenant root.</summary>
    public const string TenantSegment = "t";

    /// <summary>The query key naming one key within the object.</summary>
    public const string KeyQuery = "key";

    /// <summary>The query key naming a key prefix within the object.</summary>
    public const string PrefixQuery = "prefix";

    /// <summary>The query key naming a point in time or a revision.</summary>
    public const string AtQuery = "at";

    private ExplorerAddress(
        string? tenant,
        string? area,
        ImmutableArray<string> path,
        ImmutableArray<KeyValuePair<string, string>> query)
    {
        Tenant = tenant;
        Area = area;
        PathSegments = path;
        QueryParameters = query;
    }

    /// <summary>The Explorer's Home: the estate overview, with no tenant root.</summary>
    public static ExplorerAddress Home { get; } = new(null, null, [], []);

    /// <summary>The tenant this address is rooted at, or <see langword="null"/> for no tenant node.</summary>
    public string? Tenant { get; }

    /// <summary>The area key, or <see langword="null"/> for Home.</summary>
    public string? Area { get; }

    /// <summary>The decoded path segments below the area, outermost first.</summary>
    public IReadOnlyList<string> Path => PathSegments;

    /// <summary>The decoded query parameters, in order.</summary>
    public IReadOnlyList<KeyValuePair<string, string>> Query => QueryParameters;

    /// <summary>Whether this is Home (with or without a tenant root).</summary>
    public bool IsHome => Area is null;

    /// <summary>
    /// The path segments joined with <c>/</c> - for the Data area, the logical
    /// tree id - or <see langword="null"/> when there is no path.
    /// </summary>
    public string? TreeId => PathSegments.IsEmpty ? null : string.Join('/', PathSegments);

    /// <summary>
    /// The nearest ancestor: without the query, then without the last segment,
    /// then the tenant's Home, then Home. <see langword="null"/> for Home itself.
    /// </summary>
    public ExplorerAddress? Parent
    {
        get
        {
            if (!QueryParameters.IsEmpty)
            {
                return new ExplorerAddress(Tenant, Area, PathSegments, []);
            }

            if (!PathSegments.IsEmpty)
            {
                return new ExplorerAddress(Tenant, Area, PathSegments.RemoveAt(PathSegments.Length - 1), []);
            }

            if (Area is not null)
            {
                return new ExplorerAddress(Tenant, null, [], []);
            }

            return Tenant is null ? null : Home;
        }
    }

    private ImmutableArray<string> PathSegments { get; }

    private ImmutableArray<KeyValuePair<string, string>> QueryParameters { get; }

    /// <summary>Creates an address from its decoded parts, validating each one.</summary>
    /// <param name="tenant">The tenant root, or <see langword="null"/>.</param>
    /// <param name="area">The area key, or <see langword="null"/> for Home.</param>
    /// <param name="path">The decoded path segments; only allowed with an area.</param>
    /// <param name="query">The decoded query parameters.</param>
    /// <exception cref="ArgumentException">A part is not valid in the grammar.</exception>
    public static ExplorerAddress Create(
        string? tenant,
        string? area,
        IEnumerable<string>? path = null,
        IEnumerable<KeyValuePair<string, string>>? query = null)
    {
        if (tenant is not null)
        {
            EnsureSegment(tenant, nameof(tenant));
        }

        if (area is not null && (!ExplorerAddressEncoding.IsKeyword(area) || area == TenantSegment))
        {
            throw new ArgumentException(
                $"'{area}' is not an area key: it must be a lower-case letter followed by lower-case letters, digits and hyphens, and must not be '{TenantSegment}'.",
                nameof(area));
        }

        var segments = path is null ? [] : path.ToImmutableArray();
        foreach (var segment in segments)
        {
            EnsureSegment(segment, nameof(path));
        }

        if (area is null && !segments.IsEmpty)
        {
            throw new ArgumentException("Home has no path segments.", nameof(path));
        }

        var parameters = query is null ? [] : query.ToImmutableArray();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (key, value) in parameters)
        {
            if (!ExplorerAddressEncoding.IsKeyword(key))
            {
                throw new ArgumentException($"'{key}' is not a query key.", nameof(query));
            }

            if (!seen.Add(key))
            {
                throw new ArgumentException($"The query key '{key}' appears more than once.", nameof(query));
            }

            ArgumentNullException.ThrowIfNull(value, nameof(query));
            if (!ExplorerAddressEncoding.IsWellFormed(value))
            {
                throw new ArgumentException($"The value of '{key}' is not well-formed text.", nameof(query));
            }
        }

        return new ExplorerAddress(tenant, area, segments, parameters);
    }

    /// <summary>The address of an area, optionally with path segments below it.</summary>
    /// <param name="area">The area key.</param>
    /// <param name="path">The decoded path segments.</param>
    /// <exception cref="ArgumentException">A part is not valid in the grammar.</exception>
    public static ExplorerAddress ForArea(string area, params string[] path)
    {
        ArgumentNullException.ThrowIfNull(area);
        return Create(null, area, path);
    }

    /// <summary>
    /// The address of a tree in an area: the logical tree id's <c>/</c>-separated
    /// parts become the path segments, so <c>a/crm/orders</c> is
    /// <c>/{area}/a/crm/orders</c>.
    /// </summary>
    /// <param name="area">The area key.</param>
    /// <param name="treeId">The logical tree id. Must not be empty or contain an empty part.</param>
    /// <exception cref="ArgumentException">A part is not valid in the grammar.</exception>
    public static ExplorerAddress ForTree(string area, string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Create(null, area, treeId.Split('/'));
    }

    /// <summary>Parses an address, throwing when it is not valid.</summary>
    /// <param name="text">A base-relative path and query, with or without a leading <c>/</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="text"/> is <see langword="null"/>.</exception>
    /// <exception cref="FormatException"><paramref name="text"/> is not an address.</exception>
    public static ExplorerAddress Parse(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        return TryParse(text, out var address)
            ? address
            : throw new FormatException($"'{text}' is not an Explorer address.");
    }

    /// <summary>
    /// Parses an address. Leniently, an upper-case area key is taken as its lower
    /// case, a leading <c>./</c> (the base-relative form of <see cref="ToHref"/>)
    /// and a trailing <c>/</c> are ignored, a fragment is dropped, and a raw
    /// character that the canonical form would percent-encode is taken as itself.
    /// </summary>
    /// <param name="text">A base-relative path and query, with or without a leading <c>/</c>.</param>
    /// <param name="address">The parsed address.</param>
    /// <returns><see langword="true"/> when <paramref name="text"/> is an address.</returns>
    public static bool TryParse(string? text, [NotNullWhen(true)] out ExplorerAddress? address)
    {
        address = null;
        if (text is null)
        {
            return false;
        }

        var span = text.AsSpan();
        var fragment = span.IndexOf('#');
        if (fragment >= 0)
        {
            span = span[..fragment];
        }

        var querySpan = ReadOnlySpan<char>.Empty;
        var question = span.IndexOf('?');
        if (question >= 0)
        {
            querySpan = span[(question + 1)..];
            span = span[..question];
        }

        if (span.StartsWith("./", StringComparison.Ordinal))
        {
            span = span[2..];
        }
        else if (span.StartsWith("/", StringComparison.Ordinal))
        {
            span = span[1..];
        }

        if (span.EndsWith("/", StringComparison.Ordinal))
        {
            span = span[..^1];
        }

        var segments = new List<string>();
        if (!span.IsEmpty)
        {
            foreach (var range in span.Split('/'))
            {
                var raw = span[range];
                if (raw.IsEmpty || !ExplorerAddressEncoding.TryDecode(raw, out var decoded))
                {
                    return false;
                }

                segments.Add(decoded!);
            }
        }

        string? tenant = null;
        var index = 0;
        if (segments.Count > 0 && string.Equals(segments[0], TenantSegment, StringComparison.OrdinalIgnoreCase))
        {
            if (segments.Count < 2)
            {
                return false;
            }

            tenant = segments[1];
            index = 2;
        }

        string? area = null;
        if (index < segments.Count)
        {
            area = segments[index].ToLowerInvariant();
            if (!ExplorerAddressEncoding.IsKeyword(area) || area == TenantSegment)
            {
                return false;
            }

            index++;
        }

        if (!TryParseQuery(querySpan, out var query))
        {
            return false;
        }

        address = new ExplorerAddress(tenant, area, [.. segments.Skip(index)], query);
        return true;
    }

    /// <summary>
    /// Parses the address of an absolute URI under the application's base URI,
    /// as <c>NavigationManager</c> reports them.
    /// </summary>
    /// <param name="uri">The absolute URI.</param>
    /// <param name="baseUri">The application's base URI, ending in <c>/</c>.</param>
    /// <param name="address">The parsed address.</param>
    /// <returns><see langword="true"/> when <paramref name="uri"/> is under the base and is an address.</returns>
    public static bool TryFromUri(string uri, string baseUri, [NotNullWhen(true)] out ExplorerAddress? address)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(baseUri);

        address = null;
        if (uri.StartsWith(baseUri, StringComparison.OrdinalIgnoreCase))
        {
            return TryParse(uri[baseUri.Length..], out address);
        }

        // The base URI without its trailing slash is the application root too.
        return baseUri.Length > 0
            && string.Equals(uri, baseUri[..^1], StringComparison.OrdinalIgnoreCase)
            && TryParse(string.Empty, out address);
    }

    /// <summary>The value of a query parameter, or <see langword="null"/> when absent.</summary>
    /// <param name="key">The query key.</param>
    public string? GetQuery(string key)
    {
        foreach (var (candidate, value) in QueryParameters)
        {
            if (string.Equals(candidate, key, StringComparison.Ordinal))
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>This address rooted at <paramref name="tenant"/>, or with no tenant root when <see langword="null"/>.</summary>
    /// <param name="tenant">The tenant id.</param>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is not a valid segment.</exception>
    public ExplorerAddress WithTenant(string? tenant)
    {
        if (string.Equals(tenant, Tenant, StringComparison.Ordinal))
        {
            return this;
        }

        if (tenant is not null)
        {
            EnsureSegment(tenant, nameof(tenant));
        }

        return new ExplorerAddress(tenant, Area, PathSegments, QueryParameters);
    }

    /// <summary>This address with <paramref name="path"/> in place of its path and no query.</summary>
    /// <param name="path">The decoded path segments.</param>
    /// <exception cref="ArgumentException">A segment is not valid, or this is Home.</exception>
    public ExplorerAddress WithPath(params string[] path) => Create(Tenant, Area, path);

    /// <summary>This address with the query parameter set, replaced, or (for a <see langword="null"/> value) removed.</summary>
    /// <param name="key">The query key.</param>
    /// <param name="value">The decoded value, or <see langword="null"/> to remove the parameter.</param>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not a query key.</exception>
    public ExplorerAddress WithQuery(string key, string? value)
    {
        if (!ExplorerAddressEncoding.IsKeyword(key))
        {
            throw new ArgumentException($"'{key}' is not a query key.", nameof(key));
        }

        var existing = -1;
        for (var i = 0; i < QueryParameters.Length; i++)
        {
            if (string.Equals(QueryParameters[i].Key, key, StringComparison.Ordinal))
            {
                existing = i;
                break;
            }
        }

        if (value is null)
        {
            return existing < 0 ? this : new ExplorerAddress(Tenant, Area, PathSegments, QueryParameters.RemoveAt(existing));
        }

        if (!ExplorerAddressEncoding.IsWellFormed(value))
        {
            throw new ArgumentException($"The value of '{key}' is not well-formed text.", nameof(value));
        }

        var parameter = new KeyValuePair<string, string>(key, value);
        var query = existing < 0 ? QueryParameters.Add(parameter) : QueryParameters.SetItem(existing, parameter);
        return new ExplorerAddress(Tenant, Area, PathSegments, query);
    }

    /// <summary>The canonical text of this address, starting with <c>/</c>.</summary>
    public string Format()
    {
        var builder = new StringBuilder(64);

        if (Tenant is not null)
        {
            builder.Append('/').Append(TenantSegment).Append('/').Append(ExplorerAddressEncoding.EncodeSegment(Tenant));
        }

        if (Area is not null)
        {
            builder.Append('/').Append(Area);
            foreach (var segment in PathSegments)
            {
                builder.Append('/').Append(ExplorerAddressEncoding.EncodeSegment(segment));
            }
        }

        if (builder.Length == 0)
        {
            builder.Append('/');
        }

        for (var i = 0; i < QueryParameters.Length; i++)
        {
            var (key, value) = QueryParameters[i];
            builder.Append(i == 0 ? '?' : '&').Append(key).Append('=').Append(ExplorerAddressEncoding.EncodeQueryValue(value));
        }

        return builder.ToString();
    }

    /// <summary>
    /// The canonical text relative to the application's base path - no leading
    /// <c>/</c>, and <c>./</c> for Home - for an <c>href</c> or a navigation.
    /// </summary>
    public string ToHref()
    {
        var formatted = Format();
        return formatted.Length == 1 || formatted[1] == '?'
            ? "./" + formatted[1..]
            : formatted[1..];
    }

    /// <inheritdoc />
    public bool Equals(ExplorerAddress? other) =>
        other is not null
        && string.Equals(Tenant, other.Tenant, StringComparison.Ordinal)
        && string.Equals(Area, other.Area, StringComparison.Ordinal)
        && PathSegments.SequenceEqual(other.PathSegments, StringComparer.Ordinal)
        && QueryParameters.SequenceEqual(other.QueryParameters);

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as ExplorerAddress);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Tenant, StringComparer.Ordinal);
        hash.Add(Area, StringComparer.Ordinal);
        foreach (var segment in PathSegments)
        {
            hash.Add(segment, StringComparer.Ordinal);
        }

        foreach (var parameter in QueryParameters)
        {
            hash.Add(parameter);
        }

        return hash.ToHashCode();
    }

    /// <inheritdoc />
    public override string ToString() => Format();

    private static bool TryParseQuery(ReadOnlySpan<char> text, out ImmutableArray<KeyValuePair<string, string>> query)
    {
        query = [];
        if (text.IsEmpty)
        {
            return true;
        }

        var builder = ImmutableArray.CreateBuilder<KeyValuePair<string, string>>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var range in text.Split('&'))
        {
            var parameter = text[range];
            if (parameter.IsEmpty)
            {
                continue;
            }

            var equals = parameter.IndexOf('=');
            if (equals <= 0
                || !ExplorerAddressEncoding.TryDecode(parameter[..equals], out var key)
                || !ExplorerAddressEncoding.TryDecode(parameter[(equals + 1)..], out var value))
            {
                return false;
            }

            key = key!.ToLowerInvariant();
            if (!ExplorerAddressEncoding.IsKeyword(key) || !seen.Add(key))
            {
                return false;
            }

            builder.Add(new KeyValuePair<string, string>(key, value!));
        }

        query = builder.ToImmutable();
        return true;
    }

    private static void EnsureSegment(string segment, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(segment, parameterName);
        if (segment.Length == 0 || !ExplorerAddressEncoding.IsWellFormed(segment))
        {
            throw new ArgumentException("A segment must be non-empty, well-formed text.", parameterName);
        }
    }
}
