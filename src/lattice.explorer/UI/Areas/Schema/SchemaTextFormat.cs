namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>The common text formats a format card offers, each compiled to a pattern.</summary>
internal enum SchemaTextFormat
{
    /// <summary>An email address.</summary>
    Email = 0,

    /// <summary>An http or https URL.</summary>
    Url = 1,

    /// <summary>A UUID (GUID) in its hyphenated form.</summary>
    Uuid = 2,

    /// <summary>An ISO 8601 calendar date.</summary>
    Date = 3,

    /// <summary>An ISO 8601 date and time with an offset.</summary>
    DateTime = 4,

    /// <summary>An ISO 8601 time of day.</summary>
    Time = 5,

    /// <summary>An IPv4 address.</summary>
    Ipv4 = 6,

    /// <summary>An IPv6 address.</summary>
    Ipv6 = 7,

    /// <summary>A lower-case, hyphen-separated slug.</summary>
    Slug = 8,

    /// <summary>A two-letter ISO 3166 country code.</summary>
    CountryCode = 9,

    /// <summary>A three-letter ISO 4217 currency code.</summary>
    CurrencyCode = 10,

    /// <summary>A hex colour such as #1a2b3c.</summary>
    HexColour = 11,

    /// <summary>A semantic version such as 2.1.0.</summary>
    SemanticVersion = 12,

    /// <summary>An E.164 phone number such as +441632960961.</summary>
    Phone = 13,
}
