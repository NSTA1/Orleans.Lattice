namespace Orleans.Lattice.Explorer.UI.Design;

/// <summary>
/// The static web asset paths of the Shell's design system, relative to the
/// application's base path. Every stylesheet a head must load is listed in
/// <see cref="Stylesheets"/>, in cascade order.
/// </summary>
internal static class ShellDesignAssets
{
    /// <summary>The Shell's static web asset base path.</summary>
    public const string ContentBasePath = "_content/Orleans.Lattice.Explorer.UI/";

    /// <summary>The design system's folder within the Shell's static web assets.</summary>
    public const string DesignBasePath = ContentBasePath + "design/";

    /// <summary>The self-hosted font faces (Recursive Sans Linear and Cascadia Mono).</summary>
    public const string FontsStylesheet = DesignBasePath + "lattice-fonts.css";

    /// <summary>The documentation site's own tokens, linked from <c>docs-site/template/public/tokens.css</c>.</summary>
    public const string TokensStylesheet = DesignBasePath + "tokens.css";

    /// <summary>The Explorer-only Operate tokens: density, focus ring, rows, and state roles.</summary>
    public const string OperateStylesheet = DesignBasePath + "lattice-operate.css";

    /// <summary>The one stylesheet allowed to name a layout width.</summary>
    public const string BreakpointsStylesheet = DesignBasePath + "lattice-breakpoints.css";

    /// <summary>The design primitives.</summary>
    public const string PrimitivesStylesheet = DesignBasePath + "lattice-primitives.css";

    /// <summary>
    /// The combobox's small script: it keeps Enter on a highlighted suggestion from
    /// also submitting the surrounding form, and scrolls the highlighted suggestion
    /// into view. <see cref="Components.LtComboBox"/> imports it itself.
    /// </summary>
    public const string ComboBoxModule = DesignBasePath + "lattice-combobox.js";

    /// <summary>The specifier <see cref="ComboBoxModule"/> is imported by, resolved against the document's base.</summary>
    public const string ComboBoxModuleSpecifier = "./" + ComboBoxModule;

    /// <summary>
    /// The tabs' small script: a tab row that scrolls inside its own frame keeps the
    /// selected tab in view, without scrolling the page. <see cref="Components.LtTabs"/>
    /// imports it itself.
    /// </summary>
    public const string TabsModule = DesignBasePath + "lattice-tabs.js";

    /// <summary>The specifier <see cref="TabsModule"/> is imported by, resolved against the document's base.</summary>
    public const string TabsModuleSpecifier = "./" + TabsModule;

    /// <summary>The favicon: the lattice mark on an ink tile, linked from the documentation site.</summary>
    public const string Favicon = DesignBasePath + "favicon.svg";

    /// <summary>The design system's fonts folder, with the fonts' OFL licence files beside them.</summary>
    public const string FontsBasePath = DesignBasePath + "fonts/";

    /// <summary>Every design stylesheet, in the order a head must link them.</summary>
    public static IReadOnlyList<string> Stylesheets { get; } =
    [
        FontsStylesheet,
        TokensStylesheet,
        OperateStylesheet,
        BreakpointsStylesheet,
        PrimitivesStylesheet,
    ];
}
