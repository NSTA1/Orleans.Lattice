namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// A server-authored query template compiled once into literal segments and the
/// slots between them, so rendering a request costs a single string allocation and
/// no scanning, splitting, or intermediate concatenation.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two slots, both server-controlled.</b> A template may carry
/// <see cref="ScopeToken"/>, which the facade replaces with the tenant matcher it
/// derived from the authenticated caller (plus the caller's optional tree filter),
/// and <see cref="WindowToken"/>, which it replaces with the rate window derived
/// from the clamped resolution step. Neither is caller-authored text: the scope is
/// decided server-side and the window is a duration the facade computes, so a
/// template cannot opt out of tenant isolation and a caller cannot smuggle query
/// syntax through either slot.
/// </para>
/// <para>
/// <b>Compiled once, rendered many times.</b> Parsing runs when the catalogue is
/// built (a singleton), so the per-request path walks a small array of already
/// materialised segments. <see cref="Render"/> measures the exact output length,
/// then fills the string in place through <see cref="string.Create{TState}"/>,
/// which yields exactly one allocation - the returned query text, which has to
/// exist because the backend takes it as a string.
/// </para>
/// </remarks>
internal sealed class TelemetryQueryTemplate
{
    /// <summary>
    /// The placeholder a template writes where the facade injects the tenant (and
    /// optional tree) label matchers. Each matcher is emitted with a trailing
    /// comma, so <c>{$scope$}</c> and <c>{$scope$outcome="committed"}</c> are both
    /// well-formed whether or not a scope is pinned.
    /// </summary>
    public const string ScopeToken = "$scope$";

    /// <summary>
    /// The placeholder a template writes where the facade injects the rate window
    /// (for example <c>5m</c>) derived from the clamped resolution step, as in
    /// <c>rate(metric{$scope$}[$window$])</c>.
    /// </summary>
    public const string WindowToken = "$window$";

    private readonly string[] _literals;
    private readonly TelemetryTemplateSlot[] _slots;
    private readonly int _literalLength;

    private TelemetryQueryTemplate(string[] literals, TelemetryTemplateSlot[] slots)
    {
        _literals = literals;
        _slots = slots;

        var total = 0;
        foreach (var literal in literals)
        {
            total += literal.Length;
        }

        _literalLength = total;
        HasScopeSlot = Array.IndexOf(slots, TelemetryTemplateSlot.Scope) >= 0;
    }

    /// <summary><see langword="true"/> when the template carries a <see cref="ScopeToken"/>.</summary>
    public bool HasScopeSlot { get; }

    /// <summary>
    /// Compiles <paramref name="template"/> into its literal segments and slots.
    /// </summary>
    /// <param name="template">The server-authored query template.</param>
    /// <returns>The compiled template.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="template"/> is <see langword="null"/>.</exception>
    public static TelemetryQueryTemplate Parse(string template)
    {
        ArgumentNullException.ThrowIfNull(template);

        var literals = new List<string>();
        var slots = new List<TelemetryTemplateSlot>();
        var cursor = 0;

        while (cursor < template.Length)
        {
            var scopeAt = template.IndexOf(ScopeToken, cursor, StringComparison.Ordinal);
            var windowAt = template.IndexOf(WindowToken, cursor, StringComparison.Ordinal);

            var (at, slot, tokenLength) = NextSlot(scopeAt, windowAt);
            if (at < 0)
            {
                break;
            }

            literals.Add(template[cursor..at]);
            slots.Add(slot);
            cursor = at + tokenLength;
        }

        literals.Add(template[cursor..]);
        return new TelemetryQueryTemplate([.. literals], [.. slots]);
    }

    /// <summary>
    /// Renders the template with <paramref name="scope"/> substituted for every
    /// scope slot and <paramref name="window"/> for every window slot.
    /// </summary>
    /// <param name="scope">The label matchers the facade derived server-side.</param>
    /// <param name="window">The rate window text, for example <c>300s</c>.</param>
    /// <returns>The query text to evaluate.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="window"/> is <see langword="null"/>.</exception>
    public string Render(in TelemetryScopeSelector scope, string window)
    {
        ArgumentNullException.ThrowIfNull(window);

        var scopeLength = scope.Length;
        var length = _literalLength;
        foreach (var slot in _slots)
        {
            length += slot == TelemetryTemplateSlot.Scope ? scopeLength : window.Length;
        }

        // string.Create fills the buffer in place, so the returned query text is
        // the only allocation the render performs. The state is a struct and the
        // callback captures nothing, so neither is allocated per call.
        return string.Create(length, new RenderState(this, scope, window), static (destination, state) =>
        {
            var template = state.Template;
            var written = 0;

            for (var i = 0; i < template._slots.Length; i++)
            {
                var literal = template._literals[i];
                literal.CopyTo(destination[written..]);
                written += literal.Length;

                if (template._slots[i] == TelemetryTemplateSlot.Scope)
                {
                    written += state.Scope.WriteTo(destination[written..]);
                }
                else
                {
                    state.Window.CopyTo(destination[written..]);
                    written += state.Window.Length;
                }
            }

            template._literals[^1].CopyTo(destination[written..]);
        });
    }

    private static (int At, TelemetryTemplateSlot Slot, int Length) NextSlot(int scopeAt, int windowAt)
    {
        if (scopeAt < 0 && windowAt < 0)
        {
            return (-1, TelemetryTemplateSlot.Scope, 0);
        }

        if (windowAt < 0 || (scopeAt >= 0 && scopeAt < windowAt))
        {
            return (scopeAt, TelemetryTemplateSlot.Scope, ScopeToken.Length);
        }

        return (windowAt, TelemetryTemplateSlot.Window, WindowToken.Length);
    }

    private readonly struct RenderState(TelemetryQueryTemplate template, TelemetryScopeSelector scope, string window)
    {
        public TelemetryQueryTemplate Template { get; } = template;

        public TelemetryScopeSelector Scope { get; } = scope;

        public string Window { get; } = window;
    }
}
