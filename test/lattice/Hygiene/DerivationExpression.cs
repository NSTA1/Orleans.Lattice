using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Evaluates the arithmetic in an acceptance-measure register's
/// <c>Ceiling derivation</c> cell against the constants actually declared in
/// <c>Orleans.Lattice</c> (issue #2961).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the derivation is executed rather than read.</b> A register that carried
/// its derivation as prose would be the artefact that already failed: a
/// true-sounding sentence nothing checks, which goes on sounding true while the
/// constants move underneath it. Executing the expression makes a moved constant
/// redden the register on the next build. It also makes the register's central
/// claim - that a threshold is derived from the remedy's own constants and not from
/// the magnitude of the defect - a fact rather than an assurance.
/// </para>
/// <para>
/// <b>Deliberately tiny.</b> Integer literals, <c>Type.Member</c> references, the
/// four arithmetic operators, and parentheses. Nothing else, because every feature
/// added here is a way for a derivation to say something a reader cannot verify by
/// eye, and the point of the cell is that both a person and this evaluator can
/// reach the same number.
/// </para>
/// <para>
/// <b>Absence is an error, never a zero.</b> A reference that does not resolve
/// throws. Defaulting it to zero would let a renamed constant satisfy a stale
/// derivation in silence, which is the same substitution of an absence for a
/// measured value that this whole epic is about.
/// </para>
/// <para>
/// A <see cref="TimeSpan"/> member resolves to its whole minutes, because every
/// rate bound in the WAL GC scheduler is expressed per minute. Non-public statics
/// are bound, so a <see langword="private"/> constant is readable and nothing needs
/// its visibility widened to be cited in a derivation.
/// </para>
/// </remarks>
internal static class DerivationExpression
{
    private const BindingFlags AnyStatic =
        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.FlattenHierarchy;

    private static readonly Regex ReferenceRegex = new(
        @"\b(?<type>[A-Z][A-Za-z0-9_]*)\.(?<member>[A-Za-z_][A-Za-z0-9_]*)\b",
        RegexOptions.Compiled);

    /// <summary>Evaluates a derivation expression.</summary>
    /// <param name="expression">The expression text from the register cell.</param>
    /// <returns>The integer the expression computes.</returns>
    /// <exception cref="InvalidOperationException">
    /// The expression is malformed, or names a member that does not resolve to an
    /// integer or a <see cref="TimeSpan"/>.
    /// </exception>
    internal static int Evaluate(string expression)
    {
        var parser = new Parser(expression ?? throw new InvalidOperationException("A derivation cell was null."));
        var value = parser.ParseSum();
        parser.ExpectEnd();
        return value;
    }

    /// <summary>The <c>Type.Member</c> references an expression names.</summary>
    internal static IReadOnlyList<string> References(string expression) =>
        ReferenceRegex.Matches(expression ?? string.Empty)
            .Select(static m => m.Value)
            .Distinct(StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Resolves a <c>Type.Member</c> reference against the core assembly, or returns
    /// null when no such member exists.
    /// </summary>
    internal static MemberInfo? ResolveMember(string reference)
    {
        var parts = reference.Split('.');
        if (parts.Length != 2)
        {
            return null;
        }

        var type = typeof(LatticeMetrics).Assembly.GetTypes()
            .FirstOrDefault(t => string.Equals(t.Name, parts[0], StringComparison.Ordinal));

        return type?.GetField(parts[1], AnyStatic);
    }

    private static int ResolveValue(string reference)
    {
        if (ResolveMember(reference) is not FieldInfo field)
        {
            throw new InvalidOperationException(
                $"'{reference}' does not name a static field of any type in Orleans.Lattice. A "
                + "derivation that cites a constant which no longer exists is stale, and resolving "
                + "it to a default would hide exactly that.");
        }

        var value = field.IsLiteral ? field.GetRawConstantValue() : field.GetValue(null);

        return value switch
        {
            int i => i,
            long l => checked((int)l),
            TimeSpan span => checked((int)span.TotalMinutes),
            _ => throw new InvalidOperationException(
                $"'{reference}' is a {value?.GetType().Name ?? "null"}, which a derivation cannot use. "
                + "Cite an integer constant or a TimeSpan."),
        };
    }

    /// <summary>
    /// Recursive-descent parser over the expression text. Kept as a private struct
    /// so the evaluator has no state between calls and cannot cache a constant that
    /// later changes.
    /// </summary>
    private sealed class Parser(string text)
    {
        private int position;

        internal int ParseSum()
        {
            var value = ParseProduct();

            while (true)
            {
                SkipWhitespace();
                if (Peek() == '+')
                {
                    position++;
                    value += ParseProduct();
                }
                else if (Peek() == '-')
                {
                    position++;
                    value -= ParseProduct();
                }
                else
                {
                    return value;
                }
            }
        }

        internal void ExpectEnd()
        {
            SkipWhitespace();
            if (position < text.Length)
            {
                throw new InvalidOperationException(
                    $"Unexpected '{text[position]}' at offset {position} in derivation '{text}'.");
            }
        }

        private int ParseProduct()
        {
            var value = ParseAtom();

            while (true)
            {
                SkipWhitespace();
                if (Peek() == '*')
                {
                    position++;
                    value *= ParseAtom();
                }
                else if (Peek() == '/')
                {
                    position++;
                    var divisor = ParseAtom();
                    if (divisor == 0)
                    {
                        throw new InvalidOperationException($"Division by zero in derivation '{text}'.");
                    }

                    value /= divisor;
                }
                else
                {
                    return value;
                }
            }
        }

        private int ParseAtom()
        {
            SkipWhitespace();

            if (Peek() == '(')
            {
                position++;
                var inner = ParseSum();
                SkipWhitespace();
                if (Peek() != ')')
                {
                    throw new InvalidOperationException($"Unclosed parenthesis in derivation '{text}'.");
                }

                position++;
                return inner;
            }

            if (Peek() == '-')
            {
                position++;
                return -ParseAtom();
            }

            var start = position;
            while (position < text.Length && (char.IsLetterOrDigit(text[position]) || text[position] is '.' or '_'))
            {
                position++;
            }

            if (position == start)
            {
                throw new InvalidOperationException(
                    position < text.Length
                        ? $"Unexpected '{text[position]}' at offset {position} in derivation '{text}'."
                        : $"Derivation '{text}' ended while a value was expected.");
            }

            var token = text[start..position];

            return int.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out var literal)
                ? literal
                : ResolveValue(token);
        }

        private char Peek() => position < text.Length ? text[position] : '\0';

        private void SkipWhitespace()
        {
            while (position < text.Length && char.IsWhiteSpace(text[position]))
            {
                position++;
            }
        }
    }
}
