# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from typing import Iterable, overload

FIELD_NAME_ESCAPE_CHAR = "&"
FIELD_NAME_SEGMENT_SEPARATOR = "."
FIELD_NAME_ESCAPED_CHARS = {FIELD_NAME_ESCAPE_CHAR, FIELD_NAME_SEGMENT_SEPARATOR}
FIELD_NAME_ESCAPE_MAP = {
    e_char: f"{FIELD_NAME_ESCAPE_CHAR}{e_char}" for e_char in FIELD_NAME_ESCAPED_CHARS
}
ILLEGAL_ESCAPE_ERROR_MESSAGE_TEMPLATE = (
    "Illegal escape sequence found while parsing field path "
    "specification '{field_path}': '{escape_sequence}'"
)
UNTERMINATED_ESCAPE_ERROR_MESSAGE_TEMPLATE = (
    "Unterminated escape sequence found at end of path specification '{field_path}'"
)


def _escape_field_name(field_name: str | int) -> str:
    """
    Escape a single literal field-name path segment.

    Args:
        field_name: a literal field-name path segment, i.e. a single field name
            identifying a key in a JSON-like document. Example: "sub.key&friends".
            Non-negative whole numbers (representing indexes in lists) become strings.

    Returns:
        a string expressing the input in escaped form, i.e. with the necessary
        escape sequences conforming to the Data API escaping rules.

    Example:
        >>> escape_field_name("sub.key&friends")
        'sub&.key&&friends'
    """

    return "".join(FIELD_NAME_ESCAPE_MAP.get(char, char) for char in f"{field_name}")


@overload
def escape_field_names(field_names: Iterable[str | int]) -> str: ...


@overload
def escape_field_names(*field_names: str | int) -> str: ...


# This mypy directive essentially hides the 'funny' call pattern of passing multiple iterators:
def escape_field_names(*field_names: str | int | Iterable[str | int]) -> str:  # type: ignore[misc]
    """
    Escape one or more field-name path segments into a full string expression.

    Args:
        field_names: the function accepts any number of string or integer (non-negative)
            arguments - or, equivalently, a single argument with an iterable of them.
            In either case, the input(s) is a literal, unescaped specification for
            a path in a document.

    Returns:
        a single string resulting from dot-concatenation of the input segments, with
        each path segment having been escaped conforming to the Data API escaping rules.

    Example:
        >>> escape_field_names()
        ''
        >>> escape_field_names("f")
        'f'
        >>> escape_field_names(123)
        '123'
        >>> escape_field_names("f", 123, "tom&jerry")
        'f.123.tom&&jerry'
        >>> escape_field_names(["f"])
        'f'
        >>> escape_field_names(123)
        '123'
        >>> escape_field_names(["f", 123, "tom&jerry"])
        'f.123.tom&&jerry'
    """
    _field_names: Iterable[str | int]
    # strings are iterables, so:
    if len(field_names) == 1 and not isinstance(field_names[0], (str, int)):
        _field_names = field_names[0]
    else:
        # user passing a list of string-or-ints.
        # But secretly any arg may still be Iterable:
        _field_names = [
            segment
            for field_name in field_names
            for segment in (
                [field_name] if isinstance(field_name, (str, int)) else field_name
            )
        ]

    return FIELD_NAME_SEGMENT_SEPARATOR.join(
        [_escape_field_name(f_n) for f_n in _field_names]
    )


def unescape_field_path(field_path: str) -> list[str]:
    """Apply unescaping rules to a single-string field path.

    The result is a list of the individual field names, each a literal
    with nothing escaped anymore.

    Args:
        field_path: an expression denoting a field path following dot-notation
            with escaping for special characters.

    Returns:
        a list of literal field names. Even "number-looking" fields, such as "0"
        or "12", are returned as strings and it is up to the caller to interpret
        these according to the context.

    Example:
        >>> unescape_field_path("a.b")
        ['a', 'b']
        >>>
        >>> unescape_field_path("a&.b")
        ['a.b']
        >>>
        >>> unescape_field_path("a&&b&.c")
        ['a&b.c']
        >>>
        >>> unescape_field_path("a&.b.c&&d")
        ['a.b', 'c&d']
    """

    segments: list[str] = []
    buffer = ""
    path_length = len(field_path)
    if path_length == 0:
        return segments

    char_i = 0
    while char_i < path_length:
        char = field_path[char_i]
        if char == FIELD_NAME_SEGMENT_SEPARATOR:
            segments += [buffer]
            buffer = ""
        elif char == FIELD_NAME_ESCAPE_CHAR:
            if char_i + 1 >= path_length:
                msg = UNTERMINATED_ESCAPE_ERROR_MESSAGE_TEMPLATE.format(
                    field_path=field_path,
                )
                raise ValueError(msg)
            char_i += 1
            char = field_path[char_i]
            if char in FIELD_NAME_ESCAPED_CHARS:
                buffer += char
            else:
                msg = ILLEGAL_ESCAPE_ERROR_MESSAGE_TEMPLATE.format(
                    field_path=field_path,
                    escape_sequence=f"{FIELD_NAME_ESCAPE_CHAR}{char}",
                )
                raise ValueError(msg)
        else:
            buffer += char
        char_i += 1
    segments += [buffer]

    return segments
