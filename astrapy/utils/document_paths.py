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

from typing import Iterable

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


def escape_field_name(field_name: str) -> str:
    """
    Escape a single literal field-name path segment.

    Args:
        field_name: a literal field-name path segment, i.e. a single field name
        identifying a key in a JSON-like document. Example: "sub.key&friends"

    Returns:
        a string expressing the input in escaped form, i.e. with the necessary
        escape sequences conforming to the Data API escaping rules.

    Example:
        >>> escape_field_name("sub.key&friends")
        'sub&.key&&friends'
    """

    return "".join(FIELD_NAME_ESCAPE_MAP.get(char, char) for char in field_name)


def escape_field_names(field_names: Iterable[str]) -> list[str]:
    """
    Escape an iterable of field-name path segments one by one.

    Args:
        field_names: an iterable of literal field-name path segments. Each identifies
        a single key in a JSON-like document. Example: ["top", "sub.key"]

    Returns:
        a list with the same length as the input, where each path segment
        has been escaped conforming to the Data API escaping rules.

    Example:
        >>> escape_field_names(["top", "sub.key"])
        ['top', 'sub&.key']
    """

    return [escape_field_name(f_n) for f_n in field_names]


def field_names_to_path(field_names: Iterable[str]) -> str:
    """
    Convert an iterable of field-name path segments into a single string.

    Args:
        field_names: an iterable of literal field-name path segments. Each identifies
        a single key in a JSON-like document. Example: ["top", "sub.key"]

    Returns:
        a single string expressing the path according to the dot-notation convention,
        including any escape that may be necessary.

    Example:
        >>> field_names_to_path(["top", "sub.key"])
        'top.sub&.key'
    """

    return FIELD_NAME_SEGMENT_SEPARATOR.join(escape_field_names(field_names))


def unescape_field_path(field_path: str) -> list[str]:
    """Apply unescaping rules to a single-string field path.

    The result is a list of the individual field names, each a literal
    with nothing escaped anymore.

    Args:
        field_path: an expression denoting a field path following dot-notation
            with escaping for special characters.

    Returns:
        a list of literal field names.

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

    segments = []
    buffer = ""
    path_length = len(field_path)
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
