# Astrapy: additional technical info on selected topics



## REPL

Astrapy comes with a REPL, a customized Python interactive shell, which pre-loads
all relevant imports and comes with `client` and `database` objects instantiated and
ready-to-use.

Just ensure you have defined the environment variable as you would need for testing (see below),
or set them as command-line parameters. Start the shell with:

```
astrapy-repl
```

Try `-h` for more guidance on passing options. In a local development environment, you will
have to launch it as `uv run astrapy-repl`.

![AstraPy, REPL screenshot](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_repl.png)



## Maps as association lists

When working with Tables, `map` columns with key of a type other than text
must be expressed as association lists,
i.e. nested lists of lists: `[[key1, value1], [key2, value2], ...]`, in payloads.

By default, `Table` objects in AstraPy will use the association-list representation
only for maps wrapped by a `DataAPIMap`, and leave regular Python `dict` objects
unchanged in payloads: in practice the following row would be sent to the API as shown:

```python
from astrapy.data_types import DataAPIMap

row_to_insert = {
    "map_1": {"k": "v"},
    "map_2": DataAPIMap({"k1": "v1", "k2": "v2"}),
}

# Will be serialized as JSON like this:
#    {
#        "map_1": {"k": "v"},
#        "map_2": [["k1", "v1"], ["k2", "v2"]]
#    }
```

This behaviour is controlled by the API Option `serdes_options.encode_maps_as_lists_in_tables`,
which defaults to `MapEncodingMode.DATAAPIMAPS`. Other options are:

- `MapEncodingMode.ALWAYS` to convert also regular Python `dict` values into lists of pairs;
- `MapEncodingMode.NEVER` to never apply such conversion.

The following code demonstrates getting a `Table` object with the desired setting:

```python
from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import MapEncodingMode

my_options = APIOptions(serdes_options=SerdesOptions(
    encode_maps_as_lists_in_tables=MapEncodingMode.ALWAYS  # or even just "ALWAYS"
))

my_table = my_database.get_table("my_table", spawn_api_options=my_options)
```

See the section about API Options, and the docstring, for more details.

_Caution: if one plans to use regular Python `dict` objects to express user-defined types (UDTs),_
_setting this option to ALWAYS would interfere with the format expected by the API for UDTs._
_See the section on UDTs for more details._



## astrapy abstraction diagram

The abstractions in `astrapy` for working at the data and admin layers are structured
as depicted by this diagram:

![astrapy, abstractions chart](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_abstractions.png)

Here's a small admin-oriented example:

```python
from astrapy import DataAPIClient


# this must have "Database Administrator" permissions:
ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."

my_client = DataAPIClient(ASTRA_DB_APPLICATION_TOKEN)

my_astra_admin = my_client.get_admin()

database_list = list(my_astra_admin.list_databases())

db_info = database_list[0].info
print(db_info.name, db_info.id, db_info.region)

my_database_admin = my_astra_admin.get_database_admin(db_info.id)

my_database_admin.list_keyspaces()
my_database_admin.create_keyspace("my_dreamspace")
```



## Exceptions

The package comes with its own set of exceptions, arranged in this hierarchy:

![AstraPy, exception hierarchy](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_exceptions.png)

For more information, and code examples, consult the docstrings and the API reference linked in the README.



## Dates and times

This diagram complements the information found on the
["DataAPITimestamp and datetimes"](https://docs.datastax.com/en/astra-db-serverless/api-reference/python-client.html#dataapitimestamp-and-datetimes) page.

![AstraPy, datetime and serdes options chart](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_datetime_serdes_options.png)



## Utilities to escape field names

Field names containing special characters (`.` and `&`) must be correctly escaped
in certain Data API commands. It is a responsibility of the user to ensure escaping
is done when needed; however, AstraPy offers utilities to escape sequences of "path
segments" and -- should it ever be needed -- unescape path-strings back into
literal segments:

```python
from astrapy.utils.document_paths import (
    escape_field_names,
    unescape_field_path,
)

print(escape_field_names("f1", "f2", 12, "g.&3"))
# prints: f1.f2.12.g&.&&3

print(escape_field_names(["f1", "f2", 12, "g.&3"]))
# prints: f1.f2.12.g&.&&3

print(unescape_field_path("a&&&.b.c.d.12"))
# prints: ['a&.b', 'c', 'd', '12']
```



## Logging, instrumentation and observability

### Logging

AstraPy's actions are logged using the standard `logging` facilities.
To control the verbosity (logging level), a script preamble can have something
like:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Event observers with context manager

When logging does not suffice, you can use the **Event Observer API**, i.e. attach
an observer to the main astrapy class hierarchy (clients, databases,
tables/collections; plus the admin classes).

Events are, for example, errors and warnings, as well as requests sent
and responses received.

Events are generally issued during the lifecycle of a Data API or DevOps API
HTTP request. Note that events are in correspondence with individual requests,
and not class methods, some of which in fact involve multiple requests (e.g. the
chunked `insert_many` or the database admin's `create_database` with
`wait_until_active=True`).

The simplest usage, when one needs to capture events for a limited
span of statements, is through a context manager:

```python
from astrapy.event_observers import event_collector, ObservableEvent

ev_lst: list[ObservableEvent] = []

with event_collector(db, destination=event_list) as instrumented_db:
    # Use instrumented_db: call its methods, spawn and use collections, etc:
    instrumented_table = instrumented_db.get_table("my_table")
    instrumented_table.insert_one({"id": 123, "name": "Fox"})

for event in event_list:
    print(event.event_type)
```

For more details and options, see the docstring of
the `event_collector` utility.

### Event observers with utility factories

An event observer can be created explicitly and attached through the API Options.

```python
from astrapy.api_options import APIOptions
from astrapy.event_observers import (
    ObservableEvent,
    ObservableEventType,
    Observer,
)

my_event_list: list[ObservableEvent] = []
my_observer = Observer.from_event_list(
    my_event_list,
    # optional filtering by event type:
    event_types=[ObservableEventType.REQUEST, ObservableEventType.RESPONSE],
)

instrumented_client = my_client.with_options(
    api_options=APIOptions(event_observers={"my_obs001": my_observer})
)

# any event of the desired type emitted by 'instrumented_client',
# or by classes it has spawned, will be accumulated into 'my_event_list'.
```

See also method `Observer.from_event_dict` for a similar alternative.

This approach allows attaching multiple observers, if needed.

### Fully customized event observer

For higher control, one can subclass `Observer` directly:

```python
from typing import Any
from astrapy.api_options import APIOptions
from astrapy.event_observers import ObservableEvent, Observer

class MyObserver(Observer):
    def receive(
        self,
        event: ObservableEvent,
        sender: Any = None,
        function_name: str | None = None,
        request_id: str | None = None,
    ) -> None:
        received_item = {
            "event_type": event.event_type.value,
            "sender": sender,
            "function_name": function_name,
            "request_id": request_id,
        }
        print(f"Just received: {received_item}")

my_observer = MyObserver()
instrumented_collection = my_collection.with_options(
    api_options=APIOptions(event_observers={"custom_obs001": my_observer})
)
```
