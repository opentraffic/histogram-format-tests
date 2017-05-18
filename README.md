# Histogram Format Tests

## What is this?

This is a repository for playing around with the structure and format of various options for storing histogram tiles. Each format, and structure of the data within that format, will have different trade-offs in terms of file size and access speed for different query patterns.

There are currently implementations of three different formats:

* [FlatBuffers](https://github.com/google/flatbuffers), a format designed for storing large amounts of data for games, designed to be `mmap`ed into memory and accessed very quickly.
* [Protocol Buffers](https://developers.google.com/protocol-buffers/), a format designed for flexible, upgradeable messaging between systems.
* [ORC](https://orc.apache.org/), a format designed for compact storage and efficient retrieval of data for Hive, a query engine in the Hadoop ecosystem.

## What's the answer?

Like almost every answer, it depends. Here are some observations:

1. ORC doesn't have good reading support in anything except Java. The C++ implementation can't be passed a search term to make use of ORC's built-in indexes. It might be possible manually, but would be painful to implement.
2. Protocol Buffers is not designed for large messages, and will not load anything larger than about 60MiB. This makes a lot of sense, given Protocol Buffers is designed for network messaging. It would be possible to split up the histogram into multiple segments, but at the cost of needing to load each segment separately.

For the example that's coded here, with 10,000 segments and a somewhat realistically random distribution of data, we get the following timings:

| Format           | Per-iteration time (s) | One-off setup time (s) | File size (MiB) |
+------------------+------------------------+------------------------+-----------------+
| FlatBuffers      |                1.85e-6 |                2.7e-3  |            36   |
| Protocol Buffers |               16.92    |                0.349   |            46   |
| ORC              |                0.134   |                0.450   |             5.3 |

These data would suggest that ORC is a great format for compact storage and bulk querying, as long as you want to write everything in Java. FlatBuffers is a great format for accessing data quickly, and compresses down to 16MiB with `gzip -9` or 7.1MiB with `xz -9` if a one-off decompression is an okay price to pay (e.g: for a game, at installation time) - about 0.3s for `gzip` or 0.7s for `xz`.

Protocol Buffers is not a suitable format for this kind of data, unless wrapped in another format to keep the individual message size small. The wide availability of other formats would suggest that developing yet another container format would not be a productive use of effort.

## Structure

At this point, only the one structure has been studied:

* A "flat" structure for ORC, since ORC flattens the structure anyway. A more structured format could be forced by using a `List` for one of the columns.
* A "hybrid" structure for FlatBuffers and Protocol Buffers, which treats the vehicle type and segment ID as "structured" elements, with an unstructured "flat" list of day, hour, next segment ID and bucketed speed data.

## License

All code in this repository is available under the LGPLv3 or later. Please read the [license text](LICENSE.md) for more information.
