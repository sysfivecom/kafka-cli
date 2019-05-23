# kafka-cli

Basic kafka-cli to read topics e.g. it could be used to read the schemas from kafka.

## Usage

        usage: kafka-cli --topic=TOPIC [<flags>]

          Flags:
                --help               Show context-sensitive help (also try --help-long and --help-man).
            -o, --offset="earliest"  Offset where to start from (auto.offset.reset).
            -b, --broker="localhost:29092"
                                     Which broker to poll the messages from.
            -g, --group="<random>    Consumer Group to register. Will be defaulted randomly.
            -T, --topic=TOPIC        Which topic should be used.
            -t, --timeout="6"        Session Timeout in seconds.
            -v, --verbose            Verbose mode.
