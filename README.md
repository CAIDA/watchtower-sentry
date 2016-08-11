Watchtower Sentry
=================

Alert generation component of the [Charthouse](http://charthouse.caida.org/)
Watchtower framework.

Requirements
------------

- python (2.6, 2.7, 3.3, 3.4)
- tornado
- funcparserlib


Usage
-----

Just run `watchtower-sentry`:

    $ watchtower-sentry
    [I 141025 11:16:23 core:141] Read configuration
    [I 141025 11:16:23 core:55] Memory (10minute): init
    [I 141025 11:16:23 core:166] Loaded with options:
    ...

### Configuration

___

Time units:

> '2second', '3.5minute', '4hour', '5.2day', '6week', '7month', '8year'

> short formats are: '2s', '3m', '4.1h' ...

Value units:

> short: '2K', '3Mil', '4Bil', '5Tri'

> bytes: '2KB', '3MB', '4GB'

> bits: '2Kb', '3Mb', '4Gb'

> bps: '2Kbps', '3Mbps', '4Gbps'

> time: '2s', '3m', '4h', '5d'

The default options are:

> Note: comments are not allowed in JSON, but watchtower-sentry strips them

```js

    {
        // Graphite server URL
        "graphite_url": "http://localhost",

        // Public graphite server URL
        // Used when notifying handlers, defaults to graphite_url
        "public_graphite_url": null,

        // HTTP AUTH username
        "auth_username": null,

        // HTTP AUTH password
        "auth_password": null,

        // Path to a pidfile
        "pidfile": null,

        // Default values format (none, bytes, s, ms, short)
        // Can be redefined for each alert.
        "format": "short",

        // Default query interval
        // Can be redefined for each alert.
        "interval": "10minute",

        // Default time window for Graphite queries
        // Defaults to query interval, can be redefined for each alert.
        "time_window": "10minute",

        // Notification repeat interval
        // If an alert is failed, its notification will be repeated with the interval below
        "repeat_interval": "2hour",

        // Default end time for Graphite queries
        // Defaults to the current time, can be redefined for each alert.
        "until": "0second",

        // Default loglevel
        "logging": "info",

        // Default method (average, last_value, sum, minimum, maximum, median, percentile k [eg. percentile 25]).
        // Can be redefined for each alert.
        "method": "average",

        // Default alert to send when no data received (normal = no alert)
        // Can be redefined for each alert
        "no_data": "critical",

        // Default alert to send when loading failed (timeout, server error, etc)
        // (normal = no alert)
        // Can be redefined for each alert
        "loading_error": "critical"

        // Default prefix (used for notifications)
        "prefix": "[BEACON]",

        // Default handlers (log, smtp)
        "critical_handlers": ["log", "smtp"],
        "warning_handlers": ["log", "smtp"],
        "normal_handlers": ["log", "smtp"],

        // Send initial values (Send current values when reactor starts)
        "send_initial": true,

        // used together to ignore the missing value
        "default_nan_value": -1,
        "ignore_nan": false,

        // Default alerts (see configuration below)
        "alerts": [],

        // Path to other configuration files to include
        "include": []
    }
```

You can setup options with a configuration file. See examples for
[JSON](examples/example-config.json) and
[YAML](examples/example-config.yaml).

A `config.json` file in the same directory that you run `watchtower-sentry`
from will be used automatically.

#### Setup alerts

Currently two types of alerts are supported:
- Graphite alert (default) - check graphite metrics
- URL alert - load http and check status

> Note: comments are not allowed in JSON, but watchtower-sentry strips them

```js

  "alerts": [
    {
      // (required) Alert name
      "name": "Memory",

      // (required) Alert query
      "query": "*.memory.memory-free",

      // (optional) Alert type (charthouse, graphite, url)
      "source": "charthouse",

      // (optional) Default values format (none, bytes, s, ms, short)
      "format": "bytes",

      // (optional) Alert method (average, last_value, sum, minimum, maximum, median, percentile k [eg. percentile 25])
      "method": "average",

      // (optional) Alert interval [eg. 15second, 30minute, 2hour, 1day, 3month, 1year]
      "interval": "1minute",

      // (optional) What kind of alert to send when no data received (normal = no alert)
      "no_data": "warning",

      // (optional) Alert interval end time (see "Alert interval" for examples)
      "until": "5second",

      // (required) Alert rules
      // Rule format: "{level}: {operator} {value}"
      // Level one of [critical, warning, normal]
      // Operator one of [>, <, >=, <=, ==, !=]
      // Value (absolute value: 3000000 or short form like 3MB/12minute)
      // Multiple conditions can be separated by AND or OR conditions
      "rules": [ "critical: < 200MB", "warning: < 300MB" ]
    }
  ]
```

##### Historical values

watchtower-sentry supports "historical" values for a rule.
For example you may want to get warning when CPU usage is greater than 150% of normal usage:

    "warning: > historical * 1.5"

Or memory is less than half the usual value:

    "warning: < historical / 2"


Historical values for each query are kept. A historical value
represents the average of all values in history. Rules using a historical value will
only work after enough values have been collected (see `history_size`).

History values are kept for 1 day by default. You can change this with the `history_size`
option.

See the below example for how to send a warning when today's new user count is
less than 80% of the last 10 day average:

```js
alerts: [
  {
    "name": "Registrations",
    // Run once per day
    "interval": "1day",
    "query": "Your graphite query here",
    // Get average for last 10 days
    "history_size": "10day",
    "rules": [
      // Warning if today's new user less than 80% of average for 10 days
      "warning: < historical * 0.8",
     // Critical if today's new user less than 50% of average for 10 days
      "critical: < historical * 0.5"
    ]
  }
],
```

### Handlers

Handlers allow for notifying an external service or process of an alert firing.

#### Email Handler

Sends an email (enabled by default).

```js
{
    // SMTP default options
    "smtp": {
        "from": "beacon@graphite",
        "to": [],                   // List of email addresses to send to
        "host": "localhost",        // SMTP host
        "port": 25,                 // SMTP port
        "username": null,           // SMTP user (optional)
        "password": null,           // SMTP password (optional)
        "use_tls": false,           // Use TLS?
        "html": true,               // Send HTML emails?

        // Graphite link for emails (By default is equal to main graphite_url)
        "graphite_url": null
    }
}
```

### Command Line Usage

```
  $ watchtower-sentry --help
  Usage: watchtower-sentry [OPTIONS]

  Options:

    --config                         Path to an configuration file (JSON/YAML)
                                     (default config.json)
    --graphite_url                   Graphite URL (default http://localhost)
    --help                           show this help information
    --pidfile                        Set pid file

    --log_file_max_size              max size of log files before rollover
                                     (default 100000000)
    --log_file_num_backups           number of log files to keep (default 10)
    --log_file_prefix=PATH           Path prefix for log files. Note that if you
                                     are running multiple tornado processes,
                                     log_file_prefix must be different for each
                                     of them (e.g. include the port number)
    --log_to_stderr                  Send log output to stderr (colorized if
                                     possible). By default use stderr if
                                     --log_file_prefix is not set and no other
                                     logging is configured.
    --logging=debug|info|warning|error|none
                                     Set the Python log level. If 'none', tornado
                                     won't touch the logging configuration.
                                     (default info)
```

License and History
-------------------

Watchtower Sentry is heavily based on [Graphite Beacon](https://github.com/klen/graphite-beacon)

Licensed under a [MIT license](http://www.linfo.org/mitlicense.html)

