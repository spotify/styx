# Development Guidelines

This section describes guidelines put in place by Styx authors – they
should be helpful in making some common everyday coding choices trivial
and consistent, which helps development speed and quality.


## Logging

Choose a log level accordingly:
* `error` when a problem is encountered that was not supposed to occur
  by design. A ["This should never happen"](https://github.com/spotify/styx/blob/75f4463f52f964a131473cbc8597edd4d76834d3/styx-common/src/main/java/com/spotify/styx/util/Retrier.java#L74)
  type of thing, symptomatic of a Styx bug. 
* `warn` when Styx cannot run a workflow or respond to an API call; e.g.
  an external system being down. (Note: Styx design is presumed to be
  resilient to intermittent failures in the first place, therefore an
  external system being temporarily down is not a Styx error and should
  not inevitably lead to one.)
* `info` for other significant occurrences; e.g. starting and stopping,
  responding to API calls.
* `debug` for other occurrences, uncertain significance but eventually
   helpful in troubleshooting; e.g. workflow instance state transitions.

Problems that are likely user errors (for example, an ill-formed
workflow schedule configuration) should first and foremost be exposed to
the users, by inserting an informative message in the error event.
Additionally they can be logged in Styx logs as `info` or lower.  

Ideally the error and warning logs should be usable as the intuitive
starting points in troubleshooting situations. This is to say that they
should not be spammy. Batch warnings, employ best practices of backoff
and caching.
