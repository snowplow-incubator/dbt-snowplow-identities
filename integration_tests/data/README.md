# Integration test fixtures

`source/snowplow_identities_events.csv` is generated, not hand-written. It is the
recorded output of running identity scenarios through the identity service, so
every row is real service behaviour rather than a guess at it. Each row carries
its scenario name in `name_tracker`.

`expected/*.csv` are captured from the models' own output against that seed, and
checked by hand before being committed.

Generating the seed needs the identity service's own repository, which is
internal to Snowplow, so it cannot be regenerated from here. If the fixtures do
not cover a case you need, open an issue. Adding rows to the seed to demonstrate
it is welcome and useful, and we will turn them into a scenario and regenerate,
which replaces your rows with the generated equivalent.
