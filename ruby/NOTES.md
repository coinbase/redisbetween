# Implementing client-side caching

- Add a new url param on upstreams called `cache_prefixes=list,of,prefixes` 
    - can use special value `__all__`? not sure if we need this
- Before creating the first "normal" upstream connection on one that has `cache_prefixes`, create a dedicated
  invalidation channel and save its ID
    - loop on reading messages, processing each invalidation against the local cache
- Then on each subsequent connection, send `CLIENT TRACKING on BCAST PREFIX <list> REDIRECT <ID>` where `<ID>` is the
  client id the invalidation channel
- !!! Any time the invalidation channel disconnects or goes bad, cache must be flushed
- Redisbetween must parse keys out of commands and selectively return values for ones that are found in the cache
    - can gradually support more as needed, starting with just simple GET
- When issuing supported commands like GET, first check in mem cache for the key, then do a full round trip
    - if multiple keys are supported, we will have to parse response values as well. can probably leverage an existing
      redis package for that. radix would work, but should fix the "double decode" situation now with the codis Encoder
      as combined with radix, since radix has its own decoder
- Use a RWLock around the cache on each value
- Local cache _must_ have overall mem bounds and upper-bound TTLs (10 minutes to start? with random jitter?)
- IDEA: redisbetween could proactively fetch and store values as they are invalidated? would make sense for kill
  switches, rates and i18n maybe

# Use cases

- kill switches. each machine will only hit upstream once for the kill switch values and instead read almost entirely
  from cache. however, invalidations could cause stampedes.
- rates. similar story as above. stampedes are possible
- main. mixed workload. will have to study existing queries to find good candidates
- i18n. data rarely changes, and when it does its done by a background job that batches them all out
