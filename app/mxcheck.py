"""Cheap deliverability pre-check: does an email domain accept mail at all (MX record, or an A record fallback)?
Only domains that clearly can't receive mail are reported. Lookup errors/timeouts count as "fine"."""
import asyncio
import os

ENABLED = os.environ.get("MX_CHECK", "1") != "0"
_cache = {}


async def _dead(resolver, domain):
    import dns.resolver
    if domain in _cache:
        return _cache[domain]
    dead = False
    try:
        await resolver.resolve(domain, "MX")
    except (dns.resolver.NXDOMAIN,):
        dead = True
    except dns.resolver.NoAnswer:
        try:
            await resolver.resolve(domain, "A")
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            dead = True
        except Exception:
            pass
    except Exception:
        pass  # timeout / server failure: don't drop on uncertainty
    _cache[domain] = dead
    return dead


async def dead_domains(domains):
    if not ENABLED or not domains:
        return set()
    try:
        import dns.asyncresolver
    except ImportError:
        return set()
    resolver = dns.asyncresolver.Resolver()
    resolver.lifetime = 4.0
    sem = asyncio.Semaphore(50)

    async def one(d):
        async with sem:
            return d, await _dead(resolver, d)
    return {d for d, dead in await asyncio.gather(*(one(d) for d in domains)) if dead}
