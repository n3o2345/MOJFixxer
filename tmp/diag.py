#!/usr/bin/env python3
"""
MOJ Diagnostic — run this inside the container:
  docker exec -it iptv-moveonjoy python3 /tmp/diag.py

Tests a known-good URL format against active domains,
prints raw curl and ffprobe output so you can see exactly what's failing.
"""
import asyncio, subprocess, sys

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

TEST_SLUGS = ["ABC", "CBS", "NBC", "ESPN", "FOX"]
TEST_PATTERNS = [
    "/{slug}/index.m3u8",
    "/{slug}/index.ts",
    "/{slug}.m3u8",
]

async def run(*args, timeout=15):
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
        return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")
    except asyncio.TimeoutError:
        return -1, "", "TIMEOUT"
    except Exception as e:
        return -1, "", str(e)

async def sweep_domains():
    print("=" * 60)
    print("STEP 1: Domain sweep fl1-fl100")
    print("=" * 60)
    active = []
    tasks = []
    for fl in range(1, 101):
        domain = f"fl{fl}.moveonjoy.com"
        tasks.append((domain, asyncio.create_task(
            run("curl", "-sI", "--connect-timeout", "3", "--max-time", "5",
                "--insecure", "-A", UA, "-o", "/dev/null", "-w", "%{http_code}",
                f"http://{domain}/")
        )))
    results = []
    for domain, task in tasks:
        rc, code, _ = await task
        if rc == 0 and code.strip().isdigit() and int(code.strip()) > 0:
            results.append((domain, code.strip()))

    for domain, code in results:
        print(f"  ACTIVE: {domain}  HTTP={code}")
        active.append(domain)

    if not active:
        print("  No active domains found!")
    return active

async def test_curl(url):
    rc, out, err = await run(
        "curl", "-sv", "--max-time", "8", "--insecure", "-A", UA,
        "-o", "/dev/null", "-w", "HTTP:%{http_code} SIZE:%{size_download}",
        url
    )
    return rc, out, err

async def test_ffprobe(url):
    rc, out, err = await run(
        "ffprobe",
        "-v", "warning",          # warning instead of quiet so we see errors
        "-timeout", "8000000",
        "-probesize", "100000",
        "-analyzeduration", "2000000",
        "-user_agent", UA,
        "-allowed_extensions", "ALL",
        "-protocol_whitelist", "crypto,data,file,http,https,tcp,tls",
        "-tls_verify", "0",
        "-show_entries", "format=format_name,duration",
        "-of", "compact",
        "-i", url,
    )
    return rc, out, err

async def probe_domain(domain):
    print(f"\n{'='*60}")
    print(f"STEP 2: Probing {domain}")
    print(f"{'='*60}")

    # First: what does root return?
    for scheme in ("http", "https"):
        rc, out, err = await run(
            "curl", "-sI", "--max-time", "5", "--insecure", "-A", UA,
            "-w", "\nHTTP_CODE:%{http_code}",
            f"{scheme}://{domain}/"
        )
        code_line = [l for l in (out+err).splitlines() if "HTTP_CODE:" in l or "HTTP/" in l]
        print(f"  Root {scheme}://  rc={rc}  {' | '.join(code_line[:3])}")

    # Try known working URL format
    print(f"\n  Testing stream URLs (curl first, then ffprobe if curl OK):")
    found_any = False
    for slug in TEST_SLUGS:
        for pat in TEST_PATTERNS:
            for scheme in ("http", "https"):
                url = f"{scheme}://{domain}" + pat.replace("{slug}", slug)
                rc, out, _ = await run(
                    "curl", "-sI", "--max-time", "5", "--insecure", "-A", UA,
                    "-w", "%{http_code}",
                    "-o", "/dev/null",
                    url
                )
                code = out.strip() if out.strip().isdigit() else "?"
                if code in ("200", "206"):
                    found_any = True
                    print(f"    CURL OK  {code}  {url}")
                    # Now try ffprobe
                    frc, fout, ferr = await test_ffprobe(url)
                    if frc == 0:
                        print(f"    FFPROBE OK  →  {fout.strip()}")
                    else:
                        ferr_short = ferr.strip().splitlines()[-1] if ferr.strip() else "no output"
                        print(f"    FFPROBE FAIL rc={frc}  {ferr_short}")
                elif code not in ("000", ""):
                    print(f"    curl {code}  {url}")

    if not found_any:
        print(f"  No 200 responses on any tested URL.")
        # Show one raw curl verbose to see what the server actually says
        url = f"http://{domain}/ABC/index.m3u8"
        print(f"\n  Raw curl verbose for {url}:")
        rc, out, err = await run(
            "curl", "-sv", "--max-time", "8", "--insecure", "-A", UA,
            "-o", "-",
            url
        )
        combined = (out + err).strip()
        for line in combined.splitlines()[:30]:
            print(f"    {line}")

async def main():
    print("MOJ Diagnostic Script")
    print(f"Python: {sys.version}")

    # Check tools
    for tool in ("curl", "ffprobe"):
        rc, out, _ = await run(tool, "--version")
        ver = out.splitlines()[0] if out else "not found"
        print(f"{tool}: {ver[:60]}")
    print()

    active = await sweep_domains()
    if not active:
        print("\nERROR: No active domains. Check DNS/network from inside container.")
        return

    # Test first 3 domains
    for domain in active[:3]:
        await probe_domain(domain)

    print("\n" + "="*60)
    print("DONE")

asyncio.run(main())
