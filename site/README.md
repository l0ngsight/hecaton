# hecaton.org.uk website

The project website and the configuration schemas that Hecaton's XML namespaces point to.

| Path | Content |
|---|---|
| `static/` | The page (`index.html`), `style.css`, pixel art, `404.html`, Cloudflare `_headers` |
| `build.sh` | Builds `dist/`: copies `static/` and the schemas from the source tree |
| `../wrangler.jsonc` | The Cloudflare Worker that serves `dist/` |

The schemas are not copied into `site/`: `build.sh` takes them from
`hecaton/src/main/resources` and `hecaton-spring/src/main/resources` on every build, so the
published ones always match the code. It publishes:

| URL | Files |
|---|---|
| `https://hecaton.org.uk/schema/config/` | `hecaton-config-<v>.xsd`, `hecaton-config-<v>.json` (YAML) |
| `https://hecaton.org.uk/schema/client-config/` | `hecaton-client-config-<v>.xsd`, `hecaton-client-failover-config-<v>.xsd` |
| `https://hecaton.org.uk/schema/spring/` | `hecaton-spring-<v>.xsd`, `hecaton-spring.xsd` (the latest, as in `META-INF/spring.schemas`) |

## Build and preview locally

```sh
bash site/build.sh
python3 -m http.server 8787 -d site/dist     # http://localhost:8787
```

## Deploy to Cloudflare

The site is a Cloudflare Worker that only serves static files, configured by `wrangler.jsonc`
at the repository root. The domain is already on Cloudflare, so DNS and TLS are set up for you
when the domain is attached. Do this once:

1. **Create the Worker.** Cloudflare dashboard (account home, not the domain) → **Compute** →
   **Workers & Pages** → **Create application** → **Import a repository**. Connect GitHub if
   asked and choose `l0ngsight/hecaton`.
2. **Settings:**

   | Setting | Value |
   |---|---|
   | Project name | `hecaton` (must match `name` in `wrangler.jsonc`) |
   | Build command | `bash site/build.sh` |
   | Deploy command | `npx wrangler deploy` |
   | Non-production branch deploy command | `npx wrangler versions upload` |
   | Path | `/` |

   **Deploy**. Open the `hecaton.<account>.workers.dev` URL it shows and check the page and
   `/schema/config/`.
3. **Build only when the site or schemas change** (optional): the Worker → **Settings** →
   **Build** → **Build watch paths** → include `site/*`, `wrangler.jsonc`,
   `hecaton/src/main/resources/*`, `hecaton-spring/src/main/resources/*`.
4. **Attach the domain:** the Worker → **Settings** → **Domains & Routes** → **Add** →
   **Custom domain** → `hecaton.org.uk`. Cloudflare adds the DNS record and certificate. Add
   `www.hecaton.org.uk` the same way if you want it, then redirect it: the zone
   `hecaton.org.uk` → **Rules** → **Redirect Rules** → template *Redirect from WWW to root*.
   If the domain already has a DNS record for `hecaton.org.uk` (a parking page, for example),
   delete it first.
5. **Check:** after a few minutes (certificate issuing):

   ```sh
   curl -sI https://hecaton.org.uk/ | head -1
   curl -sI https://hecaton.org.uk/schema/config/hecaton-config-5.7.xsd | grep -iE '^(HTTP|content-type)'
   ```

   Both should answer `200`, the schema with `content-type: application/xml`. The schemas must
   be served directly (no redirect): `XmlConfigSchemaLocationTest` fetches them without following
   redirects. When they are, remove its line from `tools/ci/known-test-failures.txt`.

From then on every push to `main` redeploys the site.

**Without Git integration:** build locally and deploy with Wrangler (needs a Cloudflare login):

```sh
bash site/build.sh
npx wrangler deploy
```

`npx wrangler dev` serves the built site locally the way Cloudflare will.
