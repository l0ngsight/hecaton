# hecaton.org.uk website

The project website and the configuration schemas that Hecaton's XML namespaces point to.

| Path | Content |
|---|---|
| `static/` | The page (`index.html`), `style.css`, pixel art, `404.html`, Cloudflare `_headers` |
| `build.sh` | Builds `dist/`: copies `static/` and the schemas from the source tree |

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

## Deploy to Cloudflare Pages

The domain is already on Cloudflare, so DNS and TLS are set up for you when the domain is
attached. Do this once:

1. **Create the project.** Cloudflare dashboard → **Workers & Pages** → **Create** → **Pages**
   tab → **Import an existing Git repository**. Connect GitHub if asked and choose
   `l0ngsight/hecaton`.
2. **Build settings:**

   | Setting | Value |
   |---|---|
   | Project name | `hecaton` (the site is then also at `hecaton.pages.dev`) |
   | Production branch | `main` |
   | Framework preset | None |
   | Build command | `bash site/build.sh` |
   | Build output directory | `site/dist` |
   | Root directory | *(empty)* |

   **Save and Deploy**. The first build takes a minute; open the `*.pages.dev` URL it shows
   and check the page and `/schema/config/`.
3. **Build only when the site or schemas change** (optional): project → **Settings** →
   **Builds** → **Build watch paths** → include `site/*`, `hecaton/src/main/resources/*`,
   `hecaton-spring/src/main/resources/*`. Pull requests that touch them get a preview URL.
4. **Attach the domain:** project → **Custom domains** → **Set up a custom domain** →
   `hecaton.org.uk` → **Activate domain**. Cloudflare adds the DNS record itself. Repeat for
   `www.hecaton.org.uk` if you want it.
5. **Redirect `www`** (if added): the zone `hecaton.org.uk` → **Rules** → **Redirect Rules** →
   template *Redirect from WWW to root*, status 301.
6. **Check:** after a few minutes (certificate issuing):

   ```sh
   curl -sI https://hecaton.org.uk/ | head -1
   curl -sI https://hecaton.org.uk/schema/config/hecaton-config-5.7.xsd | grep -iE '^(HTTP|content-type)'
   ```

   Both should answer `200`, the schema with `content-type: application/xml`. The schemas must
   be served directly (no redirect): `XmlConfigSchemaLocationTest` fetches them without following
   redirects. When they are, remove its line from `tools/ci/known-test-failures.txt`.

From then on every push to `main` redeploys the site.

**Without Git integration:** build locally and upload with Wrangler (needs a Cloudflare login):

```sh
bash site/build.sh
npx wrangler pages deploy site/dist --project-name hecaton --branch main
```
