import os
from pathlib import Path

_BASE = Path(__file__).parent.parent  # .env files live in the notebooks root
ENV_FILE = _BASE / ".env"
ENV_PRIVATE_FILE = _BASE / ".env.private"
ENV_KEYS = ["DB_HOST", "DB_USER", "DB_PW", "OPENAI_API_KEY"]


def _read_env_file(path: Path) -> dict:
    env = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def _read_env() -> dict:
    """Read .env, then overlay .env.private (private takes priority)."""
    env = _read_env_file(ENV_FILE)
    env.update(_read_env_file(ENV_PRIVATE_FILE))
    return env


def setup_env(force: bool = False) -> None:
    """Prompt for each env var and write to .env.private file.

    Args:
        force: If True, prompt for all keys even if already set.
               If the user leaves input blank, the existing value is kept.
    """
    current = _read_env()

    for key in ENV_KEYS:
        existing = current.get(key)
        if existing and not force:
            continue
        hint = f" (current: {existing!r}, leave blank to keep)" if existing else ""
        value = input(f"{key}{hint}: ").strip()
        if value:
            current[key] = value
        # if blank and existing, keep existing (already in dict)

    lines = [f"{k}={v}" for k, v in current.items()]
    ENV_PRIVATE_FILE.write_text("\n".join(lines) + "\n")
    print(f".env.private written to {ENV_PRIVATE_FILE}")


def load_env() -> None:
    """Load variables from .env / .env.private into os.environ."""
    env = _read_env()
    for k, v in env.items():
        os.environ[k] = v
    print(f"Loaded {list(env.keys())} into os.environ")


_PDF_DIR   = Path(__file__).parent / "data" / "pdfs"
_CONTI_DIR = Path(__file__).parent / "data" / "conti"


def _user_conn_params() -> dict:
    """Return teradatasql connection kwargs for the regular DB user."""
    env = _read_env()
    host = env.get("DB_HOST", "")
    user = env.get("DB_USER", "")
    pw   = env.get("DB_PW", "")
    if not all([host, user, pw]):
        raise ValueError("DB_HOST, DB_USER and DB_PW must be set in .env or .env.private")
    return {"host": host, "user": user, "password": pw, "encryptdata": "true"}


def load_pdfs(table: str = "pdf_documents") -> None:
    """
    Create {DB_USER}.{table} (if not exists) and upload all PDFs from data/pdfs/.

    Schema:
        file_name  VARCHAR(255) NOT NULL  -- PRIMARY INDEX
        file_pdf   BLOB
    """
    import teradatasql

    env  = _read_env()
    user = env.get("DB_USER", "")
    fqn  = f"{user}.{table}"

    pdfs = sorted(_PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDF files found in {_PDF_DIR}")
        return

    with teradatasql.connect(**_user_conn_params()) as conn:
        with conn.cursor() as cur:
            # Create table if it doesn't exist (Teradata error 3803 = already exists)
            try:
                cur.execute(f"""
                    CREATE TABLE {fqn} (
                        file_name VARCHAR(255) CHARACTER SET UNICODE NOT NULL,
                        file_pdf  BLOB
                    ) PRIMARY INDEX (file_name)
                """)
                print(f"Table {fqn} created")
            except Exception as e:
                if "3803" in str(e):
                    print(f"Table {fqn} already exists — reusing")
                else:
                    raise

            # Upsert: delete then insert so re-runs are safe
            for path in pdfs:
                blob = path.read_bytes()
                cur.execute(f"DELETE FROM {fqn} WHERE file_name = ?", [path.name])
                cur.execute(f"INSERT INTO {fqn} (file_name, file_pdf) VALUES (?, ?)",
                            [path.name, blob])
                print(f"  uploaded {path.name} ({len(blob):,} bytes)")

    print(f"Loaded {len(pdfs)} PDFs into {fqn}")


def drop_pdf_table(table: str = "pdf_documents") -> None:
    """Drop {DB_USER}.{table} if it exists."""
    import teradatasql

    env  = _read_env()
    user = env.get("DB_USER", "")
    fqn  = f"{user}.{table}"

    with teradatasql.connect(**_user_conn_params()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"DROP TABLE {fqn}")
                print(f"Table {fqn} dropped")
            except Exception as e:
                if "3807" in str(e):   # 3807 = object does not exist
                    print(f"Table {fqn} does not exist — nothing to drop")
                else:
                    raise


def load_conti(table: str = "conti") -> None:
    """
    Create {DB_USER}.{table} (if not exists) and upload all images from data/conti/.

    Schema:
        conto_id   VARCHAR(255) NOT NULL  -- PRIMARY INDEX (filename stem, e.g. conto_01)
        conto_img  BLOB
    """
    import teradatasql

    env  = _read_env()
    user = env.get("DB_USER", "")
    fqn  = f"{user}.{table}"

    images = sorted(
        f for f in _CONTI_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    )
    if not images:
        print(f"No image files found in {_CONTI_DIR}")
        return

    with teradatasql.connect(**_user_conn_params()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"""
                    CREATE TABLE {fqn} (
                        conto_id  VARCHAR(255) CHARACTER SET UNICODE NOT NULL,
                        conto_img BLOB
                    ) PRIMARY INDEX (conto_id)
                """)
                print(f"Table {fqn} created")
            except Exception as e:
                if "3803" in str(e):
                    print(f"Table {fqn} already exists — reusing")
                else:
                    raise

            for path in images:
                conto_id = path.stem          # e.g. "conto_01"
                blob     = path.read_bytes()
                cur.execute(f"DELETE FROM {fqn} WHERE conto_id = ?", [conto_id])
                cur.execute(f"INSERT INTO {fqn} (conto_id, conto_img) VALUES (?, ?)",
                            [conto_id, blob])
                print(f"  uploaded {path.name} ({len(blob):,} bytes)")

    print(f"Loaded {len(images)} images into {fqn}")


def drop_conti_table(table: str = "conti") -> None:
    """Drop {DB_USER}.{table} if it exists."""
    import teradatasql

    env  = _read_env()
    user = env.get("DB_USER", "")
    fqn  = f"{user}.{table}"

    with teradatasql.connect(**_user_conn_params()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"DROP TABLE {fqn}")
                print(f"Table {fqn} dropped")
            except Exception as e:
                if "3807" in str(e):
                    print(f"Table {fqn} does not exist — nothing to drop")
                else:
                    raise


def show_pdfs_widget() -> None:
    """Interactive ◀/▶ navigator that wraps show_pdfs()."""
    import ipywidgets as widgets
    from IPython.display import display as ipy_display

    pdfs = sorted(_PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDF files found in {_PDF_DIR}")
        return

    idx  = [0]
    out  = widgets.Output()
    lbl  = widgets.Label(value=pdfs[0].name)
    prev = widgets.Button(description="◀", layout=widgets.Layout(width="50px"))
    nxt  = widgets.Button(description="▶", layout=widgets.Layout(width="50px"))

    def _show():
        lbl.value = pdfs[idx[0]].name
        with out:
            out.clear_output(wait=True)
            show_pdfs(idx[0] + 1)

    def _prev(_):
        idx[0] = (idx[0] - 1) % len(pdfs)
        _show()

    def _next(_):
        idx[0] = (idx[0] + 1) % len(pdfs)
        _show()

    prev.on_click(_prev)
    nxt.on_click(_next)

    ipy_display(widgets.VBox([
        widgets.HBox([prev, nxt, lbl]),
        out,
    ]))
    _show()


def show_pdfs(n: int = 1) -> None:
    """Display PDF number n (1-based) from data/pdfs/ inline in the notebook.

    Args:
        n: Patient number, 1–20.
    """
    from IPython.display import display as ipy_display, IFrame

    pdfs = sorted(_PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDF files found in {_PDF_DIR}")
        return

    n = max(1, min(n, len(pdfs)))
    path = pdfs[n - 1]

    # Jupyter serves files relative to the notebook root; build that relative path.
    notebook_root = _PDF_DIR.parent.parent.parent  # demosetup/data/pdfs -> notebooks/
    rel = path.relative_to(notebook_root).as_posix()

    print(f"Showing: {path.name}")
    ipy_display(IFrame(src=rel, width=700, height=900))


def show_conti() -> None:
    """Interactive ipywidgets viewer for images in data/conti/."""
    import ipywidgets as widgets
    from IPython.display import display as ipy_display

    images = sorted(
        f for f in _CONTI_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    )
    if not images:
        print(f"No images found in {_CONTI_DIR}")
        return

    idx = [0]  # mutable index

    def _mime(path: Path) -> str:
        ext = path.suffix.lower()
        return {"jpg": "image/jpeg", "jpeg": "image/jpeg",
                "png": "image/png", "webp": "image/webp",
                "gif": "image/gif"}.get(ext.lstrip("."), "image/jpeg")

    img_widget  = widgets.Image(value=images[0].read_bytes(), format=_mime(images[0]), width=500)
    label       = widgets.Label(value=images[0].name,
                                layout=widgets.Layout(justify_content="center"))
    btn_prev    = widgets.Button(description="◀", layout=widgets.Layout(width="60px"))
    btn_next    = widgets.Button(description="▶", layout=widgets.Layout(width="60px"))

    def _update():
        path = images[idx[0]]
        img_widget.value  = path.read_bytes()
        img_widget.format = _mime(path)
        label.value       = path.name

    def _prev(_):
        idx[0] = (idx[0] - 1) % len(images)
        _update()

    def _next(_):
        idx[0] = (idx[0] + 1) % len(images)
        _update()

    btn_prev.on_click(_prev)
    btn_next.on_click(_next)

    nav = widgets.HBox(
        [btn_prev, img_widget, btn_next],
        layout=widgets.Layout(align_items="center", justify_content="center"),
    )
    ui = widgets.VBox(
        [nav, label],
        layout=widgets.Layout(align_items="center"),
    )
    ipy_display(ui)


def smoke_test_db() -> None:
    """Verify Teradata connectivity using DB_HOST / DB_USER / DB_PW."""
    import teradatasql
    env = _read_env()
    host = env.get("DB_HOST", "")
    user = env.get("DB_USER", "")
    pw = env.get("DB_PW", "")
    if not all([host, user, pw]):
        print("FAIL  DB: DB_HOST, DB_USER and DB_PW must all be set")
        return
    try:
        with teradatasql.connect(host=host, user=user, password=pw, encryptdata="true") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 'ok' AS ping")
                result = cur.fetchone()[0]
        print(f"OK    DB: connected to {host} as {user} (ping={result!r})")
    except Exception as e:
        print(f"FAIL  DB: {e}")


def smoke_test_openai() -> None:
    """Verify OpenAI API key with a minimal models list call."""
    import urllib.request, urllib.error, json
    env = _read_env()
    api_key = env.get("OPENAI_API_KEY", "")
    if not api_key:
        print("FAIL  OpenAI: OPENAI_API_KEY is not set")
        return
    req = urllib.request.Request(
        "https://api.openai.com/v1/models",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        first = data["data"][0]["id"] if data.get("data") else "?"
        print(f"OK    OpenAI: API key valid (first model: {first!r})")
    except urllib.error.HTTPError as e:
        print(f"FAIL  OpenAI: HTTP {e.code} — {e.reason}")
    except Exception as e:
        print(f"FAIL  OpenAI: {e}")