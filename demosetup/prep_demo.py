import os
import teradatasql
from pathlib import Path
from .utils import _read_env, load_pdfs, drop_pdf_table, load_conti, drop_conti_table

_DEMOSETUP_DIR = Path(__file__).parent
_OPENAI_JAR = _DEMOSETUP_DIR / "openai.client-1.2.0.jar"
_PDF_JAR = _DEMOSETUP_DIR / "pdfparse-1.0-SNAPSHOT.jar"


def _dbc_conn_params() -> dict:
    """Return teradatasql connection kwargs for the dbc admin user."""
    env = _read_env()
    host = env.get("DB_HOST") or os.getenv("DB_HOST")
    pw = env.get("DB_PW") or os.getenv("DB_PW")  # dbc password == user password
    if not host or not pw:
        raise ValueError("DB_HOST and DB_PW must be set in .env or .env.private")
    return {"host": host, "user": "dbc", "password": pw, "encryptdata": "true"}


# ---------------------------------------------------------------------------
# OpenAI client UDFs
# ---------------------------------------------------------------------------

def install_openai_udfs(install_database: str = "td_udfs"):
    """Install CompleteChat, Embeddings and ImageVision functions."""
    if not _OPENAI_JAR.exists():
        raise FileNotFoundError(f"JAR not found: {_OPENAI_JAR}")

    with teradatasql.connect(**_dbc_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT HASHAMP()+1 as num_amps")
            num_amps = cur.fetchone()[0]
            perm_size = 8_000_000 * num_amps

            print(f"AMPs: {num_amps}  |  Creating database {install_database} (PERM={perm_size})")
            cur.execute(f"CREATE DATABASE {install_database} AS PERM = {perm_size}")
            cur.execute(f"DATABASE {install_database}")
            cur.execute(f"GRANT CREATE EXTERNAL PROCEDURE ON {install_database} TO dbc")
            cur.execute(f"GRANT CREATE FUNCTION ON {install_database} TO dbc")

            print("Installing OpenAI client JAR...")
            cur.execute(f"""
                CALL SQLJ.INSTALL_JAR(
                    'cj!{_OPENAI_JAR.absolute()}',
                    'OPENAI_CLIENT',
                    0
                )
            """)

            print("Creating CompleteChat...")
            cur.execute(f"""
                REPLACE FUNCTION {install_database}.CompleteChat()
                RETURNS TABLE VARYING USING FUNCTION OpenAIClientTO_contract
                LANGUAGE JAVA
                NO SQL
                PARAMETER STYLE SQLTable
                EXTERNAL NAME 'OPENAI_CLIENT:com.teradata.openai.client.ChatCompletionTO.execute()';
            """)

            print("Creating Embeddings...")
            cur.execute(f"""
                REPLACE FUNCTION {install_database}.Embeddings()
                RETURNS TABLE VARYING USING FUNCTION EmbeddingsTO_contract
                LANGUAGE JAVA
                NO SQL
                PARAMETER STYLE SQLTable
                EXTERNAL NAME 'OPENAI_CLIENT:com.teradata.openai.client.EmbeddingsTO.execute()';
            """)

            print("Creating ImageVision...")
            cur.execute(f"""
                REPLACE FUNCTION {install_database}.ImageVision()
                RETURNS TABLE VARYING USING FUNCTION ImageVisionTO_contract
                LANGUAGE JAVA
                NO SQL
                PARAMETER STYLE SQLTable
                EXTERNAL NAME 'OPENAI_CLIENT:com.teradata.openai.client.ImageVisionTO.execute()';
            """)

            print(f"OpenAI UDFs installed in {install_database}")


def uninstall_openai_udfs(install_database: str = "td_udfs"):
    """Drop CompleteChat, Embeddings and ImageVision functions and the JAR."""
    with teradatasql.connect(**_dbc_conn_params()) as conn:
        with conn.cursor() as cur:
            for fn in ("CompleteChat", "Embeddings", "ImageVision"):
                try:
                    cur.execute(f"DROP FUNCTION {install_database}.{fn}")
                    print(f"Dropped {install_database}.{fn}")
                except Exception as e:
                    print(f"Warning ({fn}): {e}")
            try:
                cur.execute(f"DATABASE {install_database}")
                cur.execute(f"CALL SQLJ.REMOVE_JAR('OPENAI_CLIENT', 0)")
                print("OpenAI client JAR removed")
            except Exception as e:
                print(f"Warning (JAR): {e}")


# ---------------------------------------------------------------------------
# PDF UDFs
# ---------------------------------------------------------------------------

def install_pdf_udfs(install_database: str = "td_udfs"):
    """Install PDFParse and PDFFormExtract functions."""
    if not _PDF_JAR.exists():
        raise FileNotFoundError(f"JAR not found: {_PDF_JAR}")

    with teradatasql.connect(**_dbc_conn_params()) as conn:
        with conn.cursor() as cur:
            # Database may already exist (created by install_openai_udfs)
            try:
                cur.execute("SELECT HASHAMP()+1 as num_amps")
                num_amps = cur.fetchone()[0]
                perm_size = 5 * 1_024 * 1_024 * num_amps
                cur.execute(f"CREATE DATABASE {install_database} AS PERM = {perm_size}")
                cur.execute(f"GRANT CREATE FUNCTION ON {install_database} TO dbc")
                print(f"Database {install_database} created")
            except Exception:
                print(f"Database {install_database} already exists — reusing")

            cur.execute(f"DATABASE {install_database}")

            print("Installing PDF parser JAR...")
            cur.execute(f"""
                CALL SQLJ.INSTALL_JAR(
                    'cj!{_PDF_JAR.absolute()}',
                    'PDF',
                    0
                )
            """)

            print("Creating PDFParse...")
            cur.execute(f"""
                CREATE FUNCTION {install_database}.PDFParse (
                    file BLOB
                )
                RETURNS VARCHAR(32000) CHARACTER SET UNICODE
                LANGUAGE JAVA
                NO SQL
                RETURNS NULL ON NULL INPUT
                PARAMETER STYLE JAVA
                EXTERNAL NAME 'PDF:com.teradata.pdfparse.MartinPDFParse.parsePdfFromBlob'
            """)

            print("Creating PDFFormExtract...")
            cur.execute(f"""
                CREATE FUNCTION {install_database}.PDFFormExtract (
                    file BLOB
                )
                RETURNS VARCHAR(32000) CHARACTER SET UNICODE
                LANGUAGE JAVA
                NO SQL
                RETURNS NULL ON NULL INPUT
                PARAMETER STYLE JAVA
                EXTERNAL NAME 'PDF:com.teradata.pdfparse.MartinPDFFormExtract.extractFormFields'
            """)

            print(f"PDF UDFs installed in {install_database}")


def uninstall_pdf_udfs(install_database: str = "td_udfs"):
    """Drop PDFParse and PDFFormExtract functions and the JAR."""
    with teradatasql.connect(**_dbc_conn_params()) as conn:
        with conn.cursor() as cur:
            for fn in ("PDFParse", "PDFFormExtract"):
                try:
                    cur.execute(f"DROP FUNCTION {install_database}.{fn}")
                    print(f"Dropped {install_database}.{fn}")
                except Exception as e:
                    print(f"Warning ({fn}): {e}")
            try:
                cur.execute(f"DATABASE {install_database}")
                cur.execute(f"CALL SQLJ.REMOVE_JAR('PDF', 0)")
                print("PDF JAR removed")
            except Exception as e:
                print(f"Warning (JAR): {e}")


# ---------------------------------------------------------------------------
# Grant / Revoke
# ---------------------------------------------------------------------------

def grant_execution_rights(install_database: str = "td_udfs", target_database: str = "demo_user"):
    """Grant EXECUTE FUNCTION on all UDFs to a target user/database."""
    with teradatasql.connect(**_dbc_conn_params()) as conn:
        with conn.cursor() as cur:
            for fn in ("CompleteChat", "Embeddings", "ImageVision", "PDFParse", "PDFFormExtract"):
                print(f"Granting EXECUTE FUNCTION on {install_database}.{fn} to {target_database}...")
                cur.execute(f"GRANT EXECUTE FUNCTION ON {install_database}.{fn} TO {target_database}")
            print(f"All execution rights granted to {target_database}")


# ---------------------------------------------------------------------------
# One-shot helpers
# ---------------------------------------------------------------------------

def demosetup():
    """Install all UDFs, grant rights to demo_user, and upload all demo data."""
    install_openai_udfs()
    install_pdf_udfs()
    grant_execution_rights()
    load_pdfs()
    load_conti()


def democlean():
    """Remove all demo data tables, UDFs and the UDF database."""
    drop_pdf_table()
    drop_conti_table()
    uninstall_openai_udfs()
    uninstall_pdf_udfs()
    with teradatasql.connect(**_dbc_conn_params()) as conn:
        with conn.cursor() as cur:
            db = "td_udfs"
            try:
                cur.execute(f"DELETE DATABASE {db}")
            except Exception as e:
                print(f"Warning (DELETE DATABASE): {e}")
            try:
                cur.execute(f"DROP DATABASE {db}")
                print(f"Database {db} dropped")
            except Exception as e:
                print(f"Warning (DROP DATABASE): {e}")
    print("\nCleanup complete!")
