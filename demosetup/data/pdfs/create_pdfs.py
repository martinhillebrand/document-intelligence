"""
Generate fillable admission-form PDFs from pazienti.csv.

Each PDF contains interactive AcroForm text fields pre-filled with patient
data, so the form can be edited and the PDF form-field extractor can be tested.

Requires: pip install reportlab
Output:   one PDF per patient in the same directory as this script.
"""

import csv
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.pdfgen import canvas

_HERE = Path(__file__).parent
_CSV  = _HERE / "pazienti.csv"
_OUT  = _HERE

# ── colours ───────────────────────────────────────────────────────────────────
_RED   = colors.HexColor("#C0392B")
_GREY  = colors.HexColor("#555555")
_LGREY = colors.HexColor("#F8F8F8")
_BLACK = colors.black
_WHITE = colors.white

W, H     = A4          # 595.28 x 841.89 pt
MARGIN_L = 2.2 * cm
MARGIN_R = W - 2.2 * cm

FIELD_H  = 16          # pt – height of a single-line AcroForm field
LABEL_H  = 9           # pt – space reserved for the label above the field
ROW_H    = FIELD_H + LABEL_H + 6   # total vertical space per field row


# ── helpers ───────────────────────────────────────────────────────────────────

def _label(c: canvas.Canvas, text: str, x: float, y: float) -> None:
    """Draw a small grey label above a field."""
    c.setFont("Helvetica", 7)
    c.setFillColor(_GREY)
    c.drawString(x, y, text.upper())


def _field(c: canvas.Canvas, name: str, label: str, value: str,
           x: float, y: float, w: float,
           multiline: bool = False, height: int = FIELD_H) -> None:
    """
    Draw a labelled AcroForm text field.

    y is the TOP of the overall slot (label + field box).
    The label sits at the top; the field box sits below it.
    """
    label_y  = y - LABEL_H + 1          # baseline of the label text
    field_y  = y - LABEL_H - height     # bottom of the field box (reportlab origin)

    _label(c, label, x, label_y)

    c.acroForm.textfield(
        name=name,
        value=value,
        x=x,
        y=field_y,
        width=w,
        height=height,
        fontSize=8.5,
        borderColor=colors.HexColor("#BBBBBB"),
        fillColor=_LGREY,
        textColor=_BLACK,
        forceBorder=True,
        borderWidth=0.5,
        fieldFlags="multiline" if multiline else "",
        maxlen=1000,
    )


def _section(c: canvas.Canvas, title: str, y: float) -> float:
    """Coloured section-header band; returns y at bottom of band."""
    c.setFillColor(_RED)
    c.rect(MARGIN_L, y - 4, MARGIN_R - MARGIN_L, 18, fill=1, stroke=0)
    c.setFillColor(_WHITE)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(MARGIN_L + 5, y + 1, title.upper())
    return y - 26


def _hline(c: canvas.Canvas, y: float) -> None:
    c.setStrokeColor(_RED)
    c.setLineWidth(0.8)
    c.line(MARGIN_L, y, MARGIN_R, y)


# ── page builder ──────────────────────────────────────────────────────────────

def build_pdf(patient: dict, out_path: Path) -> None:
    c = canvas.Canvas(str(out_path), pagesize=A4)

    col_w = MARGIN_R - MARGIN_L
    half  = col_w / 2 - 0.3 * cm
    third = col_w / 3 - 0.3 * cm
    gap   = 0.6 * cm

    # ── header (static, not a form field) ────────────────────────────────────
    c.setFillColor(_RED)
    c.rect(0, H - 2.5 * cm, W, 2.5 * cm, fill=1, stroke=0)

    c.setFillColor(_WHITE)
    c.setFont("Helvetica-Bold", 15)
    c.drawString(MARGIN_L, H - 1.35 * cm, "OSPEDALE REGIONALE DI LUGANO")
    c.setFont("Helvetica", 9)
    c.drawString(MARGIN_L, H - 1.9 * cm, "Ente Ospedaliero Cantonale – Cantone Ticino")

    c.setFont("Helvetica-Bold", 9)
    c.drawRightString(MARGIN_R, H - 1.2 * cm, f"ID: {patient['paziente_id']}")
    c.setFont("Helvetica", 8)
    c.drawRightString(MARGIN_R, H - 1.75 * cm, f"Ricovero: {patient['data_ricovero']}")

    c.setFillColor(_BLACK)
    c.setFont("Helvetica-Bold", 13)
    c.drawCentredString(W / 2, H - 3.3 * cm, "MODULO DI AMMISSIONE OSPEDALIERA")
    _hline(c, H - 3.6 * cm)

    y = H - 4.0 * cm   # current top cursor (decrements downward)

    # ── DATI PERSONALI ────────────────────────────────────────────────────────
    y = _section(c, "Dati Personali", y)

    _field(c, "nome",    "Nome",    patient["nome"],    MARGIN_L,           y, half)
    _field(c, "cognome", "Cognome", patient["cognome"], MARGIN_L+half+gap,  y, half)
    y -= ROW_H

    _field(c, "data_nascita",   "Data di Nascita",  patient["data_nascita"],  MARGIN_L,                    y, third)
    _field(c, "luogo_nascita",  "Luogo di Nascita", patient["luogo_nascita"], MARGIN_L+third+gap*0.5,      y, third)
    _field(c, "sesso",          "Sesso",            patient["sesso"],         MARGIN_L+2*(third+gap*0.5),  y, third*0.45)
    y -= ROW_H

    _field(c, "numero_avs", "Numero AVS", patient["numero_avs"], MARGIN_L,             y, third)
    _field(c, "telefono",   "Telefono",   patient["telefono"],   MARGIN_L+third+gap*0.5, y, third)
    _field(c, "email",      "Email",      patient["email"],      MARGIN_L+2*(third+gap*0.5), y, third)
    y -= ROW_H

    _field(c, "indirizzo", "Indirizzo", patient["indirizzo"], MARGIN_L,           y, half)
    _field(c, "cap",       "CAP",       patient["cap"],       MARGIN_L+half+gap,  y, 1.8*cm)
    _field(c, "citta",     "Città",     patient["citta"],     MARGIN_L+half+gap+2.1*cm, y, half-2.1*cm)
    y -= ROW_H

    _field(c, "medico_curante", "Medico Curante", patient["medico_curante"], MARGIN_L, y, col_w)
    y -= ROW_H

    # ── DATI DI RICOVERO ──────────────────────────────────────────────────────
    y = _section(c, "Dati di Ricovero", y)

    _field(c, "data_ricovero",       "Data Ricovero",   patient["data_ricovero"],        MARGIN_L,                  y, third)
    _field(c, "reparto",             "Reparto",         patient["reparto"],              MARGIN_L+third+gap*0.5,    y, third)
    _field(c, "medico_responsabile", "Medico Resp.",    patient["medico_responsabile"],  MARGIN_L+2*(third+gap*0.5), y, third)
    y -= ROW_H

    _field(c, "motivo_ricovero",    "Motivo del Ricovero",  patient["motivo_ricovero"],    MARGIN_L, y, col_w)
    y -= ROW_H

    _field(c, "diagnosi_principale", "Diagnosi Principale", patient["diagnosi_principale"], MARGIN_L, y, col_w)
    y -= ROW_H

    # ── INFORMAZIONI CLINICHE ─────────────────────────────────────────────────
    y = _section(c, "Informazioni Cliniche", y)

    _field(c, "allergie",        "Allergie",        patient["allergie"],        MARGIN_L, y, col_w)
    y -= ROW_H

    # Farmaci gets a taller multiline field
    farmaci_h = 32
    _field(c, "farmaci_correnti", "Farmaci Correnti", patient["farmaci_correnti"],
           MARGIN_L, y, col_w, multiline=True, height=farmaci_h)
    y -= (LABEL_H + farmaci_h + 10)

    # ── DATI ASSICURATIVI ─────────────────────────────────────────────────────
    y = _section(c, "Dati Assicurativi", y)

    _field(c, "assicurazione",  "Assicurazione",  patient["assicurazione"],  MARGIN_L,           y, half)
    _field(c, "numero_polizza", "Numero Polizza", patient["numero_polizza"], MARGIN_L+half+gap,  y, half)
    y -= ROW_H

    # ── NOTE CLINICHE ─────────────────────────────────────────────────────────
    y = _section(c, "Note Cliniche", y)

    note_h = 52
    _field(c, "note", "Note", patient["note"],
           MARGIN_L, y, col_w, multiline=True, height=note_h)
    y -= (LABEL_H + note_h + 12)

    # ── FIRMA ─────────────────────────────────────────────────────────────────
    _hline(c, y)
    y -= 1.2 * cm
    c.setFont("Helvetica", 8)
    c.setFillColor(_GREY)
    sig_w = (col_w - 1 * cm) / 2
    c.line(MARGIN_L, y, MARGIN_L + sig_w, y)
    c.drawString(MARGIN_L, y - 0.4 * cm, "Firma del paziente / rappresentante legale")
    c.line(MARGIN_L + sig_w + 1 * cm, y, MARGIN_R, y)
    c.drawString(MARGIN_L + sig_w + 1 * cm, y - 0.4 * cm, "Firma del medico responsabile")

    # ── footer ────────────────────────────────────────────────────────────────
    c.setFillColor(_RED)
    c.rect(0, 0, W, 0.8 * cm, fill=1, stroke=0)
    c.setFillColor(_WHITE)
    c.setFont("Helvetica", 7)
    c.drawString(MARGIN_L, 0.28 * cm,
                 "Ospedale Regionale di Lugano  |  Via Tesserete 46, 6900 Lugano  |  Tel. +41 91 811 61 11  |  www.eoc.ch")
    c.drawRightString(MARGIN_R, 0.28 * cm, "Modulo EOC-AMM-2026  |  Pagina 1 / 1")

    c.save()
    print(f"  ✓  {out_path.name}")


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    if not _CSV.exists():
        raise FileNotFoundError(f"CSV not found: {_CSV}")

    with _CSV.open(encoding="utf-8") as f:
        patients = list(csv.DictReader(f))

    print(f"Generating {len(patients)} fillable PDFs → {_OUT}")
    for p in patients:
        fname = f"ammissione_{p['paziente_id']}_{p['cognome'].lower()}.pdf"
        build_pdf(p, _OUT / fname)
    print("Done.")


if __name__ == "__main__":
    main()
