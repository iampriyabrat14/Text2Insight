"""
PDF exporter — question, AI summary, data table, and bar chart.
"""
import io
from datetime import datetime, timezone

from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.shapes import Drawing, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from backend.export.models import ExportSession

_BLUE      = colors.HexColor("#2563eb")
_INDIGO    = colors.HexColor("#4f46e5")
_DARK      = colors.HexColor("#0f172a")
_SLATE     = colors.HexColor("#334155")
_MUTED     = colors.HexColor("#64748b")
_LIGHT_BG  = colors.HexColor("#f8faff")
_BORDER    = colors.HexColor("#e2e8f0")
_ACCENT_BG = colors.HexColor("#eff6ff")
_TH_BG     = colors.HexColor("#1e3a8a")
_ROW_ALT   = colors.HexColor("#f1f5f9")


def _styles():
    base = getSampleStyleSheet()
    return {
        "cover_title": ParagraphStyle(
            "cover_title", parent=base["Title"],
            fontSize=30, textColor=_BLUE, spaceAfter=6,
            alignment=TA_CENTER, fontName="Helvetica-Bold",
        ),
        "cover_sub": ParagraphStyle(
            "cover_sub", parent=base["Normal"],
            fontSize=13, textColor=_SLATE, spaceAfter=4,
            alignment=TA_CENTER,
        ),
        "cover_meta": ParagraphStyle(
            "cover_meta", parent=base["Normal"],
            fontSize=10, textColor=_MUTED,
            alignment=TA_CENTER, spaceAfter=3,
        ),
        "q_label": ParagraphStyle(
            "q_label", parent=base["Normal"],
            fontSize=8, textColor=_BLUE, fontName="Helvetica-Bold",
            spaceBefore=14, spaceAfter=3, letterSpacing=1,
        ),
        "q_text": ParagraphStyle(
            "q_text", parent=base["Normal"],
            fontSize=11, textColor=_DARK, fontName="Helvetica-Bold",
            backColor=_ACCENT_BG,
            leftIndent=10, rightIndent=10,
            borderPad=8, borderColor=_BORDER, borderWidth=1,
            spaceAfter=10, leading=16,
        ),
        "a_label": ParagraphStyle(
            "a_label", parent=base["Normal"],
            fontSize=8, textColor=_INDIGO, fontName="Helvetica-Bold",
            spaceAfter=4, letterSpacing=1,
        ),
        "a_text": ParagraphStyle(
            "a_text", parent=base["Normal"],
            fontSize=11, textColor=_SLATE,
            spaceAfter=12, leading=17, alignment=TA_JUSTIFY,
        ),
        "section_label": ParagraphStyle(
            "section_label", parent=base["Normal"],
            fontSize=8, textColor=_MUTED, fontName="Helvetica-Bold",
            spaceBefore=10, spaceAfter=4, letterSpacing=1,
        ),
    }


# ── Shared chart-data helper ────────────────────────────────────────────────

def _pick_chart_cols(columns: list, rows: list):
    """Return (label_col, value_col) — first string col and first numeric col."""
    if not rows or not columns:
        return None, None
    sample = rows[0]
    label_col = value_col = None
    for c in columns:
        val = sample.get(c)
        try:
            float(str(val).replace(",", "").replace("$", "") or "x")
            if value_col is None:
                value_col = c
        except ValueError:
            if label_col is None:
                label_col = c
    return label_col, value_col


# ── PDF-specific builders ───────────────────────────────────────────────────

def _build_data_table(columns: list, rows: list, max_rows: int = 30) -> Table | None:
    if not columns or not rows:
        return None
    display = rows[:max_rows]

    # Truncate long cell text
    def _cell(v):
        s = str(v) if v is not None else "—"
        return s[:30] + "…" if len(s) > 30 else s

    data = [[c[:20] for c in columns]]
    for row in display:
        data.append([_cell(row.get(c)) for c in columns])

    col_count = len(columns)
    avail_w   = 16 * cm
    col_w     = min(avail_w / col_count, 4.5 * cm)

    tbl = Table(data, colWidths=[col_w] * col_count, repeatRows=1)
    tbl.setStyle(TableStyle([
        # Header
        ("BACKGROUND",  (0, 0), (-1, 0), _TH_BG),
        ("TEXTCOLOR",   (0, 0), (-1, 0), colors.white),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, 0), 8),
        ("ALIGN",       (0, 0), (-1, 0), "CENTER"),
        # Body
        ("FONTSIZE",    (0, 1), (-1, -1), 7.5),
        ("TEXTCOLOR",   (0, 1), (-1, -1), _SLATE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _ROW_ALT]),
        # Grid
        ("GRID",        (0, 0), (-1, -1), 0.4, _BORDER),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",  (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
    ]))
    return tbl


def _build_bar_chart(columns: list, rows: list) -> Drawing | None:
    label_col, value_col = _pick_chart_cols(columns, rows)
    if not label_col or not value_col:
        return None

    display = rows[:12]
    try:
        labels = [str(r.get(label_col, ""))[:14] for r in display]
        values = [float(str(r.get(value_col, 0)).replace(",", "").replace("$", "") or 0)
                  for r in display]
    except Exception:
        return None

    d = Drawing(440, 220)

    # Chart title
    title = String(220, 210, value_col, fontSize=9, fillColor=_MUTED,
                   textAnchor="middle", fontName="Helvetica")
    d.add(title)

    bc = VerticalBarChart()
    bc.x = 55
    bc.y = 40
    bc.width  = 360
    bc.height = 150

    bc.data            = [values]
    bc.bars[0].fillColor   = _BLUE
    bc.bars[0].strokeColor = None

    bc.categoryAxis.categoryNames       = labels
    bc.categoryAxis.labels.angle        = 30 if len(labels) > 6 else 0
    bc.categoryAxis.labels.fontSize     = 7
    bc.categoryAxis.labels.fontName     = "Helvetica"
    bc.categoryAxis.visibleGrid         = False
    bc.categoryAxis.strokeColor         = _BORDER

    bc.valueAxis.labels.fontSize        = 7
    bc.valueAxis.labels.fontName        = "Helvetica"
    bc.valueAxis.visibleGrid            = True
    bc.valueAxis.gridStrokeColor        = _BORDER
    bc.valueAxis.strokeColor            = _BORDER
    bc.valueAxis.forceZero              = True

    d.add(bc)
    return d


# ── Main generate function ──────────────────────────────────────────────────

def generate_pdf(session: ExportSession) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2.5 * cm, rightMargin=2.5 * cm,
        topMargin=2.5 * cm, bottomMargin=2.5 * cm,
        title=session.title,
        author=session.username,
    )
    S = _styles()
    story = []

    # ── Cover ─────────────────────────────────────────────────────────────
    story.append(Spacer(1, 2.5 * cm))
    story.append(Paragraph("AI Insights Report", S["cover_title"]))
    story.append(Spacer(1, 0.3 * cm))
    story.append(Paragraph(session.title, S["cover_sub"]))
    story.append(Spacer(1, 0.6 * cm))
    story.append(HRFlowable(width="60%", thickness=1.5, color=_BLUE, hAlign="CENTER"))
    story.append(Spacer(1, 0.5 * cm))
    export_dt = datetime.now(timezone.utc).strftime("%B %d, %Y")
    story.append(Paragraph(f"Prepared by {session.username}  ·  {export_dt}", S["cover_meta"]))
    story.append(Paragraph(
        f"{len(session.qa_pairs)} question{'s' if len(session.qa_pairs) != 1 else ''}",
        S["cover_meta"],
    ))
    story.append(PageBreak())

    # ── Q&A pairs ─────────────────────────────────────────────────────────
    for idx, pair in enumerate(session.qa_pairs, start=1):
        # Question
        story.append(Paragraph(f"QUESTION {idx}", S["q_label"]))
        story.append(Paragraph(pair["query"], S["q_text"]))

        # AI Summary
        story.append(Paragraph("AI SUMMARY", S["a_label"]))
        story.append(Paragraph(pair["summary"], S["a_text"]))

        # Data table
        result = pair.get("result_data") or {}
        cols   = result.get("columns", [])
        rows   = result.get("rows",    [])

        if cols and rows:
            story.append(Paragraph("DATA TABLE", S["section_label"]))
            tbl = _build_data_table(cols, rows)
            if tbl:
                story.append(tbl)
                story.append(Spacer(1, 0.4 * cm))

            # Bar chart
            chart = _build_bar_chart(cols, rows)
            if chart:
                story.append(Paragraph("CHART", S["section_label"]))
                story.append(chart)
                story.append(Spacer(1, 0.3 * cm))

        story.append(HRFlowable(width="100%", thickness=0.4, color=_BORDER))

    if not session.qa_pairs:
        story.append(Paragraph("No queries in this session.", S["a_text"]))

    doc.build(story)
    return buf.getvalue()
