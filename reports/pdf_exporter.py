"""
PDF Exporter — Markdown → PDF
===============================
Converts the generated Markdown research report into a
professional PDF named `{SYMBOL}_analysis.pdf`.

Uses markdown2 + weasyprint for high-fidelity rendering.
Falls back to fpdf2 if weasyprint is unavailable.
"""
import os
import re
import datetime


# ── CSS for the PDF report ───────────────────────────────────
_CSS = """
@page {
    size: A4;
    margin: 2cm 1.8cm;
    @top-right {
        content: "Equity Research Report";
        font-size: 8pt;
        color: #888;
    }
    @bottom-center {
        content: counter(page) " / " counter(pages);
        font-size: 8pt;
        color: #888;
    }
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI",
                 Helvetica, Arial, sans-serif;
    font-size: 10pt;
    line-height: 1.5;
    color: #24292e;
}

h1 { font-size: 20pt; color: #0366d6; border-bottom: 2px solid #0366d6;
     padding-bottom: 6px; margin-top: 20px; }
h2 { font-size: 14pt; color: #24292e; border-bottom: 1px solid #e1e4e8;
     padding-bottom: 4px; margin-top: 18px; }
h3 { font-size: 12pt; color: #586069; margin-top: 14px; }

table {
    border-collapse: collapse;
    width: 100%;
    margin: 10px 0;
    font-size: 9pt;
}
th, td {
    border: 1px solid #d1d5da;
    padding: 6px 10px;
    text-align: left;
}
th {
    background-color: #f6f8fa;
    font-weight: 600;
}
tr:nth-child(even) { background-color: #fafbfc; }

blockquote {
    border-left: 4px solid #dfe2e5;
    padding: 4px 16px;
    margin: 10px 0;
    color: #6a737d;
    background: #f6f8fa;
}

code {
    background: #f6f8fa;
    padding: 2px 4px;
    border-radius: 3px;
    font-size: 9pt;
}

strong { color: #24292e; }

ul, ol { margin: 6px 0; padding-left: 24px; }
li { margin: 2px 0; }

hr { border: none; border-top: 1px solid #e1e4e8; margin: 20px 0; }
"""


def export_markdown_to_pdf(md_filepath: str, symbol: str,
                           output_dir: str = "./output") -> str:
    """
    Convert a Markdown report file to PDF.

    Parameters:
        md_filepath: Path to the .md report file
        symbol: Stock symbol (e.g., "RELIANCE")
        output_dir: Directory for output PDF

    Returns:
        Path to the generated PDF, or empty string on failure.
    """
    if not os.path.exists(md_filepath):
        print(f"  ⚠ PDF export: Markdown file not found: {md_filepath}")
        return ""

    with open(md_filepath, 'r', encoding='utf-8') as f:
        md_text = f.read()

    os.makedirs(output_dir, exist_ok=True)
    # Derive PDF name from the markdown filename so they match
    md_basename = os.path.splitext(os.path.basename(md_filepath))[0]
    pdf_name = f"{md_basename}.pdf"
    pdf_path = os.path.join(output_dir, pdf_name)

    # ── Strategy 1: markdown2 + weasyprint (best quality) ─────
    try:
        import markdown2
        from weasyprint import HTML

        html_body = markdown2.markdown(
            md_text,
            extras=[
                "tables", "fenced-code-blocks", "cuddled-lists",
                "header-ids", "break-on-newline"
            ]
        )

        full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>{_CSS}</style>
</head>
<body>
{html_body}
</body>
</html>"""

        HTML(string=full_html).write_pdf(pdf_path)
        return pdf_path

    except ImportError:
        pass
    except Exception as e:
        print(f"  ⚠ weasyprint failed: {e}")

    # ── Strategy 2: markdown + pdfkit/wkhtmltopdf ────────────
    try:
        import markdown
        import pdfkit

        html_body = markdown.markdown(
            md_text,
            extensions=['tables', 'fenced_code']
        )

        full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>{_CSS}</style>
</head>
<body>
{html_body}
</body>
</html>"""

        options = {
            'page-size': 'A4',
            'margin-top': '20mm',
            'margin-bottom': '20mm',
            'margin-left': '18mm',
            'margin-right': '18mm',
            'encoding': 'UTF-8',
            'no-outline': None,
        }
        pdfkit.from_string(full_html, pdf_path, options=options)
        return pdf_path

    except ImportError:
        pass
    except Exception as e:
        print(f"  ⚠ pdfkit failed: {e}")

    # ── Strategy 3: fpdf2 (pure Python, always available) ────
    try:
        from fpdf import FPDF

        # First, try to find a system Unicode TTF font
        _unicode_font = _find_unicode_font()

        class ResearchPDF(FPDF):
            def __init__(self, unicode_font_path=None):
                super().__init__()
                self._ufont = None
                if unicode_font_path:
                    try:
                        self.add_font('UniFont', '', unicode_font_path, uni=True)
                        self.add_font('UniFont', 'B', unicode_font_path, uni=True)
                        self.add_font('UniFont', 'I', unicode_font_path, uni=True)
                        self._ufont = 'UniFont'
                    except Exception:
                        self._ufont = None

            def _font(self, style='', size=10):
                """Set font — use Unicode font if available, else Helvetica."""
                if self._ufont:
                    self.set_font(self._ufont, style, size)
                else:
                    self.set_font('Helvetica', style, size)

            def safe_multi_cell(self, w, h, text, **kw):
                """multi_cell wrapper that avoids 'not enough space' errors."""
                if not text or not text.strip():
                    return
                try:
                    self.multi_cell(w, h, text, **kw)
                except Exception:
                    # Fallback: reset X to left margin and use full width
                    try:
                        self.set_x(10)
                        self.multi_cell(0, h, text[:500], **kw)
                    except Exception:
                        pass  # skip unprintable line

            def header(self):
                self._font('I', 8)
                self.cell(0, 10, 'Equity Research Report', 0, 0, 'R')
                self.ln(12)

            def footer(self):
                self.set_y(-15)
                self._font('I', 8)
                self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

        pdf = ResearchPDF(_unicode_font)
        pdf.alias_nb_pages()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()

        # Emoji → text fallback (Arial Unicode MS lacks emoji glyphs)
        def _replace_emoji(t: str) -> str:
            _emoji_map = {
                # Status indicators — short symbols to avoid duplicating
                # the status word that follows (e.g. "✅ PASS" → "[Y] PASS")
                '\u2705': '[Y]',      # ✅
                '\u274C': '[N]',      # ❌
                '\u26A0': '(!)',      # ⚠  (base of ⚠️)
                '\u23ED': '[S]',      # ⏭  (base of ⏭️)
                '\u23F3': '',         # ⏳
                # Unicode arrows — always replace with ASCII
                '\u2191': '^',        # ↑ up arrow
                '\u2193': 'v',        # ↓ down arrow
                '\u2192': '->',       # → right arrow
                '\u2190': '<-',       # ← left arrow
                # Typographic — replace here so unicode-font path also gets them
                '\u2014': '-',        # em dash —
                '\u2013': '-',        # en dash –
                # Colored circles — differentiated, minimal noise
                '\U0001F534': '',     # 🔴 (removed — context is enough)
                '\U0001F7E1': '',     # 🟡
                '\U0001F7E2': '',     # 🟢
                '\U0001F7E0': '',     # 🟠
                '\u26AA': '',         # ⚪
                # Section icons — all removed (no bracket noise)
                '\U0001F3E5': '',      # 🏥
                '\U0001F50D': '',      # 🔍
                '\U0001F52C': '',      # 🔬
                '\U0001F3DB': '',      # 🏛
                '\U0001F3F0': '',      # 🏰
                '\U0001F6A9': '',      # 🚩
                '\U0001F4DA': '',      # 📚
                '\U0001F4CB': '',      # 📋
                '\U0001F4DD': '',      # 📝
                '\U0001F50E': '',      # 🔎
                '\U0001F4A1': '',      # 💡
                '\U0001F4B0': '',      # 💰
                '\U0001F525': '',      # 🔥
                '\U0001F4C9': '',      # 📉
                '\U0001F4CA': '',      # 📊
                '\U0001F4C8': '',      # 📈
                # Additional missing glyphs — all removed
                '\U0001F44D': '',      # 👍
                '\U0001F3F7': '',      # 🏷
                '\U0001F4CC': '',      # 📌
                '\U0001F4B5': '',      # 💵
                '\U0001F3E2': '',      # 🏢
                '\U0001F465': '',      # 👥
                '\U0001F91D': '',      # 🤝
                '\U0001F331': '',      # 🌱
                '\U0001F3AF': '',      # 🎯
                '\U0001F4C4': '',      # 📄
                '\U0001F517': '',      # 🔗
                '\U0001F6E1': '',      # 🛡
                # Currency & symbols
                '\u20B9': 'Rs.',       # ₹
                '\u2696': '',          # ⚖
                '\u26A1': '',          # ⚡
                '\uFE0F': '',          # variation selector
                # Recommendation icons
                '\u23F8': '',          # ⏸
            }
            for old, new in _emoji_map.items():
                t = t.replace(old, new)
            # Strip any remaining emoji that weren't explicitly mapped
            import re as _re
            t = _re.sub(
                '[\U0001F300-\U0001FAFF\U00002702-\U000027B0'
                '\U0000FE00-\U0000FE0F\U00002600-\U000026FF]+',
                '', t)
            return t

        # Always sanitize to pure ASCII/latin-1 — prevents Cyrillic
        # look-alikes (Сг), garbled division signs, and other
        # font rendering artefacts regardless of which font is active.
        clean = lambda t: _sanitize_for_latin(_replace_emoji(t))

        # \u2500\u2500 Helper: strip markdown formatting for PDF text \u2500\u2500\u2500\u2500\u2500\u2500\u2500
        def _strip_md(text: str) -> str:
            """Remove markdown bold/italic markers, LaTeX math,
            OCR artefacts, and other noise for plain PDF text."""
            text = text.replace('**', '').replace('*', '')
            # Strip inline LaTeX: $...$  and ^{...}
            text = re.sub(r'\$([^$]*)\$', r'\1', text)
            text = re.sub(r'\^\{[^}]*\}', '', text)
            # Remove stray $ signs
            text = text.replace('$', '')
            # OCR clean-up: scrapers confuse I/l in FIIs/DIIs
            text = (text.replace('Flls', 'FIIs')
                        .replace('Dils', 'DIIs')
                        .replace('FlIs', 'FIIs')
                        .replace('DlIs', 'DIIs')
                        .replace('FIls', 'FIIs')
                        .replace('DIls', 'DIIs')
                        .replace('Flls', 'FIIs'))
            # Replace Cyrillic look-alikes that OCR/fonts may introduce
            text = (text.replace('\u0421', 'C')   # Cyrillic С → Latin C
                        .replace('\u0433', 'r')   # Cyrillic г → Latin r
                        .replace('\u0440', 'p')   # Cyrillic р → Latin p
                        .replace('\u043E', 'o')   # Cyrillic о → Latin o
                        .replace('\u0435', 'e'))   # Cyrillic е → Latin e
            return text.strip()

        # ── Helper: parse a markdown table row into cells ────
        def _parse_row(row_str):
            """Split '| a | b | c |' into ['a', 'b', 'c']."""
            cells = row_str.strip().strip('|').split('|')
            return [c.strip() for c in cells]

        def _is_separator(row_str):
            """Check if row is a |---|---| separator."""
            return bool(re.match(r'^\|[\s\-:]+(\|[\s\-:]+)+\|?\s*$', row_str))

        def _detect_alignment(sep_row):
            """Parse separator row for column alignments."""
            cells = sep_row.strip().strip('|').split('|')
            aligns = []
            for c in cells:
                c = c.strip()
                if c.startswith(':') and c.endswith(':'):
                    aligns.append('C')
                elif c.endswith(':'):
                    aligns.append('R')
                else:
                    aligns.append('L')
            return aligns

        def _render_table(table_rows, pdf_obj):
            """Render collected markdown table rows as a proper PDF grid table
            with word-wrap, auto column widths, and headerless-table support."""
            if not table_rows:
                return

            # ── 1. Parse rows & detect separator ────────────────
            header_cells = None
            data_rows = []
            aligns = []
            for row_str in table_rows:
                if _is_separator(row_str):
                    aligns = _detect_alignment(row_str)
                    continue
                cells = _parse_row(row_str)
                if header_cells is None:
                    header_cells = cells
                else:
                    data_rows.append(cells)

            if not header_cells:
                return

            # Detect "headerless" tables (all header cells blank)
            has_header = any(c.strip() for c in header_cells)

            num_cols = len(header_cells)
            if not has_header and data_rows:
                # Treat the header cells as the first data row
                data_rows.insert(0, header_cells)
                header_cells = None

            # Normalise column count across every row
            all_rows = ([header_cells] if header_cells else []) + data_rows
            for i, row in enumerate(all_rows):
                if len(row) < num_cols:
                    all_rows[i] = row + [''] * (num_cols - len(row))
                elif len(row) > num_cols:
                    all_rows[i] = row[:num_cols]
            if len(aligns) < num_cols:
                aligns = aligns + ['L'] * (num_cols - len(aligns))

            # Re-split header / data after normalisation
            if header_cells:
                header_cells = all_rows[0]
                data_rows = all_rows[1:]
            else:
                data_rows = all_rows

            # ── 2. Calculate column widths ───────────────────────
            page_w = 190          # usable width (A4 = 210 − margins)
            font_size = 8
            line_h = 5            # line height inside cells
            cell_pad = 3          # horizontal padding

            pdf_obj._font('', font_size)

            # Measure natural (unwrapped) width per column
            col_natural = [0.0] * num_cols
            for row in all_rows:
                for ci in range(num_cols):
                    if ci < len(row):
                        txt = _strip_md(clean(row[ci]))
                        w = pdf_obj.get_string_width(txt) + cell_pad * 2
                        col_natural[ci] = max(col_natural[ci], w)

            # Ensure a reasonable minimum per column
            col_min = max(22, page_w / num_cols * 0.3)
            col_widths = [max(w, col_min) for w in col_natural]

            total = sum(col_widths)
            if total > page_w:
                # Proportionally shrink to fit
                scale = page_w / total
                col_widths = [w * scale for w in col_widths]
            elif total < page_w:
                # Distribute surplus proportionally
                surplus = page_w - total
                for ci in range(num_cols):
                    col_widths[ci] += surplus * (col_widths[ci] / total)

            # ── Post-scale: enforce minimum width per widest word ─
            # Prevents short words (e.g. "PASS") from wrapping in
            # narrow columns after proportional shrinking.
            for ci in range(num_cols):
                max_word_w = 0
                for row in all_rows:
                    if ci < len(row):
                        txt = _strip_md(clean(row[ci]))
                        for word in txt.split():
                            ww = pdf_obj.get_string_width(word)
                            max_word_w = max(max_word_w, ww)
                needed = max_word_w + cell_pad * 2 + 1
                if col_widths[ci] < needed:
                    deficit = needed - col_widths[ci]
                    col_widths[ci] = needed
                    # Steal space from the widest column
                    widest = max(range(num_cols),
                                 key=lambda x: col_widths[x])
                    if widest != ci:
                        col_widths[widest] = max(
                            col_widths[widest] - deficit,
                            cell_pad * 2 + 5)

            # ── helper: compute row height for a list of cells ───
            def _row_height(cells):
                max_lines = 1
                for ci, raw in enumerate(cells):
                    txt = _strip_md(clean(raw))
                    avail = col_widths[ci] - cell_pad * 2
                    if avail < 5:
                        avail = 5
                    tw = pdf_obj.get_string_width(txt)
                    lines = max(1, int(tw / avail) + (1 if tw % avail else 0))
                    max_lines = max(max_lines, lines)
                return max(line_h, max_lines * line_h)

            # ── helper: render one row of cells ──────────────────
            def _draw_row(cells, rh, bold=False, fill_color=None):
                if pdf_obj.get_y() + rh > pdf_obj.h - 20:
                    pdf_obj.add_page()
                x0 = pdf_obj.get_x()
                y0 = pdf_obj.get_y()
                for ci in range(num_cols):
                    x = x0 + sum(col_widths[:ci])
                    txt = clean(cells[ci]) if ci < len(cells) else ''
                    txt = _strip_md(txt)
                    align = aligns[ci] if ci < len(aligns) else 'L'

                    # Draw background + border rectangle
                    if fill_color:
                        pdf_obj.set_fill_color(*fill_color)
                    pdf_obj.rect(x, y0, col_widths[ci], rh,
                                 style='DF' if fill_color else 'D')

                    # Draw text with padding, clipped to cell
                    pdf_obj.set_xy(x + cell_pad, y0 + 0.8)
                    if bold:
                        pdf_obj._font('B', font_size)
                    else:
                        pdf_obj._font('', font_size)
                    try:
                        pdf_obj.multi_cell(
                            col_widths[ci] - cell_pad * 2, line_h,
                            txt, border=0, align=align)
                    except Exception:
                        try:
                            pdf_obj.multi_cell(
                                col_widths[ci] - cell_pad * 2, line_h,
                                txt[:60], border=0, align=align)
                        except Exception:
                            pass

                pdf_obj.set_xy(x0, y0 + rh)

            # ── 3. Render header ─────────────────────────────────
            pdf_obj.set_draw_color(180, 185, 190)
            if header_cells:
                rh = _row_height(header_cells)
                _draw_row(header_cells, rh, bold=True,
                          fill_color=(230, 235, 240))

            # ── 4. Render data rows ──────────────────────────────
            for ri, row in enumerate(data_rows):
                rh = _row_height(row)
                fill = (250, 251, 252) if ri % 2 else (255, 255, 255)
                _draw_row(row, rh, bold=False, fill_color=fill)

            pdf_obj.ln(3)  # spacing after table

        # ── Main rendering loop ──────────────────────────────
        lines_list = md_text.split('\n')
        table_buffer = []        # collect contiguous table rows
        _pending_heading = None  # deferred ### heading for chart pages
        i = 0
        while i < len(lines_list):
            line = lines_list[i]
            stripped = line.strip()

            # Detect table rows (starts with |) and buffer them
            if stripped.startswith('|'):
                table_buffer.append(stripped)
                i += 1
                continue
            else:
                # Flush any buffered table
                if table_buffer:
                    _render_table(table_buffer, pdf)
                    table_buffer = []

            # Headers
            if stripped.startswith('# '):
                pdf._font('B', 18)
                text = _strip_md(stripped[2:].strip())
                pdf.safe_multi_cell(0, 10, clean(text))
                pdf.ln(2)
                # Underline
                pdf.set_draw_color(3, 102, 214)
                pdf.line(10, pdf.get_y(), 200, pdf.get_y())
                pdf.ln(4)
            elif stripped.startswith('## '):
                pdf._font('B', 14)
                text = _strip_md(stripped[3:].strip())
                pdf.safe_multi_cell(0, 8, clean(text))
                pdf.ln(1)
                pdf.set_draw_color(225, 228, 232)
                pdf.line(10, pdf.get_y(), 200, pdf.get_y())
                pdf.ln(3)
            elif stripped.startswith('### '):
                # Peek ahead: if next non-empty line is an image, defer
                # the heading so it appears on the same page as the image
                next_img = False
                for k in range(i + 1, len(lines_list)):
                    nxt = lines_list[k].strip()
                    if nxt == '':
                        continue
                    if nxt.startswith('!['):
                        next_img = True
                    break
                if not next_img:
                    pdf._font('B', 12)
                    text = _strip_md(stripped[4:].strip())
                    pdf.safe_multi_cell(0, 7, clean(text))
                    pdf.ln(2)
                else:
                    # Store heading text; it will be rendered on the new
                    # page created by the image handler below.
                    _pending_heading = _strip_md(stripped[4:].strip())

            # Blockquotes
            elif stripped.startswith('>'):
                pdf._font('I', 9)
                text = _strip_md(stripped[1:].strip())
                _saved_lm = pdf.l_margin
                pdf.set_left_margin(15)
                pdf.set_x(15)
                pdf.safe_multi_cell(175, 5, clean(text))
                pdf.set_left_margin(_saved_lm)
                pdf.ln(2)

            # List items
            elif stripped.startswith('- ') or stripped.startswith('* '):
                pdf._font('', 10)
                text = _strip_md(stripped[2:].strip())
                _saved_lm = pdf.l_margin
                pdf.set_left_margin(15)
                pdf.set_x(15)
                pdf.safe_multi_cell(175, 5, f"  - {clean(text)}")
                pdf.set_left_margin(_saved_lm)
                pdf.ln(1)

            # Separator lines
            elif stripped.startswith('---'):
                pdf.ln(2)
                pdf.set_draw_color(225, 228, 232)
                pdf.line(10, pdf.get_y(), 200, pdf.get_y())
                pdf.ln(4)

            # Bold lines
            elif stripped.startswith('**') and stripped.endswith('**'):
                pdf._font('B', 10)
                text = _strip_md(stripped)
                pdf.safe_multi_cell(0, 6, clean(text))
                pdf.ln(1)

            # Inline images  ![alt](path)  — one image per page
            elif stripped.startswith('!['):
                img_match = re.match(r'!\[.*?\]\((.+?)\)', stripped)
                if img_match:
                    img_rel = img_match.group(1)
                    # Skip data URIs — fpdf2 can only embed files
                    if not img_rel.startswith('data:'):
                        md_dir = os.path.dirname(os.path.abspath(md_filepath))
                        img_abs = os.path.normpath(os.path.join(md_dir, img_rel))
                        if os.path.isfile(img_abs):
                            try:
                                # Always start a fresh page for each chart
                                pdf.add_page()
                                # Render deferred heading if present
                                if _pending_heading:
                                    pdf._font('B', 12)
                                    pdf.safe_multi_cell(0, 7, clean(_pending_heading))
                                    pdf.ln(2)
                                    _pending_heading = None
                                usable_w = pdf.w - pdf.l_margin - pdf.r_margin
                                usable_h = pdf.h - pdf.t_margin - pdf.b_margin - pdf.get_y() + pdf.t_margin - 5
                                # Fit image to page while keeping aspect ratio
                                from PIL import Image as PILImage
                                with PILImage.open(img_abs) as im:
                                    iw, ih = im.size
                                aspect = ih / iw if iw else 1
                                render_w = usable_w
                                render_h = render_w * aspect
                                if render_h > usable_h:
                                    render_h = usable_h
                                    render_w = render_h / aspect
                                x_pos = pdf.l_margin + (usable_w - render_w) / 2
                                pdf.image(img_abs, x=x_pos, y=pdf.get_y(),
                                          w=render_w, h=render_h)
                                pdf.ln(render_h + 4)
                            except Exception as img_err:
                                pdf._font('I', 8)
                                pdf.safe_multi_cell(0, 5, f"[Image error: {img_err}]")
                                pdf.ln(1)
                        else:
                            pdf._font('I', 8)
                            pdf.safe_multi_cell(0, 5, f"[Image not found: {img_rel}]")
                            pdf.ln(1)

            # Normal text
            elif stripped:
                pdf._font('', 10)
                text = _strip_md(stripped)
                pdf.safe_multi_cell(0, 5, clean(text))
                pdf.ln(1)

            # Empty line
            else:
                pdf.ln(3)

            i += 1

        # Flush any remaining table at end of file
        if table_buffer:
            _render_table(table_buffer, pdf)

        pdf.output(pdf_path)
        return pdf_path

    except ImportError:
        print("  ⚠ fpdf2 not installed — attempting auto-install...")
        import subprocess, sys
        subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                               'fpdf2', '-q'])
        print("  ✔ fpdf2 installed — retrying PDF export...")
        return export_markdown_to_pdf(md_filepath, symbol, output_dir)
    except Exception as e:
        print(f"  ⚠ fpdf2 failed: {e}")

    # All 3 strategies exhausted
    raise RuntimeError(
        "PDF generation failed — none of weasyprint / pdfkit / fpdf2 "
        "could produce a PDF.  Run:  pip install fpdf2"
    )


# ── Font helpers ──────────────────────────────────────────────
def _find_unicode_font() -> str:
    """Search for a system TTF font that supports Unicode."""
    import platform
    candidates = []
    if platform.system() == 'Darwin':
        candidates = [
            '/System/Library/Fonts/Supplemental/Arial Unicode.ttf',
            '/Library/Fonts/Arial Unicode.ttf',
            '/System/Library/Fonts/Helvetica.ttc',
            '/System/Library/Fonts/Supplemental/DejaVuSans.ttf',
        ]
    elif platform.system() == 'Linux':
        candidates = [
            '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
            '/usr/share/fonts/TTF/DejaVuSans.ttf',
            '/usr/share/fonts/dejavu/DejaVuSans.ttf',
            '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
        ]
    elif platform.system() == 'Windows':
        candidates = [
            r'C:\Windows\Fonts\arial.ttf',
            r'C:\Windows\Fonts\calibri.ttf',
            r'C:\Windows\Fonts\segoeui.ttf',
        ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return ""


def _sanitize_for_latin(text: str) -> str:
    """
    Replace Unicode characters with ASCII equivalents so that
    fpdf2's built-in Helvetica (latin-1) font can render them.
    """
    import re
    # ── Typographic replacements ──
    replacements = {
        '\u2014': '-',     # em dash —
        '\u2013': '-',     # en dash –
        '\u2018': "'",     # left single quote '
        '\u2019': "'",     # right single quote '
        '\u201C': '"',     # left double quote "
        '\u201D': '"',     # right double quote "
        '\u2026': '...',   # ellipsis …
        '\u2022': '-',     # bullet •
        '\u2023': '>',     # triangular bullet ‣
        '\u2043': '-',     # hyphen bullet ⁃
        '\u2190': '<-',    # left arrow ←
        '\u2192': '->',    # right arrow →
        '\u2191': '^',     # up arrow ↑
        '\u2193': 'v',     # down arrow ↓
        '\u2264': '<=',    # less than or equal ≤
        '\u2265': '>=',    # greater than or equal ≥
        '\u2260': '!=',    # not equal ≠
        '\u00D7': 'x',     # multiplication sign ×
        # ÷ (U+00F7) is valid latin-1 — Helvetica renders it natively
        '\u20B9': 'Rs.',   # Indian Rupee ₹
        '\u20AC': 'EUR',   # Euro €
        '\u00A3': 'GBP',   # Pound £
        '\u00A5': 'JPY',   # Yen ¥
        '\u2103': 'C',     # degree Celsius ℃
        '\u2109': 'F',     # degree Fahrenheit ℉
        '\u00B0': 'deg',   # degree sign °
        '\u00B1': '+/-',   # plus-minus ±
        '\u00BD': '1/2',   # fraction ½
        '\u00BC': '1/4',   # fraction ¼
        '\u00BE': '3/4',   # fraction ¾
        '\u2030': 'permil',  # per mille ‰
        '\u221E': 'inf',     # infinity ∞
        '\u2248': '~=',      # approximately ≈
        '\u2713': '[Y]',     # check mark ✓
        '\u2717': '[N]',     # cross mark ✗
        '\u2605': '*',       # star ★
        '\u2606': '*',       # white star ☆
        '\u25CF': '-',       # black circle ●
        '\u25CB': 'o',       # white circle ○
        '\u25A0': '#',       # black square ■
        '\u25A1': '[]',      # white square □
        '\u00A0': ' ',       # non-breaking space
        '\u200B': '',        # zero-width space
        '\u200C': '',        # zero-width non-joiner
        '\u200D': '',        # zero-width joiner
        '\uFEFF': '',        # BOM
        # Minus sign (U+2212) — not in latin-1, would become '?'
        '\u2212': '-',
        # Cyrillic look-alikes — OCR/fonts sometimes introduce these
        '\u0410': 'A', '\u0412': 'B', '\u0421': 'C', '\u0415': 'E',
        '\u041D': 'H', '\u041A': 'K', '\u041C': 'M', '\u041E': 'O',
        '\u0420': 'P', '\u0422': 'T', '\u0425': 'X',
        '\u0430': 'a', '\u0435': 'e', '\u043E': 'o', '\u0440': 'p',
        '\u0441': 'c', '\u0443': 'y', '\u0445': 'x',
        '\u0433': 'r',   # Cyrillic г → Latin r  (the Сг → Cr fix)
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    # ── Strip emoji ranges ──
    emoji_pattern = re.compile(
        "["
        "\U0001F300-\U0001F9FF"
        "\U00002702-\U000027B0"
        "\U0000FE00-\U0000FE0F"
        "\U00002600-\U000026FF"
        "\U0001FA00-\U0001FAFF"
        "]+",
        flags=re.UNICODE
    )
    text = emoji_pattern.sub('', text)

    # ── Final pass: replace any remaining non-latin-1 chars ──
    cleaned = []
    for ch in text:
        try:
            ch.encode('latin-1')
            cleaned.append(ch)
        except UnicodeEncodeError:
            cleaned.append('?')
    return ''.join(cleaned).strip()
